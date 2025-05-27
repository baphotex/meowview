package main

import (
	"os"
	"fmt"
	"encoding/json"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/websocket"
)

type WebSocketMessage struct {
	DID    string `json:"did"`
	TimeUS int64  `json:"time_us"`
	Kind   string `json:"kind"`
	Commit struct {
		Rev        string          `json:"rev"`
		Operation  string          `json:"operation"`
		Collection string          `json:"collection"`
		Rkey       string          `json:"rkey"`
		Record     json.RawMessage `json:"record"`
		CID        string          `json:"cid"`
	} `json:"commit"`
}

func createKeyspace(session *gocql.Session) error {
	const maxRetries = 20
	var err error

	for i := 0; i < maxRetries; i++ {
		err = session.Query(`
			CREATE KEYSPACE IF NOT EXISTS cat 
			WITH replication = {
				'class': 'SimpleStrategy',
				'replication_factor': 1
			}`).Exec()
		if err == nil {
			return nil
		}
		log.Printf("keyspace creation attempt %d failed: %v", i+1, err)
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("failed to create keyspace after %d attempts: %v", maxRetries, err)
}

func main() {
	log.Println("starting meow server")
	cassandraHost := os.Getenv("CASSANDRA_HOST")
	if cassandraHost == "" {
		cassandraHost = "127.0.0.1"
	}
	cluster := gocql.NewCluster(cassandraHost)
	cluster.Timeout = 5 * time.Second
	cluster.ProtoVersion = 4

	// Create keyspace
	systemCluster := gocql.NewCluster(cassandraHost)
	systemCluster.Keyspace = "system"
	systemCluster.ProtoVersion = 4
	systemCluster.Timeout = 10 * time.Second

	systemSession, err := systemCluster.CreateSession()
	if err != nil {
		log.Fatal("System session:", err)
	}
	defer systemSession.Close()
	if err := createKeyspace(systemSession); err != nil {
		log.Fatal("Create keyspace:", err)
	}

	err = systemSession.Query(`
		CREATE KEYSPACE IF NOT EXISTS cat 
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec()
	if err != nil {
		log.Fatal("Create keyspace:", err)
	}

	// Create table session
	cluster.Keyspace = "cat"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Cassandra session:", err)
	}
	defer session.Close()

	// Create table with DID column
	err = session.Query(`
		CREATE TABLE IF NOT EXISTS meows (
			rkey TEXT PRIMARY KEY,
			time_us BIGINT,
			cid TEXT,
			did TEXT,
			record BLOB
		)`).Exec()
	if err != nil {
		log.Fatal("Create table:", err)
	}

	// Create secondary index on DID
	err = session.Query(`
		CREATE INDEX IF NOT EXISTS meows_did_idx 
		ON meows (did)`).Exec()
	if err != nil {
		log.Fatal("Create index:", err)
	}

	// WebSocket connection remains the same
	conn, _, err := websocket.DefaultDialer.Dial(
		"wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=cat.kasey.moe.meow",
		nil,
	)
	if err != nil {
		log.Fatal("dial:", err)
	}
	log.Println("connected to websocket")
	defer conn.Close()

	for {
		_, message, err := conn.ReadMessage()
		log.Printf("Received raw message: %s", string(message))
		if err != nil {
			log.Println("read error:", err)
			continue
		}

		var msg WebSocketMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Println("json unmarshal error:", err)
			continue
		}

		log.Printf("Parsed message - DID: %s, Rkey: %s, Operation: %s", msg.DID, msg.Commit.Rkey, msg.Commit.Operation)

		op := msg.Commit.Operation
		rkey := msg.Commit.Rkey

		switch op {
		case "create", "update":
			recordBytes := []byte(msg.Commit.Record)
			err := session.Query(`
				INSERT INTO meows (rkey, time_us, cid, did, record) 
				VALUES (?, ?, ?, ?, ?)`,
				msg.Commit.Rkey,
				msg.TimeUS,
				msg.Commit.CID,
				msg.DID,  // Added DID value
				recordBytes,
			).Exec()
			if err != nil {
				log.Println("insert error:", err)
			}

		case "delete":
			err := session.Query(`DELETE FROM meows WHERE rkey = ?`, rkey).Exec()
			if err != nil {
				log.Println("delete error:", err)
			}

		default:
			log.Printf("Unknown operation: %s\n", op)
		}
	}
}
