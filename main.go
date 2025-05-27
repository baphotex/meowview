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

type MeowRecord struct {
	Type    string `json:"$type"`
	Emotion *string `json:"emotion,omitempty"`
	Subject *string `json:"subject,omitempty"`
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
		log.Fatal("system session:", err)
	}
	defer systemSession.Close()
	if err := createKeyspace(systemSession); err != nil {
		log.Fatal("create keyspace:", err)
	}

	err = systemSession.Query(`
		CREATE KEYSPACE IF NOT EXISTS cat 
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec()
	if err != nil {
		log.Fatal("create keyspace:", err)
	}

	// Create table session
	cluster.Keyspace = "cat"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("cassandra session:", err)
	}
	defer session.Close()

	// Create table with DID column
	err = session.Query(`
		CREATE TABLE IF NOT EXISTS meows (
			rkey TEXT PRIMARY KEY,
			time_us BIGINT,
			cid TEXT,
			did TEXT,
			emotion TEXT,
			subject TEXT
		)`).Exec()
	if err != nil {
		log.Fatal("create table:", err)
	}

	// Create secondary index on DID
	err = session.Query(`
		CREATE INDEX IF NOT EXISTS meows_did_idx 
		ON meows (did)`).Exec()
	if err != nil {
		log.Fatal("create actor index:", err)
	}
	
	// create secondary index on subject
	err = session.Query(`
		CREATE INDEX IF NOT EXISTS meows_subject_idx 
		ON meows (subject)`).Exec()
	if err != nil {
		log.Fatal("create subject index:", err)
	}

	// WebSocket connection remains the same
	conn, _, err := websocket.DefaultDialer.Dial(
		"wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=moe.kasey.meow",
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

		var record MeowRecord
		if err := json.Unmarshal(msg.Commit.Record, &record); err != nil {
			log.Println("record parse error:", err)
			continue
		}
		
		var emotion *string
		if record.Emotion != nil {
			truncated := *record.Emotion
			if len(truncated) > 50 {
				truncated = (truncated)[:50]
				log.Println("emotion too long, truncating to 50 characters")
			}
			emotion = &truncated
		}

		log.Printf("Parsed message - DID: %s, Rkey: %s, Operation: %s", msg.DID, msg.Commit.Rkey, msg.Commit.Operation)

		op := msg.Commit.Operation
		rkey := msg.Commit.Rkey

		switch op {
		case "create", "update":
			err := session.Query(`
				INSERT INTO meows (rkey, time_us, cid, did, emotion, subject) 
				VALUES (?, ?, ?, ?, ?, ?)`,
				msg.Commit.Rkey,
				msg.TimeUS,
				msg.Commit.CID,
				msg.DID,  //
				emotion, // can be nil
				record.Subject,
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
