package main

import (
	"os"
	"fmt"
	"encoding/json"
	"log"
	"time"
	"strings"
	"net/http"
	"regexp"
	
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/gorilla/websocket"
	"github.com/google/uuid"
)

type DIDDocument struct {
	ID string `json:"id"`
}

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

type MeowResponse struct {
	Rkey string `json:"rkey"`
	TimeUS int64 `json:"time_us"`
	CID string `json:"cid"`
	DID string `json:"did"`
	Emotion string `json:"emotion"`
	Subject string `json:"subject"`
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
			id UUID PRIMARY KEY,
			rkey TEXT,
			time_us BIGINT,
			cid TEXT,
			did TEXT,
			emotion TEXT,
			subject TEXT
		)`).Exec()
	if err != nil {
		log.Fatal("create table:", err)
	}
	
	// craete secondary index on rkey
	err = session.Query(`
		CREATE INDEX IF NOT EXISTS meows_rkey_idx 
		ON meows (rkey)`).Exec()
	if err != nil {
		log.Fatal("create rkey index:", err)
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

	// create secondary index on time 
	err = session.QUERY(`
		CREATE INDEX IF NOT EXISTS meows_time_idx 
		ON meows (time_us)`).Exec()
	if err != nil {
		log.Fatal("create time index:", err)
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
	
	go func() {
		r := setupRouter(session) 
		if err := r.Run(":8134"); err != nil {
			log.Fatal("router error:", err)
		}
	}

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
			// coerce emotion to a lower case string
			// exclude possible sql injections and malicious input
			emotion = strings.ToLower(record.Emotion)
			truncated := *record.Emotion
			if len(truncated) > 50 {
				truncated = (truncated)[:50]
				log.Println("emotion too long, truncating to 50 characters")
			}
			emotion = &truncated

			if strings.Contains(emotion, ";") || strings.Contains(emotion, "'") || strings.Contains(emotion, "\"") || strings.Contains(emotion, "`") {
				log.Println("emotion contains malicious input, ignoring")
				continue
			}
			if string.Contains(emotion, "create") || string.Contains(emotion, "insert") || string.Contains(emotion, "update") || string.Contains(emotion, "delete") || string.Contains(emotion, "drop") {
				log.Println("emotion contains malicious input, ignoring")
				continue
			}
			

		}
		// coerce emotion to 
		var subject *string
		if record.Subject != nil {
			subject = validateSubject(*record.Subject)
		}
		else {
			subject = nil
		}

		log.Printf("Parsed message - DID: %s, Rkey: %s, Operation: %s", msg.DID, msg.Commit.Rkey, msg.Commit.Operation)

		op := msg.Commit.Operation
		rkey := msg.Commit.Rkey
		id := uuid.New()

		switch op {
		case "create", "update":
			err := session.Query(`
				INSERT INTO meows (id, rkey, time_us, cid, did, emotion, subject) 
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				id,
				msg.Commit.Rkey,
				msg.TimeUS,
				msg.Commit.CID,
				msg.DID,  //
				emotion, // can be nil
				subject, // can be nil
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

func validateSubject(subject string) string {
	// starts with did:plc and starts with did:web, make requet to the did doc or the plc directory
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if strings.HasPrefix(subject, "did:plc:") {
		return validatePLCDID(ctx, subject)
	}
	
	if strings.HasPrefix(subject, "did:web:") {
		return validateWebDID(ctx, subject)
	}
	
	return nil 
}

func validatePLCDID(ctx context.Context, did string) string {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("https://plc.directory/%s", did)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("PLC DID request error: %v", err)
		return nil
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("PLC DID fetch error: %v", err)
		return nil
	}
	defer resp.Body.Close()

	var doc DIDDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		log.Printf("PLC DID decode error: %v", err)
		return nil
	}

	return doc.ID
}


func validateWebDID(ctx context.Context, did string) string {
	parts := strings.SplitN(did, ":", 3)
	if len(parts) != 3 {
		return nil
	}

	domain := parts[2]
	url := fmt.Sprintf("https://%s/.well-known/did.json", domain)

	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("Web DID request error: %v", err)
		return nil
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Web DID fetch error: %v", err)
		return nil
	}
	defer resp.Body.Close()

	var doc DIDDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		log.Printf("Web DID decode error: %v", err)
		return nil
	}

	return doc.ID
}

func setupRouter(session *gocql.Session) *gin.Engine {
	r := gin.Default()

	// 1. Get last N meows by time
	r.GET("/_endpoints/getLastMeows", func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if limit > 100 {
			limit = 100
		}

		var meows []MeowResponse
		iter := session.Query(`
			SELECT rkey, time_us, cid, did, emotion, subject
			FROM cat.meows 
			LIMIT ?
			ALLOW FILTERING`,
			limit,
		).Iter()

		var m MeowResponse
		for iter.Scan(&m.RKey, &m.TimeUS, &m.CID, &m.DID, &m.Emotion. &m.Subject) {
			meows = append(meows, m)
			m = MeowResponse{}
		}

		if err := iter.Close(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, meows)
	})

	// 2. Get meows by DID
	r.GET("/_endpoints/getActorMeows", func(c *gin.Context) {
		did := c.Query("did")
		validatedDid := validateDID(did)
		var meows []MeowResponse

		iter := session.Query(`
			SELECT rkey, time_us, cid, did, emotion, subject
			FROM cat.meows 
			WHERE did = ?
			ALLOW FILTERING`,
			validatedDid,
		).Iter()

		var m MeowResponse
		for iter.Scan(&m.RKey, &m.TimeUS, &m.CID, &m.DID, &m.Emotion, &m.Subject) {
			meows = append(meows, m)
			m = MeowResponse{}
		}

		if err := iter.Close(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, meows)
	})

	// 3. Get meows by subject DID
	r.GET("/_endpoints/getSubjectMeows", func(c *gin.Context) {
		subject := c.Query("did")
		validatedSubject := validateDID(subject)
		var meows []MeowResponse

		iter := session.Query(`
			SELECT rkey, time_us, cid, did, emotion, subject
			FROM cat.meows 
			WHERE subject = ?
			ALLOW FILTERING`,
			validatedSubject,
		).Iter()

		var m MeowResponse
		for iter.Scan(&m.RKey, &m.TimeUS, &m.CID, &m.DID, &m.Emotion, &m.Subject) {
			meows = append(meows, m)
			m = MeowResponse{}
		}

		if err := iter.Close(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, meows)
	})

	// 4. Get specific meow
	r.GET("/_endpoints/getMeow", func(c *gin.Context) {
		rkey := c.Query("rkey")
		did := c.Query("did")
		validatedDid := validateDID(did)
		if validatedDid != did {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid did"})
			return
		}
		// validate the rkey 3lq4slogsz52p - it must be a valid string 13 letters, and only alpha numerics
		re := regexp.MustCompile(`^[a-z0-9]{13}$`)
		if !re.MatchString(rkey) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid rkey"})
			return
		}

		var m MeowResponse
		err := session.Query(`
			SELECT rkey, time_us, cid, did, emotion, subject
			FROM cat.meows 
			WHERE rkey = ? AND did = ?
			LIMIT 1`,
			rkey, validatedDid,
		).Scan(&m.Rkey, &m.TimeUS, &m.CID, &m.DID, &m.Emotion, &m.Subject)

		if err != nil {
			if err == gocql.ErrNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "meow not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		m.RKey = rkey
		c.JSON(http.StatusOK, m)
	})

	return r
}

