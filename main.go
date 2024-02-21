package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	// "strings"
	"os" // Import the os package
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // Import the PostgreSQL driver
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var (
	// ... (variables)
	batchSize     = 1 // Set the batch size
	batchMessages []string
)
var db *sql.DB

type RequestBody struct {
	Project string `json:"project"`
	UserID  string `json:"userId"`
}

type InfoData struct {
	ActivityUUID       string    `json:"activity_uuid"`
	UserUID            string    `json:"user_uid"`
	OrganizationID     string    `json:"organization_id"`
	Timestamp          time.Time `json:"timestamp"`
	AppName            string    `json:"app_name"`
	URL                string    `json:"url"`
	PageTitle          string    `json:"page_title"`
	ProductivityStatus string    `json:"productivity_status"`
	Meridian           string    `json:"meridian"`
	IPAddress          string    `json:"ip_address"`
	MacAddress         string    `json:"mac_address"`
	MouseMovement      bool      `json:"mouse_movement"`
	MouseClicks        int       `json:"mouse_clicks"`
	KeysClicks         int       `json:"keys_clicks"`
	Status             int       `json:"status"`
	CPUUsage           string    `json:"cpu_usage"`
	RAMUsage           string    `json:"ram_usage"`
	ScreenshotUID      string    `json:"screenshot_uid"`
}

func main() {
	// PostgreSQL connection string
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	connStr := os.Getenv("POSTGRES_CONN_STR")

	// connStr := "f7hiu7ql46m8ev2cpbp1:pscale_pw_5Hr2xQwvZQYg83n069wNs7dNAreLmYq302zM9rlRLSG@tcp(aws.connect.psdb.cloud)/tracktime?tls=true&interpolateParams=true"

	// Establish a database connection
	fmt.Println(connStr)
	// Connect to the PostgreSQL database
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		panic(err) // Print and exit on error
	}
	defer db.Close()

	// Check if the connection is successful
	if err := db.Ping(); err != nil {
		fmt.Println("Error connecting to the database:", err)
		return
	}

	fmt.Println("Connected to the database")
	// Check if we're connected
	// err = db.Ping()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer db.Close()

	// Kafka settings
	mechanism, err := scram.Mechanism(scram.SHA512, "c21pbGluZy1naWJib24tNjc4OSSKcT9_efyCQyls2uRGWVqeWwlnKZJuIrKK-Mg", "OWJjOTVjMDUtYTE2My00NGU5LTg4ODMtOWE4ZjRhZjEyMmU4")
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	topic := "kafka_test" // Replace with your topic name
	// partition := 0

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"smiling-gibbon-6789-us1-kafka.upstash.io:9092"}, // Replace with your Upstash Kafka broker endpoint
		GroupID: "test_kafka",
		Topic:   topic,
		Dialer:  dialer,
	})
	defer r.Close()

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	// defer cancel()
	// Kafka consumer loop
	for {
		// Set a context with timeout for each read operation
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*300) // Example: 5-second timeout
		select {
		case <-time.After(time.Millisecond * 100): // Check for message periodically
			m, err := r.ReadMessage(ctx)

			if err != nil {
				if err == context.DeadlineExceeded {
					fmt.Println("Context deadline exceeded while reading Kafka message")
				} else {
					fmt.Println("Error reading Kafka message:", err)
				}
				cancel()
				break // Break the select, not the for loop
			}

			fmt.Println("Received message:", string(m.Value))
			batchMessages = append(batchMessages, string(m.Value)) // accumulate messages in the batch

			if len(batchMessages) >= batchSize {
				// Process the batch when it reaches the desired size
				processBatch(db, batchMessages)
				batchMessages = nil // reset the batch
			}
			// Process the received message

			cancel() // Cancel the context after processing the message

		case <-ctx.Done():
			// Context was cancelled, possibly due to timeout
			fmt.Println("Context canceled or deadline exceeded")
			cancel()
			continue // Continue to the next iteration of the loop
		}
	}
}

func processBatch(db *sql.DB, messages []string) {
	for _, message := range messages {
		var infoData InfoData
		if err := json.Unmarshal([]byte(message), &infoData); err != nil {
			log.Printf("Error unmarshalling message: %v\n", err)
			continue // Skip to the next message if there's an error
		}

		if err := insertOrUpdateProject(db, infoData); err != nil {
			log.Printf("Error inserting/updating data: %v\n", err)
			// Consider whether to continue or return/exit based on your error handling policy
		}
		confirmDataAdded(db)
	}
}

func createNewTable(db *sql.DB) error {
	connStr := os.Getenv("MYSQL_CONN_STR")
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	createTableSQL := `
    CREATE TABLE IF NOT EXISTS user_activity (
        activity_uuid VARCHAR(255) PRIMARY KEY,
        user_uid VARCHAR(255),
        organization_id VARCHAR(255),
        timestamp DATETIME,
        app_name VARCHAR(255),
        url VARCHAR(255),
        page_title VARCHAR(255),
        productivity_status VARCHAR(255),
        meridian VARCHAR(255),
        ip_address VARCHAR(255),
        mac_address VARCHAR(255),
        mouse_movement BOOLEAN,
        mouse_clicks INT,
        keys_clicks INT,
        status INT,
        cpu_usage VARCHAR(255),
        ram_usage VARCHAR(255),
        screenshot_uid VARCHAR(255)
    );`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	fmt.Println("Table 'user_activity' created successfully.")
	return nil
}

func fetchData(db *sql.DB) {
	rows, err := db.Query("SELECT * FROM user_activity")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Iterate through the result rows and print the data
	for rows.Next() {
		var user_title, mac_address, usb_info, user_app_name, user_process_id, user_window_id, img_id string
		var id int
		if err := rows.Scan(&id, &user_title, &mac_address, &usb_info, &user_app_name, &user_process_id, &user_window_id, &img_id); err != nil {
			panic(err)
		}
		fmt.Println(id, user_title, mac_address, usb_info, user_app_name, user_process_id, user_window_id, img_id) // Replace with your actual column names
	}

	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func insertOrUpdateProject(db *sql.DB, data InfoData) error {
	sqlStatement := `
    INSERT INTO user_activity (activity_uuid, user_uid, organization_id, timestamp, app_name, url, page_title, productivity_status, meridian, ip_address, mac_address, mouse_movement, mouse_clicks, keys_clicks, status, cpu_usage, ram_usage, screenshot_uid)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `

	_, err := db.Exec(sqlStatement, data.ActivityUUID, data.UserUID, data.OrganizationID, data.Timestamp, data.AppName, data.URL, data.PageTitle, data.ProductivityStatus, data.Meridian, data.IPAddress, data.MacAddress, data.MouseMovement, data.MouseClicks, data.KeysClicks, data.Status, data.CPUUsage, data.RAMUsage, data.ScreenshotUID)
	if err != nil {
		return err
	}

	fmt.Println("Data inserted successfully.")
	return nil
}

func confirmDataAdded(db *sql.DB) {
	query := "SELECT COUNT(*) FROM user_activity"
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatalf("Error querying the database: %v", err)
	}

	fmt.Printf("Number of records in user_activity table: %d\n", count)
}
