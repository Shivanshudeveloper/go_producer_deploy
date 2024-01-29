package main

import (
	"crypto/tls"
	"database/sql"

	// "encoding/base64"
	"encoding/json"
	"fmt"

	// "io/ioutil"
	"log"
	"os"

	// "strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
}

var (
	batchSize     = 1
	batchMessages []string
)

var db *sql.DB

// var producer sarama.SyncProducer // Upstash Kafka producer

type InfoData struct {
	ActivityUUID       string    `json:"activity_uuid"`
	UserUID            string    `json:"user_uid"`
	OrganizationID     string    `json:"organization_id"`
	Timestamp          time.Time `json:"timestamp"`
	AppName            string    `json:"app_name"`
	URL                string    `json:"url"`
	PageTitle          string    `json:"page_title"`
	Screenshot         string    `json:"screenshot"`
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

func deleteAllImgInfo() error {
	connStr := os.Getenv("POSTGRES_CONN_STR")
	// Open a database connection
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	sqlStatement := "DELETE FROM img_info"

	// Execute the SQL statement to delete all data from the table
	_, err = db.Exec(sqlStatement)
	if err != nil {
		return err
	}

	fmt.Println("All data in the img_info table deleted successfully.")
	return nil
}

func saveImgStatus(img_name string, status string) error {
	connStr := os.Getenv("POSTGRES_CONN_STR")
	fmt.Println(img_name)
	fmt.Println(status)
	// Open a database connection
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	sqlStatement := `
    INSERT INTO img_info (img_name,status)
    VALUES ($1, $2)
    `

	// Execute the SQL statement to insert the data
	_, err = db.Exec(sqlStatement, img_name, status)
	if err != nil {
		return err
	}

	fmt.Println("Data inserted successfully.")
	return nil
}

// func insertOrUpdateProject(message MessageData, img_id string) error {
// 	connStr := os.Getenv("POSTGRES_CONN_STR")
// 	db, err := sql.Open("mysql", connStr)
// 	if err != nil {
// 		return err
// 	}
// 	defer db.Close()

// 	sqlStatement := `
//     INSERT INTO system_info (user_title,mac_address, usb_info, user_app_name, user_process_id, user_window_id,img_id)
//     VALUES ($1, $2, $3, $4, $5, $6, $7)
//     `

// 	// Execute the SQL statement to insert the data
// 	_, err = db.Exec(sqlStatement, message.UserTitle, message.MacInfo, message.UsbInfo, message.UserAppName, message.UserProcessID, message.UserWindowID, img_id)
// 	if err != nil {
// 		return err
// 	}

// 	fmt.Println("Data inserted successfully.")
// 	return nil
// }

func insertOrUpdateProject(data InfoData) error {
	connStr := os.Getenv("MYSQL_CONN_STR") // Make sure this is the correct environment variable for your MySQL connection string
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	sqlStatement := `
    INSERT INTO user_activity (activity_uuid, user_uid, organization_id, timestamp, app_name, url, page_title, screenshot, productivity_status, meridian, ip_address, mac_address, mouse_movement, mouse_clicks, keys_clicks, status, cpu_usage, ram_usage, screenshot_uid)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	// Execute the SQL statement to insert the data
	_, err = db.Exec(sqlStatement, data.ActivityUUID, data.UserUID, data.OrganizationID, data.Timestamp, data.AppName, data.URL, data.PageTitle, data.Screenshot, data.ProductivityStatus, data.Meridian, data.IPAddress, data.MacAddress, data.MouseMovement, data.MouseClicks, data.KeysClicks, data.Status, data.CPUUsage, data.RAMUsage, data.ScreenshotUID)

	if err != nil {
		return err
	}

	fmt.Println("Data inserted successfully.")
	return nil
}

func createNewTable() error {
	// Database connection string
	connStr := os.Getenv("POSTGRES_CONN_STR")
	// Open a database connection
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	// SQL statement to create a new table
	createTableSQL := `
    CREATE TABLE IF NOT EXISTS img_info (
        id SERIAL PRIMARY KEY,
        img_name TEXT,
        status TEXT
    );`

	// Execute the SQL statement to create the table
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	fmt.Println("Table 'img_info' created successfully.")
	return nil
}

func main() {
	app := fiber.New()
	loadEnv()

	app.Post("/produce", func(ctx *fiber.Ctx) error {
		var kafkaData interface{}
		fmt.Println("Received raw request data:")
		fmt.Println(string(ctx.Body()))

		if err := json.Unmarshal(ctx.Body(), &kafkaData); err != nil {
			return ctx.Status(fiber.StatusBadRequest).SendString("Failed to parse JSON")
		}

		mechanism, err := scram.Mechanism(scram.SHA512, "c21pbGluZy1naWJib24tNjc4OSSKcT9_efyCQyls2uRGWVqeWwlnKZJuIrKK-Mg", "OWJjOTVjMDUtYTE2My00NGU5LTg4ODMtOWE4ZjRhZjEyMmU4")
		if err != nil {
			log.Fatalf("Error creating SCRAM mechanism: %v", err)
		}

		writerConfig := kafka.WriterConfig{
			Brokers: []string{"smiling-gibbon-6789-us1-kafka.upstash.io:9092"},
			Topic:   "kafka_test",
			Dialer: &kafka.Dialer{
				SASLMechanism: mechanism,
				TLS:           &tls.Config{},
			},
		}

		kafkaWriter := kafka.NewWriter(writerConfig)
		defer kafkaWriter.Close()

		switch data := kafkaData.(type) {
		case []interface{}:
			for _, item := range data {
				itemBytes, err := json.Marshal(item)
				if err != nil {
					return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to marshal JSON")
				}
				if err := sendToKafka(kafkaWriter, itemBytes, ctx); err != nil {
					return err
				}
			}
		case map[string]interface{}:
			itemBytes, err := json.Marshal(data)
			if err != nil {
				return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to marshal JSON")
			}
			if err := sendToKafka(kafkaWriter, itemBytes, ctx); err != nil {
				return err
			}
		default:
			return ctx.Status(fiber.StatusBadRequest).SendString("Invalid JSON format")
		}

		return ctx.Status(fiber.StatusOK).SendString("Message produced and processed successfully.")
	})

	app.Listen(":8080")
}

func sendToKafka(kafkaWriter *kafka.Writer, message []byte, ctx *fiber.Ctx) error {
	err := kafkaWriter.WriteMessages(ctx.Context(), kafka.Message{
		Value: message,
	})

	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to produce message to Kafka")
	}
	return nil
}
