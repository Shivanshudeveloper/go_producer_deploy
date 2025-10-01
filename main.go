package main

import (
	"crypto/tls"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/gofiber/fiber/v2"
    "github.com/joho/godotenv"
    "github.com/segmentio/kafka-go"
    "github.com/segmentio/kafka-go/sasl/scram"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "io/ioutil"
    "log"
    "os"
    "path/filepath"
    "time"
    "bytes"
	"net/http"
    // "image"
    // _ "image/png" // If you plan to support JPEG images as well
    // "github.com/nfnt/resize"
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

// JSON Data in go should contain capitalized keys for example `thumbnail` should not be like this it should be 
// `Thumbnail`it should be capital not small and if it's small then go will not be able to read this data field because 
// json marshal in go always reads the capitalize key's not small one at all learning 
// from experience and bug which took whole day to fix. day learned this thing --> 19/02/2024 by a learner

// InfoData for receiving from Rust (with base64 data)
type InfoDataFromRust struct {
    ActivityUUID       string    `json:"activity_uuid"`
    UserUID            string    `json:"user_id"`
    OrganizationID     string    `json:"organization_id"`
    Timestamp          time.Time `json:"timestamp"`
    AppName            string    `json:"app_name"`
    URL                string    `json:"url"`
    PageTitle          string    `json:"page_title"`
    ScreenshotWebP     string    `json:"screenshot_webp"`     // WebP base64 from Rust
    ThumbnailJPEG      string    `json:"thumbnail_jpeg"`      // JPEG base64 from Rust
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
    ScreenshotUID      string    `json:"screenshot_uid"`      // WebP UID from Rust
    ThumbnailUID       string    `json:"thumbnail_uid"`       // JPEG UID from Rust
    Device_user_name   string    `json:"device_user_name"`
}

// InfoData for sending to Kafka (without base64 data)
type InfoDataForKafka struct {
    ActivityUUID       string    `json:"activity_uuid"`
    UserUID            string    `json:"user_id"`
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
    ScreenshotUID      string    `json:"screenshot_uid"`      // ✅ Only UIDs for Kafka
    ThumbnailUID       string    `json:"thumbnail_uid"`       // ✅ Only UIDs for Kafka
    Device_user_name   string    `json:"device_user_name"`
}

// processData handles the common logic for processing InfoData, whether it comes from a single object or an array.
func processData(infoData InfoDataFromRust, ctx *fiber.Ctx) error {
    // ✅ USE: UIDs already provided by Rust desktop app
    // Rust sends: screenshot_uid (WebP format) and thumbnail_uid (JPEG format)
    // No need to generate new UIDs - use the ones from Rust
    log.Printf("Using UIDs from Rust: ScreenshotUID=%s, ThumbnailUID=%s", infoData.ScreenshotUID, infoData.ThumbnailUID)

    log.Printf("✅ Processing data for user: %s, activity: %s", infoData.UserUID, infoData.ActivityUUID)
    log.Printf("📸 WebP UID: %s", infoData.ScreenshotUID)
    log.Printf("🖼️  JPEG UID: %s", infoData.ThumbnailUID)

    // Upload WebP screenshot (full-size for list & detail views)
    err := uploadWebPToWasabi(infoData, ctx)
    if err != nil {
        log.Printf("❌ Failed to upload WebP to Wasabi: %v", err)
        return fmt.Errorf("failed to upload WebP to Wasabi: %v", err)
    }

    // Upload JPEG thumbnail (for fallback/download)
    err = uploadJPEGThumbnailToWasabi(infoData, ctx)
    if err != nil {
        log.Printf("❌ Failed to upload JPEG thumbnail to Wasabi: %v", err)
        return fmt.Errorf("failed to upload JPEG thumbnail to Wasabi: %v", err)
    }

    // ✅ CREATE CLEAN MESSAGE: Remove base64 data, keep only UIDs
    cleanData := InfoDataForKafka{
        ActivityUUID:       infoData.ActivityUUID,
        UserUID:            infoData.UserUID,
        OrganizationID:     infoData.OrganizationID,
        Timestamp:          infoData.Timestamp,
        AppName:            infoData.AppName,
        URL:                infoData.URL,
        PageTitle:          infoData.PageTitle,
        ProductivityStatus: infoData.ProductivityStatus,
        Meridian:           infoData.Meridian,
        IPAddress:          infoData.IPAddress,
        MacAddress:         infoData.MacAddress,
        MouseMovement:      infoData.MouseMovement,
        MouseClicks:        infoData.MouseClicks,
        KeysClicks:         infoData.KeysClicks,
        Status:             infoData.Status,
        CPUUsage:           infoData.CPUUsage,
        RAMUsage:           infoData.RAMUsage,
        ScreenshotUID:      infoData.ScreenshotUID,      // ✅ Only UIDs, no base64
        ThumbnailUID:       infoData.ThumbnailUID,       // ✅ Only UIDs, no base64
        Device_user_name:   infoData.Device_user_name,
    }
    
    log.Printf("📨 Preparing clean Kafka message with UIDs: WebP=%s, JPEG=%s", cleanData.ScreenshotUID, cleanData.ThumbnailUID)

    message, err := json.Marshal(cleanData)
    if err != nil {
        log.Printf("❌ Failed to marshal infoData to JSON: %v", err)
        return fmt.Errorf("failed to marshal infoData to JSON: %v", err)
    }

    // Send to Kafka
    err = sendToKafka(message, ctx)
    if err != nil {
        log.Printf("❌ Failed to send message to Kafka: %v", err)
        return fmt.Errorf("failed to send message to Kafka: %v", err)
    }

    log.Printf("✅ Data processed successfully for user: %s", infoData.UserUID)
    return nil
}

func main() {
    app := fiber.New()
    loadEnv()

    app.Post("/produce", func(ctx *fiber.Ctx) error {
		body := ctx.Body()

        // Try to unmarshal the body into a single InfoData struct
        var singleInfoData InfoDataFromRust
        if err := json.Unmarshal(body, &singleInfoData); err == nil {
            // Handle single object
            log.Printf("Processing single InfoData object")
            if err := processData(singleInfoData,ctx); err != nil {
                log.Println("Error processing InfoData:", err)
                return ctx.Status(fiber.StatusInternalServerError).SendString(err.Error())
            }
            return ctx.SendString("Data processed successfully")
        }

        // Try to unmarshal the body into a slice of InfoData structs
        var multipleInfoData []InfoDataFromRust
        if err := json.Unmarshal(body, &multipleInfoData); err == nil {
            // Handle array of objects
            log.Printf("Processing array of %d InfoData objects", len(multipleInfoData))
            for i, data := range multipleInfoData {
                log.Printf("Processing object %d of %d", i+1, len(multipleInfoData))
                if err := processData(data,ctx); err != nil {
                    log.Println("Error processing InfoData:", err)
                    return ctx.Status(fiber.StatusInternalServerError).SendString(err.Error())
                }
            }
            return ctx.SendString("All data processed successfully")
        }

        // If neither unmarshalling succeeded
        log.Println("Invalid JSON format received")
        return ctx.Status(fiber.StatusBadRequest).SendString("Invalid JSON format")
    })

    log.Println("Server starting on :8080...")
    log.Fatal(app.Listen(":8080"))
}

func sendToKafka(message []byte, ctx *fiber.Ctx) error {
	
    userName := os.Getenv("KAFKA_USER_NAME")
    password := os.Getenv("KAFKA_PASSWORD")
	mechanism, err := scram.Mechanism(scram.SHA256, userName, password)
    // userName := trackTIme-2
    // password :-9g72zV0EcLbA50v6jnnmfiFRvwqUKZ
	if err != nil {
		log.Fatalf("Error creating SCRAM mechanism: %v", err)
	}

	writerConfig := kafka.WriterConfig{
		Brokers: []string{os.Getenv("KAFKA_BROKER")},
		Topic:   os.Getenv("TOPIC"),
		Dialer: &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		},
	}

	kafkaWriter := kafka.NewWriter(writerConfig)
	defer kafkaWriter.Close()

	// Produce the message to Kafka
	err = kafkaWriter.WriteMessages(ctx.Context(), kafka.Message{
		Value: message,
	})

	if err != nil {
		log.Printf("Failed to produce message to Kafka: %v", err)
		return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to produce message to Kafka: " + err.Error())
	}
	
	log.Printf("Message produced to Kafka successfully. Message size: %d bytes", len(message))
	return nil
}

// NEW: Upload WebP screenshot to Wasabi
func uploadWebPToWasabi(infoData InfoDataFromRust, ctx *fiber.Ctx) error {
    decodedScreenshot, err := base64.StdEncoding.DecodeString(infoData.ScreenshotWebP)

    log.Printf("Size of image being uploaded: %d bytes", len(decodedScreenshot))
    if err != nil {
        log.Printf("Base64 decode error for screenshot: %v", err)
        return fmt.Errorf("failed to decode base64 screenshot: %v", err)
    }
    // Save locally (optional for debugging)
    localDir := "./screenshots_webp"
    if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create local directory: %v", err)
    }
    localFilePath := filepath.Join(localDir, fmt.Sprintf("%s.webp", infoData.ActivityUUID))
    if err := ioutil.WriteFile(localFilePath, decodedScreenshot, 0644); err != nil {
        log.Printf("⚠️ Warning: Failed to save local copy: %v", err)
    } else {
        log.Printf("💾 Local copy saved: %s", localFilePath)
    }

    screenshotObjectKey := infoData.ScreenshotUID  // Use the mapped WebP UID
    log.Printf("infoData - ActivityUUID: %s, UserUID: %s, OrganizationID: %s", infoData.ActivityUUID, infoData.UserUID, infoData.OrganizationID)

	wasabiEndpoint := os.Getenv("S3_ENDPOINT")
	wasabiAccessKey := os.Getenv("WASABI_ACCESS_KEY")
	wasabiSecretKey := os.Getenv("WASABI_SECRET_KEY")
	wasabiBucket := os.Getenv("WASABI_BUCKET_NAME")

	// wasabiObjectKey := "screenshots/" + time.Now().Format("2006-01-02/15-04-05") + ".png"

	log.Println("Connecting to Wasabi...")

	// Customize the HTTP client for AWS session with a timeout
    httpClient := &http.Client{
        Timeout: 10 * time.Second, // Set the desired timeout duration here
    }

	awsConfig := &aws.Config{
        Region:           aws.String("us-west-1"), // Make sure to use the correct region
        Credentials:      credentials.NewStaticCredentials(wasabiAccessKey, wasabiSecretKey, ""),
        Endpoint:         aws.String(wasabiEndpoint),
        DisableSSL:       aws.Bool(true),
        S3ForcePathStyle: aws.Bool(true),
        HTTPClient:       httpClient, // Use the custom HTTP client with timeout
    }

	 // Create a new AWS session using the customized awsConfig
	 sess, err := session.NewSession(awsConfig)
	 if err != nil {
		 return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to create AWS session: " + err.Error())
	 }

	// Create an S3 client
	s3Client := s3.New(sess)

	log.Println("Connected to Wasabi.")

	// Create a bucket if it doesn't exist
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(wasabiBucket),
	})
	log.Println("Image uploading to wasabi.....")

    if err != nil {
        if awsErr, ok := err.(awserr.Error); ok {
            // Prints out the AWS error code and message
            log.Printf("AWS Error: %s - %s", awsErr.Code(), awsErr.Message())
            if reqErr, ok := err.(awserr.RequestFailure); ok {
                // A RequestFailure is an AWS error with additional information like status code
                log.Printf("Request Error: Code - %d, Request ID - %s", reqErr.StatusCode(), reqErr.RequestID())
            }
        } else {
            // Generic error handling
            log.Printf("Error: %s", err.Error())
        }
        return fmt.Errorf("failed to upload screenshot to Wasabi: %v", err)
    }

	log.Println("📤 WebP upload started...")
	
	 // Upload the WebP to Wasabi with correct ContentType and caching
	 _, err = s3Client.PutObject(&s3.PutObjectInput{
        Bucket:       aws.String(wasabiBucket),
        Key:          aws.String(screenshotObjectKey),
        Body:         bytes.NewReader(decodedScreenshot),
        ContentType:  aws.String("image/webp"),
        CacheControl: aws.String("public, max-age=31536000"), // 1 year cache
    })
	
	if err != nil {
        log.Printf("❌ Error uploading WebP to Wasabi: %v", err)
        return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to upload WebP to Wasabi: " + err.Error())
    }
    log.Printf("✅ WebP uploaded successfully: %s (%d bytes)", screenshotObjectKey, len(decodedScreenshot))

    // // After uploading the original image successfully
    // err = compressAndUploadImage(s3Client, wasabiBucket, screenshotObjectKey, decodedScreenshot)
    // if err != nil {
    //     log.Printf("Failed to compress and upload image: %v", err)
    //     return ctx.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to compress and upload image: %v", err))
    // }

    return nil
}

// NEW: Upload JPEG thumbnail to Wasabi
func uploadJPEGThumbnailToWasabi(infoData InfoDataFromRust, ctx *fiber.Ctx) error {
    decodedThumbnail, err := base64.StdEncoding.DecodeString(infoData.ThumbnailJPEG)

    if err != nil {
        log.Printf("Error decoding base64 data: %v", err)
        return err
    }

    log.Printf("📏 Thumbnail size: %d bytes", len(decodedThumbnail))

    // Save locally (optional for debugging)
    localDir := "./thumbnails_jpeg"
    if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create local directory for thumbnail: %v", err)
    }
    localFilePathForThumbnail := filepath.Join(localDir, fmt.Sprintf("%s.jpg", infoData.ActivityUUID))
    if err := ioutil.WriteFile(localFilePathForThumbnail, decodedThumbnail, 0644); err != nil {
        log.Printf("⚠️ Warning: Failed to save local thumbnail: %v", err)
    } else {
        log.Printf("💾 Local thumbnail saved: %s", localFilePathForThumbnail)
    }

    thumbnailObjectKey := infoData.ThumbnailUID  // Use the mapped JPEG UID

	wasabiEndpoint := os.Getenv("S3_ENDPOINT")
	wasabiAccessKey := os.Getenv("WASABI_ACCESS_KEY")
	wasabiSecretKey := os.Getenv("WASABI_SECRET_KEY")
	wasabiBucket := os.Getenv("WASABI_BUCKET_NAME")

    log.Println("Connecting to Wasabi...")

	// Customize the HTTP client for AWS session with a timeout
    httpClient := &http.Client{
        Timeout: 10 * time.Second, // Set the desired timeout duration here
    }

	awsConfig := &aws.Config{
        Region:           aws.String("us-west-1"), // Make sure to use the correct region
        Credentials:      credentials.NewStaticCredentials(wasabiAccessKey, wasabiSecretKey, ""),
        Endpoint:         aws.String(wasabiEndpoint),
        DisableSSL:       aws.Bool(true),
        S3ForcePathStyle: aws.Bool(true),
        HTTPClient:       httpClient, 
    }

     // Create a new AWS session using the customized awsConfig
	 sess, err := session.NewSession(awsConfig)
	 if err != nil {
		 return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to create AWS session: " + err.Error())
	 }
    
     //created a client for thumbnail uploading
     s3Client := s3.New(sess)

    log.Println("Connected to Wasabi.")

	// Create a bucket if it doesn't exist
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(wasabiBucket),
	})

	log.Println("thumbnail uploading to wasabi.....")

    if err != nil {
        if awsErr, ok := err.(awserr.Error); ok {
            // Prints out the AWS error code and message
            log.Printf("AWS Error: %s - %s", awsErr.Code(), awsErr.Message())
            if reqErr, ok := err.(awserr.RequestFailure); ok {
                // A RequestFailure is an AWS error with additional information like status code
                log.Printf("Request Error: Code - %d, Request ID - %s", reqErr.StatusCode(), reqErr.RequestID())
            }
        } else {
            // Generic error handling
            log.Printf("Error: %s", err.Error())
        }
        return fmt.Errorf("failed to upload screenshot to Wasabi: %v", err)
    }

    log.Println("📤 JPEG thumbnail upload started...")
	
	 // Upload the JPEG thumbnail to Wasabi
	 _, err = s3Client.PutObject(&s3.PutObjectInput{
        Bucket:       aws.String(wasabiBucket),
        Key:          aws.String(thumbnailObjectKey),
        Body:         bytes.NewReader(decodedThumbnail),
        ContentType:  aws.String("image/jpeg"),
        CacheControl: aws.String("public, max-age=31536000"), // 1 year cache
    })

    if err != nil {
        log.Printf("❌ Error uploading JPEG thumbnail to Wasabi: %v", err)
        return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to upload JPEG thumbnail to Wasabi: " + err.Error())
    }
    
    log.Printf("✅ JPEG thumbnail uploaded successfully: %s (%d bytes)", thumbnailObjectKey, len(decodedThumbnail))
    return nil
}

// func compressAndUploadImage(s3Client *s3.S3, bucketName, objectKey string, imageBuffer []byte) error {
//     img, _, err := image.Decode(bytes.NewReader(imageBuffer))
//     if err != nil {
//         return fmt.Errorf("image decode error: %v", err)
//     }

//     // Resize the image to a new width while maintaining aspect ratio
//     compressedImage := resize.Resize(1024, 0, img, resize.Lanczos3)

//     var compressedBuffer bytes.Buffer
//     err = png.Encode(&compressedBuffer, compressedImage)
//     if err != nil {
//         return fmt.Errorf("image encode error: %v", err)
//     }

//     // Define the compressed object key
//     compressedObjectKey := strings.Replace(objectKey, "screenshots/", "compressed_images/", 1)

//     // Upload the compressed image to Wasabi
//     _, err = s3Client.PutObject(&s3.PutObjectInput{
//         Bucket:      aws.String(bucketName),
//         Key:         aws.String(compressedObjectKey),
//         Body:        bytes.NewReader(compressedBuffer.Bytes()),
//         ContentType: aws.String("image/png"),
//     })
//     if err != nil {
//         return fmt.Errorf("failed to upload compressed screenshot to Wasabi: %v", err)
//     }

//     fmt.Printf("Compressed image uploaded successfully: %s\n", compressedObjectKey)
//     return nil
// }