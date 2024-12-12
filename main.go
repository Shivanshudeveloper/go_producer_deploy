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


// var producer sarama.SyncProducer // Upstash Kafka producer

// JSON Data in go should contain capitalized keys for example `thumbnail` should not be like this it should be 
// `Thumbnail`it should be capital not small and if it's small then go will not be able to read this data field because 
// json marshal in go always reads the capitalize key's not small one at all learning 
// from experience and bug which took whole day to fix. day learned this thing --> 19/02/2024 by a learner

type InfoData struct {
    ActivityUUID       string    `json:"activity_uuid"`
    UserUID            string    `json:"user_id"`
    OrganizationID     string    `json:"organization_id"`
    Timestamp          time.Time `json:"timestamp"`
    AppName            string    `json:"app_name"`
    URL                string    `json:"url"`
    PageTitle          string    `json:"page_title"`
    Screenshot         string    `json:"screenshot"`
    Thumbnail          string    `json:"thumbnail"`
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
    Device_user_name   string    `json:"device_user_name"`
}

// processData handles the common logic for processing InfoData, whether it comes from a single object or an array.
func processData(infoData InfoData,ctx *fiber.Ctx) error {
    // Your logic for processing each InfoData, including uploading to Wasabi and sending to Kafka
    // Note: This function should NOT directly interact with `ctx`.
    // Instead, return errors for the caller to handle appropriately.

    // Example:
    err := uploadToWasabi(infoData,ctx)
    if err != nil {
        return fmt.Errorf("failed to upload Screenshot to Wasabi: %v", err)
    }

    err = uploadThumbnailToWasabi(infoData,ctx)
    if err != nil {
        return fmt.Errorf("failed to upload Thumbnail to Wasabi: %v", err)
    }

    message, err := json.Marshal(infoData)
    if err != nil {
        return fmt.Errorf("failed to marshal infoData to JSON: %v", err)
    }

    // Assuming you have adapted your sendToKafka function to not require ctx as an argument
    err = sendToKafka(message,ctx)
    if err != nil {
        return fmt.Errorf("failed to send message to Kafka: %v", err)
    }

    return nil
}


func main() {
    app := fiber.New()
    loadEnv()

    app.Post("/produce", func(ctx *fiber.Ctx) error {
		body := ctx.Body()

        // Try to unmarshal the body into a single InfoData struct
        var singleInfoData InfoData
        if err := json.Unmarshal(body, &singleInfoData); err == nil {
            // Handle single object
            if err := processData(singleInfoData,ctx); err != nil {
                log.Println("Error processing InfoData:", err)
                return ctx.Status(fiber.StatusInternalServerError).SendString(err.Error())
            }
            return ctx.SendString("Data processed successfully")
        }

        // Try to unmarshal the body into a slice of InfoData structs
        var multipleInfoData []InfoData
        if err := json.Unmarshal(body, &multipleInfoData); err == nil {
            // Handle array of objects
            for _, data := range multipleInfoData {
                if err := processData(data,ctx); err != nil {
                    log.Println("Error processing InfoData:", err)
                    return ctx.Status(fiber.StatusInternalServerError).SendString(err.Error())
                }
            }
            return ctx.SendString("All data processed successfully")
        }

        // If neither unmarshalling succeeded
        return ctx.Status(fiber.StatusBadRequest).SendString("Invalid JSON format")
    })

    log.Fatal(app.Listen(":8080"))
}



func sendToKafka(message []byte, ctx *fiber.Ctx) error {
	
    userName := os.Getenv("KAFKA_USER_NAME");
    password := os.Getenv("KAFKA_PASSWORD");
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
			return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to produce message to Kafka: " + err.Error())
		}
		
		fmt.Println("Message produced to Kafka.")

		return nil
}

func uploadToWasabi(infoData InfoData,ctx *fiber.Ctx) error {

    // log.Printf("thumbnailDataBase64 data: %s",infoData.Thumbnail);
    
	// decodedThumbnail, err := base64.StdEncoding.DecodeString(infoData.Thumbnail)

    // log.Printf("decodedThumbnail: %s",decodedThumbnail);

    decodedScreenshot, err := base64.StdEncoding.DecodeString(infoData.Screenshot)


    log.Printf("Size of image being uploaded: %d bytes", len(decodedScreenshot))
    if err != nil {
        log.Printf("Base64 decode error for screenshot: %v", err)
        return fmt.Errorf("failed to decode base64 screenshot: %v", err)
    }
    // Specify the local directory where screenshots will be saved
    localDir := "./screenshots"
    if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create local directory: %v", err)
    }

    // Create a unique filename for the local screenshot
    localFilePath := filepath.Join(localDir, fmt.Sprintf("%s.png", infoData.ActivityUUID))
    
    // Save the screenshot locally
    if err := ioutil.WriteFile(localFilePath, decodedScreenshot, 0644); err != nil {
        return fmt.Errorf("failed to save screenshot locally: %v", err)
    }
    log.Printf("Screenshot saved locally: %s", localFilePath)

    // screenshotObjectKey := "screenshots/" + infoData.ActivityUUID + "|"+ infoData.UserUID + ".png"
    log.Printf("screenshot name",infoData.ScreenshotUID);

    // log.Printf("data",infoData.ActivityUUID,infoData.UserUID)
    // screenshotObjectKey := infoData.ScreenshotUID;
    // screenshotObjectKey := "screenshots/" + infoData.ActivityUUID + "|" + infoData.UserUID + ".jpeg";
    screenshotObjectKey := fmt.Sprintf("screenshots/%s|%s.png", infoData.ActivityUUID, infoData.UserUID)
    log.Printf("infoData",infoData.ActivityUUID,infoData.UserUID,infoData.OrganizationID)

	wasabiEndpoint := os.Getenv("S3_ENDPOINT");
	wasabiAccessKey := os.Getenv("WASABI_ACCESS_KEY"); 
	wasabiSecretKey := os.Getenv("WASABI_SECRET_KEY"); 
	wasabiBucket := os.Getenv("WASABI_BUCKET_NAME"); 

	// wasabiObjectKey := "screenshots/" + time.Now().Format("2006-01-02/15-04-05") + ".png"

	fmt.Println("Connecting to Wasabi...")

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

	fmt.Println("Connected to Wasabi.")

	// Create a bucket if it doesn't exist
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(wasabiBucket),
	})
	fmt.Println("Image uploading to wasabi.....");


    

	// if err != nil {
	// 	return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to create Wasabi bucket: " + err.Error())
	// }

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

	fmt.Println("Image uploading Started");
	
	 // Upload the screenshot to Wasabi
	 _, err = s3Client.PutObject(&s3.PutObjectInput{
        Bucket:      aws.String(wasabiBucket),
        Key:         aws.String(screenshotObjectKey),
        Body:        bytes.NewReader(decodedScreenshot),
        ContentType: aws.String("image/png"),
    })
	

	if err != nil {
        log.Printf("Error uploading image to Wasabi: %v", err)
        return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to upload screenshot to Wasabi: " + err.Error())
    }
    log.Printf("Image successfully uploaded to Wasabi: %s", screenshotObjectKey)
	fmt.Println("Image uploaded");

    // Upload the screenshot to Wasabi as before
    log.Printf("Screenshot uploaded to Wasabi: %s", screenshotObjectKey)

    // // After uploading the original image successfully
    // err = compressAndUploadImage(s3Client, wasabiBucket, screenshotObjectKey, decodedScreenshot)
    // if err != nil {
    //     log.Printf("Failed to compress and upload image: %v", err)
    //     return ctx.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Failed to compress and upload image: %v", err))
    // }

    return nil
}

func uploadThumbnailToWasabi(infoData InfoData, ctx*fiber.Ctx) error {


    decodedScreenshot, err := base64.StdEncoding.DecodeString(infoData.Thumbnail);

    if err != nil {
        log.Printf("Error decoding base64 data: %v", err)
        return err
    }

    // if err != nil {
    //     return fmt.Errorf("failed to decode base64 screenshot for thumbnail: %v", err)
    // }

    localDir := "./thumbnailFolder"
    if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create local directory for thumbnail: %v", err)
    }

    localFilePathForThumbnail := filepath.Join(localDir, fmt.Sprintf("%s.jpeg", infoData.ActivityUUID))

    if err := ioutil.WriteFile(localFilePathForThumbnail, decodedScreenshot, 0644); err != nil {
        return fmt.Errorf("failed to save thumbnail Screenshot locally: %v", err)
    }

    log.Printf("Thumbnail saved locally: %s", localFilePathForThumbnail);

    screenshotObjectKey := "thumbnails/" + infoData.ActivityUUID + "|" + infoData.UserUID + ".jpeg";

	wasabiEndpoint := os.Getenv("S3_ENDPOINT");
	wasabiAccessKey := os.Getenv("WASABI_ACCESS_KEY"); 
	wasabiSecretKey := os.Getenv("WASABI_SECRET_KEY"); 
	wasabiBucket := os.Getenv("WASABI_BUCKET_NAME"); 

    fmt.Println("Connecting to Wasabi...")

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

    fmt.Println("Connected to Wasabi.")

	// Create a bucket if it doesn't exist
	_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(wasabiBucket),
	})

	fmt.Println("thumbnail uploading to wasabi.....");

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

    fmt.Println("thumbnail uploading Started");
	
	 // Upload the screenshot to Wasabi
	 _, err = s3Client.PutObject(&s3.PutObjectInput{
        Bucket:      aws.String(wasabiBucket),
        Key:         aws.String(screenshotObjectKey),
        Body:        bytes.NewReader(decodedScreenshot),
        ContentType: aws.String("image/jpeg"),
    })

    if err != nil {
        return ctx.Status(fiber.StatusInternalServerError).SendString("Failed to upload thumbnail to Wasabi: " + err.Error())
    }
    
	fmt.Println("thumbnail uploaded");

    // Upload the screenshot to Wasabi as before
    log.Printf("Thumbnail uploaded to Wasabi: %s", screenshotObjectKey)

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