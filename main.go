package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	batchSize   = 100000 // 100K records per batch
	workers     = 1     // Start with 1 worker, increase later
	lastIDFile  = "last_id.txt"
	exportDir   = "exports"
	logFilePath = "export.log"
)

func main() {
	// Setup logging to both console and file
	setupLogging()
	log.Println("📜 Logging started...")

	// Get user inputs
	mongoURI, dbName, collectionName := getUserInputs()

	// Connect to MongoDB
	log.Println("✅ Connecting to MongoDB...")
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("❌ Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.TODO())
	log.Println("✅ Successfully connected to MongoDB!")

	db := client.Database(dbName)
	collection := db.Collection(collectionName)

	// Create export directory
	if err := os.MkdirAll(exportDir, os.ModePerm); err != nil {
		log.Fatalf("❌ Failed to create directory: %v", err)
	}

	// Load last exported ID
	lastID := loadLastID()
	log.Printf("🔄 Resuming export from last ID: %v\n", lastID)

	startTime := time.Now()

	// Worker group
	var wg sync.WaitGroup
	workChan := make(chan primitive.ObjectID, workers) // Buffered channel

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go exportWorker(i, collection, exportDir, workChan, &wg)
	}

	// Feed initial work
	workChan <- lastID

	// Close work channel once all workers are done
	go func() {
		wg.Wait()
		close(workChan)
	}()

	// Wait for completion
	wg.Wait()

	log.Println("✅ Export completed successfully!")
	elapsedTime := time.Since(startTime)
	log.Printf("🚀 Total time taken: %s\n", elapsedTime)
}

// Worker function to export records in parallel
func exportWorker(workerID int, collection *mongo.Collection, exportDir string, workChan <-chan primitive.ObjectID, wg *sync.WaitGroup) {
	defer wg.Done()

	for lastID := range workChan {
		batchNum := 1
		for {
			var filter bson.M
			if !lastID.IsZero() {
				filter = bson.M{"_id": bson.M{"$gt": lastID}}
			} else {
				filter = bson.M{}
			}

			cursor, err := collection.Find(
				context.TODO(),
				filter,
				options.Find().SetLimit(batchSize).SetSort(bson.D{{"_id", 1}}),
			)
			if err != nil {
				log.Printf("❌ Worker %d: Failed to fetch data: %v\n", workerID, err)
				return
			}

			var results []bson.M
			if err := cursor.All(context.TODO(), &results); err != nil {
				log.Printf("❌ Worker %d: Failed to decode batch: %v\n", workerID, err)
				return
			}

			// Stop if no more data
			if len(results) == 0 {
				log.Printf("✅ Worker %d: No more records to export.\n", workerID)
				break
			}

			// Write batch to JSON file
			filePath := filepath.Join(exportDir, fmt.Sprintf("batch_%d_worker_%d.json", batchNum, workerID))
			file, err := os.Create(filePath)
			if err != nil {
				log.Printf("❌ Worker %d: Failed to create file: %v\n", workerID, err)
				return
			}
			encoder := json.NewEncoder(file)
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(results); err != nil {
				log.Printf("❌ Worker %d: Failed to write JSON: %v\n", workerID, err)
				return
			}
			file.Close()

			// Save last processed _id for resumption
			lastID = results[len(results)-1]["_id"].(primitive.ObjectID)
			saveLastID(lastID)

			// Print progress logs
			log.Printf("✅ Worker %d: Exported batch %d (%d records) -> %s\n", workerID, batchNum, len(results), filePath)

			batchNum++
		}
	}
}

// Save last processed _id to file
func saveLastID(lastID primitive.ObjectID) {
	file, err := os.Create(lastIDFile)
	if err != nil {
		log.Printf("⚠️ Warning: Failed to save last _id: %v\n", err)
		return
	}
	defer file.Close()

	_, err = file.WriteString(lastID.Hex())
	if err != nil {
		log.Printf("⚠️ Warning: Failed to write last _id to file: %v\n", err)
	}
}

// Load last processed _id from file
func loadLastID() primitive.ObjectID {
	data, err := os.ReadFile(lastIDFile)
	if err != nil {
		log.Println("🔄 No previous last_id found. Starting fresh...")
		return primitive.NilObjectID
	}

	lastID, err := primitive.ObjectIDFromHex(strings.TrimSpace(string(data)))
	if err != nil {
		log.Printf("⚠️ Warning: Invalid _id format in last_id.txt, starting from scratch.")
		return primitive.NilObjectID
	}

	log.Printf("🔄 Resuming export from last _id: %s\n", lastID.Hex())
	return lastID
}

// Get user inputs safely
func getUserInputs() (string, string, string) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter MongoDB connection string: ")
	mongoURI, _ := reader.ReadString('\n')
	mongoURI = strings.TrimSpace(mongoURI)

	fmt.Print("Enter database name: ")
	dbName, _ := reader.ReadString('\n')
	dbName = strings.TrimSpace(dbName)

	fmt.Print("Enter collection name: ")
	collectionName, _ := reader.ReadString('\n')
	collectionName = strings.TrimSpace(collectionName)

	return mongoURI, dbName, collectionName
}

// Setup logging to console and file
func setupLogging() {
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("⚠️ Warning: Failed to create log file, using default stdout\n")
		return
	}
	log.SetOutput(logFile)
	log.Println("📜 Logging started...")
}
