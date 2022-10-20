package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
	"unsafe"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type requestStatistics struct {
	ActivityID                                  string
	CommandName                                 string
	EstimatedDelayFromRateLimitingInMilliseconds int
	RequestCharge                               float64
	RequestDurationInMilliseconds               int
	RetriedDueToRateLimiting                    bool
	OK                                          int
}

func main() {
	// Set client options and connect to MongoDB
	err := godotenv.Load(".env")
		if err != nil {
			log.Printf("error loading .env file")
		}
	
	client, err := mongo.NewClient(options.Client().ApplyURI(os.Getenv("MONGODB_URI")))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.TODO(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	
	// Mongodb Database/collection
	database := client.Database("testDB")
	collection := database.Collection("testCollection")

	// Get Mongo verison information and print
	buildInfoCmd := bson.D{bson.E{Key: "buildInfo", Value: 1}}
	var buildInfoDoc bson.M
	if err := database.RunCommand(ctx, buildInfoCmd).Decode(&buildInfoDoc); err != nil {
		log.Printf("Failed to run buildInfo command: %v", err)
	}
	log.Println("Database version:", buildInfoDoc["version"])

	// Obtain size of the file and print
	jsonFile, _ := os.Stat(os.Getenv("FILENAME"))
	jsonFileSize := jsonFile.Size()
	jsonFileSizeMB := jsonFileSize

	log.Printf("The JSON file size is %d bytes\n", jsonFileSizeMB)

	// Open the file and read it. 
	fileContent, err := os.Open(os.Getenv("FILENAME"))
	if err != nil {
		log.Fatal(err)
		return
	}

	log.Printf("%s opened successfully", os.Getenv("FILENAME"))

	defer fileContent.Close()

	byteResult, err := ioutil.ReadAll(fileContent)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Create a map where the key is string and the value is of type
	// interface. interface() is an empty interface and any object can
	// be stored in it.
	var res map[string]interface{}

	// Stores the JSON data from the file into res
	json.Unmarshal([]byte(byteResult), &res)
	
	_, err = collection.InsertOne(context.TODO(), res)
	
	if err != nil {
		log.Fatal(err)
	}

	//log.Printf("Successfully inserted into Mongo\n\n%v\n", res)
	
	statistics, err := GetLastRequestStats(client, database)
	if err != nil {
		log.Fatal(err)
	}
	requestChargeValue := fmt.Sprintf("ActivityID: %v, RequestCharge: %.2f %dms\n ", statistics.ActivityID, statistics.RequestCharge, statistics.RequestDurationInMilliseconds)
	log.Printf("%v",requestChargeValue)
}

func GetLastRequestStats(client *mongo.Client, database *mongo.Database) (*requestStatistics, error) {
	ctx := context.TODO()

	statistics := requestStatistics{}
	map1 := map[string]interface{}{"getLastRequestStatistics": "1"}
	err := database.RunCommand(ctx, map1, nil).Decode(&statistics)
	if err != nil {
		return nil, err
	}
	return &statistics, nil
}
