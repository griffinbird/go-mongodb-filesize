package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

)

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

	buildInfoCmd := bson.D{bson.E{Key: "buildInfo", Value: 1}}
	var buildInfoDoc bson.M
	if err := database.RunCommand(ctx, buildInfoCmd).Decode(&buildInfoDoc); err != nil {
		log.Printf("Failed to run buildInfo command: %v", err)
	}
	log.Println("Database version:", buildInfoDoc["version"])

	jsonFile, _ := os.Stat(os.Getenv("FILENAME"))
	jsonFileSize := jsonFile.Size()
	jsonFileSizeMB := jsonFileSize

	log.Printf("The JSON file size is %d bytes\n", jsonFileSizeMB)

	fileContent, err := os.Open(os.Getenv("FILENAME"))

	if err != nil {
		log.Fatal(err)
		return
	}

	log.Printf("%s opened successfully", os.Getenv("FILENAME"))

	defer fileContent.Close()

	byteResult, _ := ioutil.ReadAll(fileContent)

	var res map[string]interface{}
	json.Unmarshal([]byte(byteResult), &res)

	_, err = collection.InsertOne(context.TODO(), res)
	
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Successfully inserted into Mongo\n\n%v\n", res)	
}
