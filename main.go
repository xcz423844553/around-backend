package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

const (
	POST_INDEX = "post"
	ES_URL     = "http://35.239.60.232:9200"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

func main() {
	fmt.Println("started-service")
	createIndexIfNotExist()
	http.HandleFunc("/post", handlerPost)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)

	}
	fmt.Fprintf(w, "Post received: %s\n", p.Message)
}
