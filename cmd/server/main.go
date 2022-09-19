package main

import (
	"log"

	"github.com/kartpop/dclog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
