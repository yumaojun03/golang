package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// for test active connection
		time.Sleep(time.Second * 2)
		fmt.Fprintf(w, "Hello World, %v\n", time.Now())
	})

	s := &http.Server{
		Addr:           ":8080",
		Handler:        http.DefaultServeMux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		log.Printf("server start at: 127.0.0.1:8080")
		log.Println(s.ListenAndServe())
		log.Println("server shutdown")
	}()

	// Handle SIGINT, SIGTERM, SIGKILL, SIGHUP, SIGQUIT
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT)
	log.Println(<-ch)

	// Stop the service gracefully.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Println(s.Shutdown(ctx))

	// Wait gorotine print shutdown message
	time.Sleep(time.Second * 10)
	log.Println("done.")
}
