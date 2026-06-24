package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	monolithURL       *url.URL
	moviesServiceURL  *url.URL
	eventsServiceURL  *url.URL
	migrationPercent  int
	gradualMigration  bool
)

func main() {
	var err error

	monolithURL, err = url.Parse(getEnv("MONOLITH_URL", "http://monolith:8080"))
	if err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}

	moviesServiceURL, err = url.Parse(getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"))
	if err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}

	eventsServiceURL, err = url.Parse(getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"))
	if err != nil {
		log.Fatalf("Invalid EVENTS_SERVICE_URL: %v", err)
	}

	migrationPercent, err = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT, defaulting to 0")
		migrationPercent = 0
	}

	gradualMigration = strings.ToLower(getEnv("GRADUAL_MIGRATION", "false")) == "true"

	log.Printf("Proxy starting. Migration percent: %d, Gradual migration: %v", migrationPercent, gradualMigration)

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", moviesProxyHandler)
	http.HandleFunc("/api/movies/", moviesProxyHandler)
	http.HandleFunc("/api/users", monolithProxyHandler)
	http.HandleFunc("/api/users/", monolithProxyHandler)
	http.HandleFunc("/api/payments", monolithProxyHandler)
	http.HandleFunc("/api/payments/", monolithProxyHandler)
	http.HandleFunc("/api/subscriptions", monolithProxyHandler)
	http.HandleFunc("/api/subscriptions/", monolithProxyHandler)
	http.HandleFunc("/api/events/", eventsProxyHandler)

	port := getEnv("PORT", "8000")
	log.Printf("Starting proxy on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Strangler Fig Proxy is healthy")
}

func moviesProxyHandler(w http.ResponseWriter, r *http.Request) {
	targetURL := monolithURL

	if gradualMigration {
		roll := rand.Intn(100)
		if roll < migrationPercent {
			targetURL = moviesServiceURL
			log.Printf("Routing /api/movies to movies-service (migration %d%%, roll %d)", migrationPercent, roll)
		} else {
			log.Printf("Routing /api/movies to monolith (migration %d%%, roll %d)", migrationPercent, roll)
		}
	} else {
		targetURL = monolithURL
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ServeHTTP(w, r)
}

func monolithProxyHandler(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(monolithURL)
	proxy.ServeHTTP(w, r)
}

func eventsProxyHandler(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(eventsServiceURL)
	proxy.ServeHTTP(w, r)
}
