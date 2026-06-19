package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var (
	kafkaBrokers []string
	producer     sarama.AsyncProducer
	consumer     sarama.Consumer
)

type EventResponse struct {
	Status  string `json:"status"`
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

func main() {
	kafkaBrokers = parseBrokers()

	var err error
	producer, err = createProducer()
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	consumer, err = createConsumer()
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	topics := []string{"movie-events", "user-events", "payment-events"}
	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go consumeTopic(topic, &wg)
	}

	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events microservice on port %s", port)
	log.Printf("Kafka brokers: %v", kafkaBrokers)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: nil,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	wg.Wait()
}

func parseBrokers() []string {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := []string{brokersEnv}
	return brokers
}

func createProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewAsyncProducer(kafkaBrokers, config)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range producer.Errors() {
			log.Printf("Kafka producer error: %v", err)
		}
	}()

	return producer, nil
}

func createConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer, err := sarama.NewConsumer(kafkaBrokers, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func consumeTopic(topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("Failed to get partitions for topic %s: %v", topic, err)
		return
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Failed to start consumer for topic %s partition %d: %v", topic, partition, err)
			return
		}
		defer pc.Close()

		go func(p sarama.PartitionConsumer) {
			for message := range p.Messages() {
				log.Printf("[Consumer] Topic: %s | Partition: %d | Offset: %d | Message: %s",
					message.Topic, message.Partition, message.Offset, string(message.Value))
			}
		}(pc)
	}

	log.Printf("Consumer started for topic: %s", topic)

	select {}
}

func publishEvent(topic string, payload map[string]interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %v", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(data),
	}

	producer.Input() <- msg
	select {
	case <-producer.Successes():
		log.Printf("Message published to topic %s", topic)
		return nil
	case err := <-producer.Errors():
		log.Printf("Failed to publish to topic %s: %v", topic, err)
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout publishing to topic %s", topic)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	handleEvent(w, r, "movie-events")
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	handleEvent(w, r, "user-events")
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	handleEvent(w, r, "payment-events")
}

func handleEvent(w http.ResponseWriter, r *http.Request, topic string) {
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("Failed to decode request body: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(EventResponse{
			Status:  "error",
			Topic:   topic,
			Message: "invalid request body",
		})
		return
	}

	if err := publishEvent(topic, payload); err != nil {
		log.Printf("Failed to publish event to %s: %v", topic, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(EventResponse{
			Status:  "error",
			Topic:   topic,
			Message: err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:  "success",
		Topic:   topic,
		Message: "event published",
	})
}
