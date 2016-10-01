package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/cwillia9/groxy"
)

type MySerde struct {
	key []byte
}

func (s *MySerde) Serialize(r *http.Request) ([]byte, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

	v := r.FormValue("myvalue")
	fmt.Println("Got value of:", v)
	s.key = []byte("ok")
	return []byte(v), nil
}

func (s *MySerde) Deserialize(b []byte, w http.ResponseWriter) error {
	fmt.Println("Got bytes:", b)

	fmt.Fprintf(w, string(b))
	return nil
}

func (s *MySerde) Key(r *http.Request) ([]byte, error) {
	if s.key == nil {
		_, err := s.Serialize(r)
		if err != nil {
			return nil, err
		}
	}
	return s.key, nil
}

func main() {
	client, err := sarama.NewClient([]string{"localhost:9092"}, nil)
	if err != nil {
		fmt.Println(err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println("Failed to create consumer")
		return
	}
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		fmt.Println("Failed to create producer")
		return
	}
	partitionConsumer, err := consumer.ConsumePartition("req_topic", 0, sarama.OffsetNewest)

	go consumeAndProduce(partitionConsumer, producer)

	k := &groxy.KafkaContext{
		Client: client,
	}
	ctx, err := groxy.NewContext(k, "resp_topic", "localhost", "req_topic", 2000)
	if err != nil {
		fmt.Println(err)
	}

	http.Handle("/coltonstuff", groxy.NewFifoHandler(ctx, &MySerde{}))
	http.Handle("/samstuff", groxy.NewFifoHandler(ctx, &MySerde{}))
	err = http.ListenAndServe(":9999", nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
	fmt.Println("Leaving main loop")
}

func consumeAndProduce(pc sarama.PartitionConsumer, p sarama.AsyncProducer) {
	for m := range pc.Messages() {
		p.Input() <- &sarama.ProducerMessage{
			Topic: "resp_topic",
			Key:   sarama.StringEncoder("blah"),
			Value: sarama.ByteEncoder(m.Value),
		}
	}
}
