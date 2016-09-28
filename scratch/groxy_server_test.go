package main

import(
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func waitHundred(wg *sync.WaitGroup) {
	time.Sleep(100)
	if wg != nil {
		wg.Done()
	}
}

func TestBasicProducerFunctionality(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	mockProducer := mocks.NewAsyncProducer(t, cfg)
	mockProducer.ExpectInputAndFail(errors.New("ok"))
	mockProducer.Input() <- &sarama.ProducerMessage{
		Topic: "test_topic",
		Key: 	sarama.StringEncoder("this_key"),
		Value:	sarama.StringEncoder("this_value"),
	}

	select {
		case s := <- mockProducer.Successes():
			fmt.Println(s)
			fmt.Println("success")
		case e := <- mockProducer.Errors():
			fmt.Println(e)
			fmt.Println("error")
	}

	go waitHundred(&wg)
	wg.Wait()
}



func TestBasicConsumerFunctionality(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	mockFetchResponse.SetMessage("test_topic", 0, 123, sarama.StringEncoder("test_message_123"))
	mockFetchResponse.SetMessage("test_topic", 0, 124, sarama.StringEncoder("test_message_124"))
	mockFetchResponse.SetMessage("test_topic", 0, 125, sarama.StringEncoder("test_message_125"))

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 123),
		"FetchRequest": mockFetchResponse,
	})

	fmt.Println(broker0.Addr())
	master, err := sarama.NewConsumer([]string{broker0.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := master.ConsumePartition("test_topic", 0, 123)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		select {
		case message := <-consumer.Messages():
			fmt.Println("key:", string(message.Key), "value:", string(message.Value))
			assertMessageOffset(t, message, int64(123 + i))
		case err := <-consumer.Errors():
			t.Error(err)
		}
	}

	safeClose(t, consumer)
	safeClose(t, master)
	broker0.Close()
}

func TestBldConnection(t *testing.T) {
	client, err := sarama.NewClient([]string{"bld-kafka8-01.f4tech.com:9092","bld-kafka8-03.f4tech.com:9092","bld-kafka8-03.f4tech.com:9092"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(client.Partitions("alm-spans"))
	fmt.Println(client.Leader("alm-spans", 2))

}

func assertMessageOffset(t *testing.T, msg *sarama.ConsumerMessage, expectedOffset int64) {
	if msg.Offset != expectedOffset {
		t.Errorf("Incorrect message offset: expected=%d, actual=%d", expectedOffset, msg.Offset)
	}
}

func safeClose(t testing.TB, c io.Closer) {
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}




