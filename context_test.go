package groxy

import(
	"fmt"
	//"log"
	//"os"
	"testing"
	"time"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)



func TestBasicConsumerFunctionality(t *testing.T) {
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	broker0 := sarama.NewMockBroker(t, 0)
	leader := sarama.NewMockBroker(t, 1)

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	mockFetchResponse.SetMessage("my_topic", 0, 123, sarama.StringEncoder("test_message_123"))
	mockFetchResponse.SetMessage("my_topic", 0, 124, sarama.StringEncoder("test_message_124"))
	mockFetchResponse.SetMessage("my_topic", 0, 125, sarama.StringEncoder("test_message_125"))


	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()).
			SetBroker(leader.Addr(), leader.BrokerID()).
			SetLeader("produc_topic", 0, leader.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})

	client, err := sarama.NewClient([]string{broker0.Addr(), leader.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	k := &KafkaContext{Client: client}

	cfg := sarama.NewConfig()

	cfg.Producer.Return.Successes = true
	producer := mocks.NewAsyncProducer(t, cfg)
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()
	producer.ExpectInputAndSucceed()

	k.Producer = producer

	consumer := mocks.NewConsumer(t, nil)
	partitionconsumer := consumer.ExpectConsumePartition("my_topic", 0, sarama.OffsetNewest)
	fmt.Println(consumer)
	k.Consumer = consumer
	k.PartitionConsumer = partitionconsumer

	ctx, err := NewContext(k, "my_topic", "wilco09", "produce_topic", 300)

	receiveChan := make(chan []byte)

	go func(){
		for v := range receiveChan {
			Logger.Info("received message:", string(v))
		}
	}()

	go func() {
		for v := range ctx.Kafka.Producer.Successes() {
			Logger.Info(fmt.Sprint(v))
			val, _ := v.Value.Encode()
			partitionconsumer.YieldMessage(&sarama.ConsumerMessage{
				Key: []byte("wilco09"),
				Value: val,
				})
		}
	}()

	ctx.Produce(receiveChan, []byte("ok"), []byte("hello"))
	ctx.Produce(receiveChan, []byte("ok"), []byte("hello"))
	ctx.Produce(receiveChan, []byte("ok"), []byte("hello"))
	ctx.Produce(receiveChan, []byte("ok"), []byte("hello"))
	ctx.Produce(receiveChan, []byte("ok"), []byte("hello"))


	time.Sleep(100 * time.Millisecond)
	client.Close()
	producer.Close()
	broker0.Close()
	leader.Close()
}


func NotTestLessBasicFunctionality(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	seedBroker.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition("my_topic", 0, sarama.ErrNoError)
	leader.Returns(prodSuccess)

	client, err := sarama.NewClient([]string{seedBroker.Addr(), leader.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	k := &KafkaContext{Client: client}
	_, _ = NewContext(k, "my_topic", "wilco09", "produce_topic", 30)

	
	

	seedBroker.Close()
	leader.Close()
}