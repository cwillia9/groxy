package groxy

import(
	"fmt"
	"testing"
	"github.com/Shopify/sarama"
)



func TestBasicConsumerFunctionality(t *testing.T) {
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


	ctx, err := NewContext("my_topic", []string{broker0.Addr()}, []string{leader.Addr()}, "wilco09", "produce_topic", 30)

	fmt.Println(ctx)
	fmt.Println(err)


	ctx.Produce([]byte("ok"), []byte("hello"))

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

	ctx, err := NewContext("my_topic", []string{seedBroker.Addr()}, []string{seedBroker.Addr()}, "wilco09", "produce_topic", 30)

	fmt.Println(ctx)
	fmt.Println(err)

	seedBroker.Close()
	leader.Close()
}