package groxy

import(
	"fmt"
	"github.com/Shopify/sarama"
	hash "github.com/aviddiviner/go-murmur"
)
// consume topic
// consume brokers
// poduce brokers
// self hostname
// timeout
// produce topic

type Context struct {
	consumeTopic string
	consumeBrokers []string
	produceBrokers []string
	selfHostname string
	timeout int
	produceTopic string

	client   sarama.Client
	producer sarama.AsyncProducer
	brokerConsumer sarama.Consumer 
	partitionConsumer sarama.PartitionConsumer

	httpChannels map[string]chan []byte
}

func NewContext(consumeTopic string,
				consumeBrokers []string,
				produceBrokers []string,
				hostname string,
				produceTopic string,
				timeout int) (*Context, error) {
	// Do we need to pass in config??
	client, err := sarama.NewClient(consumeBrokers, nil)
	if err != nil {
		return nil, err
	}

	fmt.Println("here")
	consumePartitions, err := client.Partitions(consumeTopic)
	if err != nil {
		return nil, err 
	}
	fmt.Println("here")
	// Figure out what seed value to use
	consumePartition := hash.MurmurHash2([]byte(hostname), 123) % uint32(len(consumePartitions))

	leadBroker, err := client.Leader(consumeTopic, int32(consumePartition))
	fmt.Println("leadBroker", leadBroker.ID())
	if err != nil {
		return nil, err 
	}
	fmt.Println("here")
	// Again, do we need config here?
	master, err := sarama.NewConsumer([]string{leadBroker.Addr()}, nil)
	if err != nil {
		return nil, err
	}
	fmt.Println("here")

	// Hardcoding OffsetNewest here
	consumer, err := master.ConsumePartition(consumeTopic, int32(consumePartition), sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}
	fmt.Println("here")
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &Context{
		consumeTopic: consumeTopic,
		consumeBrokers: consumeBrokers,
		produceBrokers: produceBrokers,
		selfHostname: hostname,
		timeout: timeout,
		produceTopic: produceTopic,
		client: client,
		producer: producer,
		brokerConsumer: master,
		partitionConsumer: consumer,
		httpChannels: make(map[string]chan []byte),
	}, nil
}

func (ctx *Context) Produce(key, value []byte) {
	ctx.producer.Input() <- &sarama.ProducerMessage{
		Topic: 	ctx.produceTopic,
		Key: 	sarama.ByteEncoder(key),
		Value:	sarama.ByteEncoder(value),
	}
}