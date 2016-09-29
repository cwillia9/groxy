package groxy

import(
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	hash "github.com/aviddiviner/go-murmur"
)
// consume topic
// consume brokers
// poduce brokers
// self hostname
// timeout
// produce topic

type idIncrementer struct {
	seed 		int64
	count    	int32
	mu 			sync.Mutex
}

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

	httpChannels map[int32]chan []byte
	muhttpChannels *sync.Mutex

	idInc *idIncrementer
}

const GROXY_MAGIC_STRING = "GrOxy"

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
	
	consumePartition := identifyPartition(uint32(len(consumePartitions)), []byte(hostname))

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
		httpChannels: make(map[int32]chan []byte),
		muhttpChannels: new(sync.Mutex),
		idInc: &idIncrementer{
			seed: rand.Int63(),
			count: 0,
		},
	}, nil
}

func (ctx *Context) Produce(respchan chan []byte, key, value []byte) error {
	identifier := ctx.idInc.Inc()

	ctx.muhttpChannels.Lock()
	if _, ok := ctx.httpChannels[identifier]; ok {
		ctx.muhttpChannels.Unlock()
		return errors.New("identifier already exists")
	}
	ctx.httpChannels[identifier] = respchan
	ctx.muhttpChannels.Unlock()

	go ctx.deleteKeyAfter(ctx.timeout, identifier)

	//append magic byte and idetifier
	header, err := binaryHeader(ctx.idInc.seed, identifier)
	fmt.Println("header:", header)
	if err != nil {
		return err
	}

	appendedValue := append(header, value...)

	ctx.producer.Input() <- &sarama.ProducerMessage{
		Topic: 	ctx.produceTopic,
		Key: 	sarama.ByteEncoder(key),
		Value:	sarama.ByteEncoder(appendedValue),
	}

	return nil
}

func identifyPartition(partitionsCount uint32, key []byte) uint32 {
	// Figure out what seed value to use
	return hash.MurmurHash2(key, 123) % partitionsCount
}

func (ctx *Context) deleteKeyAfter(ms int, key int32) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	ctx.muhttpChannels.Lock()
	defer ctx.muhttpChannels.Unlock()
	if _, ok := ctx.httpChannels[key]; ok {
		fmt.Println("about to delete key:", key)
		delete(ctx.httpChannels, key)
	}
}

func (ctx *Context) Keys() []int32 {
	ctx.muhttpChannels.Lock()

	keys := make([]int32, len(ctx.httpChannels))
	defer ctx.muhttpChannels.Unlock()

	for k := range ctx.httpChannels {
		keys = append(keys, k)
	}
	return keys
}

func binaryHeader(seed int64, count int32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, seed)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, count)
	if err != nil {
		return nil, err
	}

	return append([]byte(GROXY_MAGIC_STRING), buf.Bytes()...), nil
}

func (i *idIncrementer) Inc() int32 {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.count += 1
	return i.count 
}






























