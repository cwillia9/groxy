package groxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	hash "github.com/aviddiviner/go-murmur"
)

type idIncrementer struct {
	seed  int64
	count int32
}

type KafkaContext struct {
	Client sarama.Client

	Consumer sarama.Consumer
	Producer sarama.AsyncProducer

	PartitionConsumer sarama.PartitionConsumer
}

type Message struct {
	RespChan   chan []byte
	Key, Value []byte
}

type Context struct {
	Input  chan *Message
	Delete chan int32
	Kafka  *KafkaContext

	consumeTopic string
	selfHostname string
	timeout      int
	produceTopic string
	httpChannels map[int32]chan []byte
	idInc        *idIncrementer
}

const GROXY_MAGIC_STRING = "GrOxy"

func NewContext(kafkaContext *KafkaContext,
	consumeTopic string,
	hostname string,
	produceTopic string,
	timeout int) (*Context, error) {

	var err error

	Logger.Debug("Starting")

	if kafkaContext.Consumer == nil {
		kafkaContext.Consumer, err = sarama.NewConsumerFromClient(kafkaContext.Client)
		if err != nil {
			Logger.Err("Failed to create new consumer from client: ", err)
			return nil, err
		}
	}

	if kafkaContext.Producer == nil {

		kafkaContext.Producer, err = sarama.NewAsyncProducerFromClient(kafkaContext.Client)
		if err != nil {
			return nil, err
		}
	}

	if kafkaContext.PartitionConsumer == nil {
		consumePartitions, err := kafkaContext.Client.Partitions(consumeTopic)
		if err != nil {
			return nil, err
		}

		consumePartition := identifyPartition(uint32(len(consumePartitions)), []byte(hostname))

		Logger.Info("consumePartition:", consumePartition, "sarama.OffsetLatest:", sarama.OffsetNewest)
		kafkaContext.PartitionConsumer, err = kafkaContext.Consumer.ConsumePartition(consumeTopic, int32(consumePartition), sarama.OffsetNewest)

		if err != nil {
			return nil, err
		}
	}

	c := &Context{
		Input:        make(chan *Message),
		Delete:       make(chan int32),
		consumeTopic: consumeTopic,
		selfHostname: hostname,
		timeout:      timeout,
		produceTopic: produceTopic,
		Kafka:        kafkaContext,
		httpChannels: make(map[int32]chan []byte),
		idInc: &idIncrementer{
			seed:  rand.Int63(),
			count: 0,
		},
	}

	go c.run()
	return c, nil
}

func (ctx *Context) run() {
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var in *Message
	var err error
	var del int32
	var msg *sarama.ConsumerMessage

	go ctx.producerSuccesses()
	go ctx.producerErrors()
	go ctx.consumerErrors()

	for {
		select {
		case in = <-ctx.Input:
			err = ctx.Produce(in.RespChan, in.Key, in.Value)
			Logger.Err(err)
		case del = <-ctx.Delete:
			if _, ok := ctx.httpChannels[del]; ok {
				delete(ctx.httpChannels, del)
			}
		case msg = <-ctx.Kafka.PartitionConsumer.Messages():
			ctx.ConsumeMessage(msg)
		case <-signals:
			Logger.Info("Got kill signal. Shutting down system.")
			ctx.Close()
			break
		}
	}
}

func (ctx *Context) Close() {
	ctx.Kafka.Producer.Close()
	ctx.Kafka.Consumer.Close()
	ctx.Kafka.Client.Close()
}

func (ctx *Context) producerSuccesses() {
	for _ = range ctx.Kafka.Producer.Successes() {
		Logger.Debug("Message successfully sent")
	}
}

func (ctx *Context) producerErrors() {
	for e := range ctx.Kafka.Producer.Errors() {
		Logger.Err("Producer error:", e)
	}
}

func (ctx *Context) consumerErrors() {
	for e := range ctx.Kafka.PartitionConsumer.Errors() {
		Logger.Err("Consumer error:", e)
	}
}

func (ctx *Context) Produce(respchan chan []byte, key, value []byte) error {
	identifier := ctx.idInc.Inc()

	if _, ok := ctx.httpChannels[identifier]; ok {
		return errors.New("identifier already exists")
	}

	ctx.httpChannels[identifier] = respchan

	// Get rid of this??
	go ctx.deleteKeyAfter(ctx.timeout, identifier)

	//append magic byte and idetifier
	header, err := binaryHeader(ctx.idInc.seed, identifier)

	if err != nil {
		return err
	}

	appendedValue := append(header, value...)

	ctx.Kafka.Producer.Input() <- &sarama.ProducerMessage{
		Topic: ctx.produceTopic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(appendedValue),
	}

	return nil
}

func identifyPartition(partitionsCount uint32, key []byte) uint32 {
	// Figure out what seed value to use
	return hash.MurmurHash2(key, 123) % partitionsCount
}

func (ctx *Context) deleteKeyAfter(ms int, key int32) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	ctx.Delete <- key
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
	i.count += 1
	return i.count
}

func (ctx *Context) ConsumeMessage(msg *sarama.ConsumerMessage) {
	value := msg.Value

	if string(msg.Key) != ctx.selfHostname {
		return
	}

	l := len(GROXY_MAGIC_STRING)
	// Is this a GrOxy message?
	if string(value[:l]) != GROXY_MAGIC_STRING {
		return
	}

	// Is the message from this process instance?
	if binary.BigEndian.Uint64(value[l:l+8]) != uint64(ctx.idInc.seed) {
		return
	}

	key := int32(binary.BigEndian.Uint32(value[l+8 : l+12]))
	if ch, ok := ctx.httpChannels[key]; ok {
		go func() {
			ch <- value[l+12:]
		}()
	}
}
