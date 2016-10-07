package groxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
	hash "github.com/aviddiviner/go-murmur"
)

type idIncrementer struct {
	seed  int64
	count int32
}

type BinaryHeader struct {
	Seed      int64
	Count     int32
	Partition uint32
	Topic     string

	Header []byte
	Len    int
}

type KafkaContext struct {
	Client sarama.Client

	Consumer sarama.Consumer
	Producer sarama.AsyncProducer

	PartitionConsumer sarama.PartitionConsumer
}

type Message struct {
	RespChan   chan<- []byte
	Key, Value []byte
	Err        error
}

type Context struct {
	Input  chan *Message
	Delete chan int32
	Kafka  *KafkaContext

	kill             chan int
	consumeTopic     string
	consumePartition uint32
	selfHostname     string
	timeout          int
	produceTopic     string
	idInc            *idIncrementer
}

const GROXY_MAGIC_STRING = "GrOxy"

func NewContext(kafkaContext *KafkaContext,
	consumeTopic string,
	hostname string,
	produceTopic string,
	timeout int) (*Context, error) {

	var err error
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
		Logger.Info("produceTopic:", produceTopic)
	}

	var consumePartition uint32
	if kafkaContext.PartitionConsumer == nil {
		consumePartitions, err := kafkaContext.Client.Partitions(consumeTopic)
		if err != nil {
			return nil, err
		}

		consumePartition = identifyPartition(uint32(len(consumePartitions)), []byte(hostname))
		kafkaContext.PartitionConsumer, err = kafkaContext.Consumer.ConsumePartition(consumeTopic, int32(consumePartition), sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}
		Logger.Info("cosumeTopic:", consumeTopic, "consumePartition:", consumePartition, "sarama.OffsetLatest:", sarama.OffsetNewest)
	}

	c := &Context{
		Input:            make(chan *Message),
		Delete:           make(chan int32),
		kill:             make(chan int),
		consumeTopic:     consumeTopic,
		consumePartition: consumePartition,
		selfHostname:     hostname,
		timeout:          timeout,
		produceTopic:     produceTopic,
		Kafka:            kafkaContext,
		idInc: &idIncrementer{
			seed:  rand.Int63(),
			count: 0,
		},
	}

	go c.run()
	return c, nil
}

func (ctx *Context) run() {
	httpChannels := make(map[int32]chan<- []byte)

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
			err = ctx.produce(in.RespChan, in.Key, in.Value, httpChannels)
			if err != nil {
				Logger.Err(err)
				in.RespChan <- nil
			}
		case del = <-ctx.Delete:
			if _, ok := httpChannels[del]; ok {
				delete(httpChannels, del)
			}
		case msg = <-ctx.Kafka.PartitionConsumer.Messages():
			ctx.consumeMessage(msg, httpChannels)
		case <-ctx.kill:
			Logger.Info("Got kill signal. Shutting down system.")
			break
		}
	}
}

func (ctx *Context) Close() {
	if err := ctx.Kafka.Producer.Close(); err != nil {
		Logger.Err(err)
	}
	if err := ctx.Kafka.Consumer.Close(); err != nil {
		Logger.Err(err)
	}
	ctx.kill <- 1
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

func (ctx *Context) produce(respchan chan<- []byte,
	key, value []byte,
	httpChannels map[int32]chan<- []byte) error {
	identifier := ctx.idInc.Inc()

	if _, ok := httpChannels[identifier]; ok {
		return errors.New("identifier already exists")
	}

	httpChannels[identifier] = respchan

	// Get rid of this??
	go ctx.deleteKeyAfter(ctx.timeout, identifier)

	//append magic byte and idetifier
	header := &BinaryHeader{
		Seed:      ctx.idInc.seed,
		Count:     identifier,
		Partition: ctx.consumePartition,
		Topic:     ctx.consumeTopic,
	}
	header.Write()
	if header.Header == nil {
		return ErrFailedToMakeHeader
	}

	appendedValue := append(header.Header, value...)

	ctx.Kafka.Producer.Input() <- &sarama.ProducerMessage{
		Topic: ctx.produceTopic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(appendedValue),
	}

	return nil
}

func identifyPartition(partitionsCount uint32, key []byte) uint32 {
	if partitionsCount < 1 {
		return 0
	}
	// Figure out what seed value to use
	return hash.MurmurHash2(key, 123) % partitionsCount
}

func (ctx *Context) deleteKeyAfter(ms int, key int32) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	ctx.Delete <- key
}

func (b *BinaryHeader) Write() {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, b.Seed)
	if err != nil {
		b.Header = nil
	}
	err = binary.Write(buf, binary.BigEndian, b.Count)
	if err != nil {
		b.Header = nil
	}

	err = binary.Write(buf, binary.BigEndian, b.Partition)
	if err != nil {
		b.Header = nil
	}

	bytes_topic := []byte(b.Topic)
	err = binary.Write(buf, binary.BigEndian, uint32(len(bytes_topic)))
	if err != nil {
		b.Header = nil
	}

	_, _ = buf.Write(bytes_topic)

	b.Header = append([]byte(GROXY_MAGIC_STRING), buf.Bytes()...)
}

func (b *BinaryHeader) Read(value []byte) error {
	if value == nil {
		return ErrFailedToReadHeader
	}

	l := len(GROXY_MAGIC_STRING)
	if len(value) <= l+20 {
		return ErrFailedToReadHeader
	}

	// Is this a GrOxy message?
	if string(value[:l]) != GROXY_MAGIC_STRING {
		return ErrNotGroxyMessage
	}

	// Is the message from this process instance?
	b.Seed = int64(binary.BigEndian.Uint64(value[l : l+8]))
	b.Count = int32(binary.BigEndian.Uint32(value[l+8 : l+12]))
	b.Partition = binary.BigEndian.Uint32(value[l+12 : l+16])

	strlen := int(binary.BigEndian.Uint32(value[l+16 : l+20]))
	if len(value) < l+20+strlen {
		return ErrFailedToReadHeader
	}
	b.Topic = string(value[l+20 : l+20+strlen])
	b.Len = l + 20 + strlen
	return nil
}

func (i *idIncrementer) Inc() int32 {
	i.count += 1
	return i.count
}

func (ctx *Context) consumeMessage(msg *sarama.ConsumerMessage,
	httpChannels map[int32]chan<- []byte) {

	value := msg.Value

	header := &BinaryHeader{}
	err := header.Read(value)
	if err != nil {
		switch err {
		case ErrNotGroxyMessage:
			//TODO:Not considering this a problem. Should we?
			Logger.Debug(err)
			return
		case ErrFailedToReadHeader:
			Logger.Err(err)
			return
		}
	}

	if header.Seed != ctx.idInc.seed {
		return
	}

	if ch, ok := httpChannels[header.Count]; ok {
		go func() {
			defer func() { _ = recover() }()
			ch <- value[header.Len:]
		}()
	}
}
