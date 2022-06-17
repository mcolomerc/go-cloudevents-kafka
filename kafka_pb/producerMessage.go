package kafka_pb

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type flagSet int8

// ProducerMessage is the collection of elements passed to the Producer in order to send a message.
type ProducerMessage struct {
	Topic          string // The Kafka topic for this message.
	Key            []byte
	Value          []byte
	Headers        []kafka.Header
	Metadata       interface{}
	Offset         int64
	Partition      int32
	Timestamp      time.Time
	retries        int
	flags          flagSet
	expectation    chan *ProducerError
	sequenceNumber int32
	producerEpoch  int16
	hasSequence    bool
}

const producerMessageOverhead = 26 // the metadata overhead of CRC, flags, etc.

const maximumRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1

func (m *ProducerMessage) byteSize(version int) int {
	var size int
	if version >= 2 {
		size = maximumRecordOverhead
		for _, h := range m.Headers {
			size += len(h.Key) + len(h.Value) + 2*binary.MaxVarintLen32
		}
	} else {
		size = producerMessageOverhead
	}
	if m.Key != nil {
		size += len(m.Key)
	}
	if m.Value != nil {
		size += len(m.Value)
	}
	return size
}

func (m *ProducerMessage) clear() {
	m.flags = 0
	m.retries = 0
	m.sequenceNumber = 0
	m.producerEpoch = 0
	m.hasSequence = false
}

type ProducerError struct {
	Msg *ProducerMessage
	Err error
}

func (pe ProducerError) Error() string {
	return fmt.Sprintf("kafka: Failed to produce message to topic %s: %s", pe.Msg.Topic, pe.Err)
}
