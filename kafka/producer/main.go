package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
)

var (
	wg sync.WaitGroup

	kafkaAddr = "172.21.3.13:9092,172.21.3.14:9092,172.21.3.15:9092"
	topic     = "yumaojun-performance-test"
	logger    = log.New(os.Stderr, "[srama]", log.LstdFlags)
)

type fullData struct {
	MsgType   string   `json:"msgtype"`
	DebugMode string   `json:"debugmode"`
	At        int64    `json:"at"`
	Path      string   `json:"path"`
	Data      *rowData `json:"data"`
}

type rowData struct {
	MsgType    string   `json:"msgtype"`
	DebugMode  string   `json:"debugmode"`
	At         int64    `json:"at"`
	Type       string   `json:"fan"`
	Tag        []string `json:"tag"`
	DataStream []*point `json:"datastream"`
}

type point struct {
	ID      string  `json:"id"`
	Value   float32 `json:"value"`
	Quality float32 `json:"quality"`
}

/*
200 device * 300 point
data = `{
		"cf688ac1af5f43b38f586aefcc8ff135": {
		  "msgtype": "devicedata",
		  "at": 1503646078,
		  "debugmode": "off",
		  "path": "cf688ac1af5f43b38f586aefcc8ff135/ec8211db5d6944eb9057aa12b46894eb/0292e9e9fd4645c8af19ac26747839f9/colin7/087a64568e1a4edda4b53a8d7bf8b3f9",
		  "data": {
			"msgtype": "devicedata",
			"debugmode": "off",
			"at": 1503646078,
			"type": "fan"
			"tag": [
			  "quality"
			],
			"datastream": [
			  {
				"id": "random1",
				"value": 3.982,
				"quality": 192
			  },
			  {
				"id": "random2",
				"value": 9.726,
				"quality": 192
			  }
			]
		  }
		}
	  }`
*/
func mockData(did string) (*map[string]*fullData, error) {

	points := []*point{}
	for i := 0; i < 300; i++ {
		p := &point{
			ID:      "random" + strconv.Itoa(i),
			Value:   rand.Float32() * 100,
			Quality: 192,
		}
		points = append(points, p)
	}
	at := time.Now().Unix()

	msgtype := "devicedata"
	debugmode := "off"
	dType := "fan"
	tag := []string{"quality"}
	path := "cf688ac1af5f43b38f586aefcc8ff135/ec8211db5d6944eb9057aa12b46894eb/0292e9e9fd4645c8af19ac26747839f9/colin7/087a64568e1a4edda4b53a8d7bf8b3f9"

	rowD := rowData{MsgType: msgtype, DebugMode: debugmode, At: at, Type: dType, Tag: tag, DataStream: points}
	fullD := fullData{MsgType: msgtype, At: at, DebugMode: debugmode, Path: path, Data: &rowD}

	data := map[string]*fullData{did: &fullD}

	return &data, nil
}

func asyncProducer(did string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true //必须有这个选项
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewAsyncProducer(strings.Split(kafkaAddr, ","), config)
	defer p.Close()
	if err != nil {
		return
	}

	//必须有这个匿名函数内容
	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					logger.Println(err.Error())
				}
			case <-success:
			}
		}
	}(p)

	t1 := time.NewTimer(time.Second * 1)

	for {
		select {
		case <-t1.C:
			var data *map[string]*fullData
			var err error

			data, err = mockData(did)
			if err != nil {
				fmt.Printf("mock data error!, %s\n", err.Error())
				data = &map[string]*fullData{"cf688ac1af5f43b38f586aefcc8ff135": &fullData{}}
			}

			sendData, err := json.Marshal(data)
			if err != nil {
				fmt.Printf("dump to json error, %s", err.Error())
			} else {
				msg := &sarama.ProducerMessage{}
				msg.Topic = topic
				msg.Partition = int32(-1)
				msg.Key = sarama.StringEncoder("my-fan-01")
				msg.Value = sarama.ByteEncoder(sendData)

				v := "async: " + strconv.Itoa(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10000))
				fmt.Fprintln(os.Stdout, v)

				p.Input() <- msg
			}

			t1.Reset(time.Second * 1)
		}
	}

}

func syncProducer(did string) {

	sarama.Logger = logger
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	t1 := time.NewTimer(time.Second * 1)

	for {
		select {
		case <-t1.C:
			var data *map[string]*fullData
			var err error

			data, err = mockData(did)
			if err != nil {
				fmt.Printf("mock data error!, %s\n", err.Error())
				data = &map[string]*fullData{"cf688ac1af5f43b38f586aefcc8ff135": &fullData{}}
			}

			sendData, err := json.Marshal(data)
			if err != nil {
				fmt.Printf("dump to json error, %s", err.Error())
				continue
			}

			msg := &sarama.ProducerMessage{}
			msg.Topic = topic
			msg.Partition = int32(-1)
			msg.Key = sarama.StringEncoder("my-fan-01")
			msg.Value = sarama.ByteEncoder(sendData)

			producer, err := sarama.NewSyncProducer(strings.Split("172.21.3.13:9092,172.21.3.14:9092,172.21.3.15:9092", ","), config)
			if err != nil {
				logger.Printf("Failed to produce message: %s", err)
				os.Exit(1)
			}

			defer producer.Close()

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				logger.Println("Failed to produce message: ", err)
			}

			logger.Printf("partition=%d, offset=%d\n", partition, offset)
			t1.Reset(time.Second * 1)
		}
	}

}

func main() {
	dids := []string{}
	for i := 0; i < 50; i++ {
		dids = append(dids, uuid.NewV4().String())
	}
	fmt.Println(dids)

	for _, did := range dids {
		wg.Add(1)
		go asyncProducer(did)
	}
	wg.Wait()
}
