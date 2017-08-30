package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/influxdata/influxdb/client/v2"
)

const (
	dbName   = "yumaojun_performance_test"
	username = "admin"
	password = "admin"
	dbAddr   = "http://192.168.204.21:8086"
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

func saveToInfluxDB(data []byte) error {
	var udata map[string]fullData
	if err := json.Unmarshal(data, &udata); err != nil {
		return fmt.Errorf("json unmarshal error, %s", err.Error())
	}

	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     dbAddr,
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  dbName,
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	for k, v := range udata {
		tags := map[string]string{"did": k}

		fields := map[string]interface{}{}
		for _, p := range v.Data.DataStream {
			fields[p.ID] = p.Value
		}

		pt, err := client.NewPoint(v.Data.Type, tags, fields, time.Unix(v.At, 0))
		if err != nil {
			return err
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			return err
		}
		fmt.Printf("constom did: %s\n", k)

	}

	return nil
}

func coustomer() {

	sarama.Logger = logger
	consumer, err := sarama.NewConsumer(strings.Split(kafkaAddr, ","), nil)
	if err != nil {
		logger.Printf("Failed to start consumer: %s\n", err)
	}

	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		logger.Println("Failed to get the list of partitions: ", err)
	}

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			logger.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
		}

		defer pc.AsyncClose()

		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(sarama.PartitionConsumer) {
				defer wg.Done()
				for msg := range pc.Messages() {
					if err := saveToInfluxDB(msg.Value); err != nil {
						fmt.Println(err)
					}
					// fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				}
			}(pc)
		}

	}

	wg.Wait()
	logger.Printf("Done consuming topic %s\n", topic)
	consumer.Close()

}

func main() {
	coustomer()
}
