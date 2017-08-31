package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/influxdata/influxdb/client/v2"
)

var (
	pcwg sync.WaitGroup
	wwg  sync.WaitGroup

	logger = log.New(os.Stderr, "[device-influxdb-tool]", log.LstdFlags)

	kafkaAddr = flag.String("kafka", "172.21.3.13:9092,172.21.3.14:9092,172.21.3.15:9092", "数据发往kafka的地址.")
	topic     = flag.String("topic", "mock-devices-default", "发往kafka的那个topic")
	dbAddr    = flag.String("dbAddr", "http://192.168.204.21:8086", "InfluxDB服务地址")
	dbName    = flag.String("dbname", "save_influx_default", "存入InfluxDB的数据库的名字")
	username  = flag.String("username", "admin", "InfluxDB的用户名")
	password  = flag.String("password", "admin", "InfluxDB的密码")
	workers   = flag.Int("partition-workers", 8, "每个kafka的partition需要多少协程处理")
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

// InfluxDBPlugin 用于将kafka里面的数据入库到influxDB
type InfluxDBPlugin struct {
	client   client.Client
	consumer sarama.Consumer
}

// NewInfluxDBPlugin 插件实例
func NewInfluxDBPlugin() (*InfluxDBPlugin, error) {
	ip := InfluxDBPlugin{}

	// 初始化influxdb客户端
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     *dbAddr,
		Username: *username,
		Password: *password,
	})
	if err != nil {
		return nil, fmt.Errorf("create influxdb client error, %s", err.Error())
	}

	// 初始化kafka客户端
	sarama.Logger = logger
	consumer, err := sarama.NewConsumer(strings.Split(*kafkaAddr, ","), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start kafka consumer: %s", err)
	}

	ip.client = c
	ip.consumer = consumer

	return &ip, nil
}

func (ip *InfluxDBPlugin) save(data []byte) error {
	var udata map[string]fullData
	if err := json.Unmarshal(data, &udata); err != nil {
		return fmt.Errorf("json unmarshal error, %s", err.Error())
	}

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  *dbName,
		Precision: "s",
	})
	if err != nil {
		return fmt.Errorf("create batchpoints error, %s", err.Error())
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
		if err := ip.client.Write(bp); err != nil {
			return err
		}
		fmt.Printf("constom did: %s\n", k)

	}

	return nil
}

// startPC 用于为每个Partition 启用一个Goroutine
func (ip *InfluxDBPlugin) startPC() error {
	partitionList, err := ip.consumer.Partitions(*topic)
	fmt.Println(partitionList)
	if err != nil {
		return fmt.Errorf("failed to get the topic:%s of partitions: %s", *topic, err)
	}

	for partition := range partitionList {
		pc, err := ip.consumer.ConsumePartition(*topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			logger.Printf("failed to start consumer for partition %d: %s\n", partition, err)
			continue
		}

		defer pc.AsyncClose()

		pcwg.Add(1)
		go func(sarama.PartitionConsumer) {
			defer pcwg.Done()

			// 为每个Partition启动多个worker
			for i := 0; i < *workers; i++ {
				wwg.Add(1)
				go ip.startWorker(pc.Messages(), int32(partition))
			}
			wwg.Wait()
		}(pc)
	}

	pcwg.Wait()
	logger.Printf("Done consuming topic %s\n", *topic)
	ip.consumer.Close()

	return nil
}

func (ip *InfluxDBPlugin) startWorker(cmchan <-chan *sarama.ConsumerMessage, pc int32) {
	defer wwg.Done()

	fmt.Printf("start worker for partition: %d\n", pc)
	for msg := range cmchan {
		if err := ip.save(msg.Value); err != nil {
			fmt.Printf("save to influxDB error, %s", err)
		}
		fmt.Println(msg.Partition)
	}
}

// Start 启动InfluxDB
func (ip *InfluxDBPlugin) Start() error {

	if err := ip.startPC(); err != nil {
		return err
	}

	return nil
}

func main() {
	flag.Parse()

	ip, err := NewInfluxDBPlugin()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := ip.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
