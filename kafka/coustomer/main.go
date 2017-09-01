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
	cluster "github.com/bsm/sarama-cluster"

	"github.com/influxdata/influxdb/client/v2"
)

var (
	wg  sync.WaitGroup
	pwg sync.WaitGroup

	logger = log.New(os.Stderr, "[device-influxdb-tool]", log.LstdFlags)

	kafkaAddr       = flag.String("kafka", "172.21.3.13:9092,172.21.3.14:9092,172.21.3.15:9092", "数据发往kafka的地址.")
	topic           = flag.String("topic", "mock-devices-default", "发往kafka的那个topic")
	dbAddr          = flag.String("dbAddr", "http://192.168.204.21:8086", "InfluxDB服务地址")
	dbName          = flag.String("dbname", "save_influx_default", "存入InfluxDB的数据库的名字")
	username        = flag.String("username", "admin", "InfluxDB的用户名")
	password        = flag.String("password", "admin", "InfluxDB的密码")
	workers         = flag.Int("partition-workers", 8, "每个partiton启动多少个协程处理")
	cousumerNumbers = flag.Int("consumer-numbers", 8, "启动几个消费者")
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
	Type       string   `json:"type"`
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
	client    client.Client
	consumers []*cluster.Consumer
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
	groupID := "device-mock-tool-01"
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Config.ClientID = "device-mock-tool"
	config.Config.ChannelBufferSize = 4096
	config.Config.Consumer.Fetch.Default = 131072
	config.Config.Consumer.MaxProcessingTime = time.Second * 30
	sarama.Logger = logger

	// 生成和partition数量相同的消费者, 组成一个消费组
	topics := []string{*topic}
	for i := 0; i < *cousumerNumbers; i++ {
		otherC, err := cluster.NewConsumer(strings.Split(*kafkaAddr, ","), groupID, topics, config)
		if err != nil {
			panic(err)
		}
		ip.consumers = append(ip.consumers, otherC)
	}

	ip.client = c
	return &ip, nil
}

func (ip *InfluxDBPlugin) save(data []byte, pid int32) error {
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
		fmt.Printf("constom did: %s, partition: %d\n", k, pid)

	}

	return nil
}

// startPC 用于为每个Partition 启用一个Goroutine
func (ip *InfluxDBPlugin) startPC(consumer *cluster.Consumer) error {
	go func(c *cluster.Consumer) {
		errors := c.Errors()
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				logger.Println(err)
			case <-noti:
			}
		}
	}(consumer)

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go ip.startWorker(consumer.Messages())
	}

	wg.Wait()
	logger.Printf("Done consuming topic %s\n", *topic)
	consumer.Close()

	pwg.Done()

	// partitionList, err := ip.rconsumer.Partitions(*topic)
	// fmt.Println(partitionList)
	// if err != nil {
	// 	return fmt.Errorf("failed to get the topic:%s of partitions: %s", *topic, err)
	// }

	// for partition := range partitionList {
	// 	pc, err := ip.rconsumer.ConsumePartition(*topic, int32(partition), sarama.OffsetNewest)
	// 	if err != nil {
	// 		logger.Printf("failed to start consumer for partition %d: %s\n", partition, err)
	// 		continue
	// 	}

	// 	defer pc.AsyncClose()

	// 	pcwg.Add(1)
	// 	go func(sarama.PartitionConsumer) {
	// 		defer pcwg.Done()

	// 		// 为每个Partition启动多个worker
	// 		for i := 0; i < *workers; i++ {
	// 			wwg.Add(1)
	// 			go ip.startWorker(pc.Messages(), int32(partition))
	// 		}
	// 		wwg.Wait()
	// 	}(pc)
	// }

	// pcwg.Wait()
	// logger.Printf("Done consuming topic %s\n", *topic)
	// ip.consumer.Close()

	return nil
}

func (ip *InfluxDBPlugin) startWorker(cmchan <-chan *sarama.ConsumerMessage) {
	defer wg.Done()

	fmt.Println("start worker...")
	for msg := range cmchan {
		// fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		if err := ip.save(msg.Value, msg.Partition); err != nil {
			fmt.Printf("save to influxDB error, %s", err)
		}
		// MarkOffset 并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset
		// 因此这里需要msg传回consumer, 进行标签
		// ip.consumer.MarkOffset(msg, "")
	}
}

// Start 启动InfluxDB
func (ip *InfluxDBPlugin) Start() error {

	for _, v := range ip.consumers {
		fmt.Println("start cousumer ...")
		pwg.Add(1)
		go ip.startPC(v)
	}

	pwg.Wait()

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
