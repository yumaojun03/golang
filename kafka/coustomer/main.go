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
	pwg sync.WaitGroup

	logger = log.New(os.Stderr, "[device-influxdb-tool]", log.LstdFlags)

	kafkaAddr       = flag.String("kafka", "172.21.3.13:9092,172.21.3.14:9092,172.21.3.15:9092", "数据发往kafka的地址.")
	topic           = flag.String("topic", "mock-devices-default", "发往kafka的那个topic")
	dbAddr          = flag.String("dbAddr", "http://192.168.204.21:8086", "InfluxDB服务地址")
	dbName          = flag.String("dbname", "save_influx_default", "存入InfluxDB的数据库的名字")
	username        = flag.String("username", "admin", "InfluxDB的用户名")
	password        = flag.String("password", "admin", "InfluxDB的密码")
	cousumerNumbers = flag.Int("consumer-numbers", 8, "启动几个消费者")
	frequency       = flag.Int("frequency", 1, "批量提交给influxDB的间隔时间,单位为秒")
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
	clients   []client.Client
	consumers []*cluster.Consumer
}

// NewInfluxDBPlugin 插件实例
func NewInfluxDBPlugin() (*InfluxDBPlugin, error) {
	ip := InfluxDBPlugin{}

	// 初始化kafka客户端
	groupID := "device-mock-tool-01"
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Config.ClientID = "device-mock-tool"
	config.Config.ChannelBufferSize = 4096
	config.Config.Consumer.Fetch.Default = 131072
	config.Config.Consumer.MaxProcessingTime = time.Second * 60
	sarama.Logger = logger

	topics := []string{*topic}
	for i := 0; i < *cousumerNumbers; i++ {
		// 初始化多个consumer
		otherC, err := cluster.NewConsumer(strings.Split(*kafkaAddr, ","), groupID, topics, config)
		if err != nil {
			panic(err)
		}
		ip.consumers = append(ip.consumers, otherC)

		// 初始化对应梳理的influxDB客户端
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     *dbAddr,
			Username: *username,
			Password: *password,
		})
		if err != nil {
			return nil, fmt.Errorf("create influxdb client error, %s", err.Error())
		}
		ip.clients = append(ip.clients, c)
	}

	return &ip, nil
}

func (ip *InfluxDBPlugin) addPT(bp client.BatchPoints, data []byte, pid int32) error {
	var udata map[string]fullData
	if err := json.Unmarshal(data, &udata); err != nil {
		return fmt.Errorf("json unmarshal error, %s", err.Error())
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

		fmt.Printf("<add point> did: %s, partition: %d\n", k, pid)

	}

	return nil
}

// startPC 用于为每个Partition 启用一个Goroutine
func (ip *InfluxDBPlugin) startPC(consumer *cluster.Consumer, inc client.Client) error {

	var bp client.BatchPoints
	var err error

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

	t1 := time.NewTicker(time.Second * time.Duration(*frequency))
	bp, err = ip.getNewBP()
	if err != nil {
		return err
	}

	for msg := range consumer.Messages() {
		select {
		case <-t1.C:
			ts := time.Now()
			if err := inc.Write(bp); err != nil {
				return err
			}

			deltaT := time.Now().Sub(ts)
			fmt.Printf("coust: %f\n", deltaT.Seconds())

			bp, err = ip.getNewBP()
			if err != nil {
				return err
			}
		default:
			if err := ip.addPT(bp, msg.Value, msg.Partition); err != nil {
				fmt.Printf("add point to influx client error, %s", err)
			}

		}
	}

	logger.Printf("Done consuming topic %s\n", *topic)
	consumer.Close()

	pwg.Done()
	return nil
}

func (ip *InfluxDBPlugin) getNewBP() (client.BatchPoints, error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  *dbName,
		Precision: "s",
	})
	if err != nil {
		return nil, fmt.Errorf("create batchpoints error, %s", err.Error())
	}

	return bp, nil
}

// Start 启动InfluxDB
func (ip *InfluxDBPlugin) Start() error {

	for i, v := range ip.consumers {
		fmt.Println("start cousumer ...")
		pwg.Add(1)
		go ip.startPC(v, ip.clients[i])

		// 避免Goroutine同时启动, 因为Goroutine内有timer
		st := (*frequency) * 100 / (*cousumerNumbers)
		if st > 0 {
			time.Sleep(time.Microsecond * time.Duration(st))
		} else {
			time.Sleep(time.Microsecond * 1)
		}
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
