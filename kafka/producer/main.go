package main

import (
	"encoding/json"
	"flag"
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

	logger = log.New(os.Stderr, "[device-mock-tool]", log.LstdFlags)

	numbers      = flag.Int("numbers", 50, "模拟多少台设备.")
	pointNumbers = flag.Int("points", 300, "每台设备模拟多少个点位.")
	kafkaAddr    = flag.String("kafka", "172.21.3.13:9092,172.21.3.14:9092,172.21.3.15:9092", "数据发往kafka的地址.")
	topic        = flag.String("topic", "mock-devices-default", "发往kafka的那个topic")
	partitions   = flag.Int("partitions", 16, "Kafka对应topic的分区数量. 其提前修改kafka配置让其partition大于16")
	frequency    = flag.Int("frequency", 5, "每个设备数据发送的间隔, 单位为秒.")
	isSyncSend   = flag.Bool("sync", false, "发送数据模式, 为了高效, 默认异步发送.")
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

// DeviceMockTool 用于模拟设备, 将数据写入kafka.
// 设备数据的设备类型就是kafka的topic, 设备会被水平
// 分区地放到每个分区里面.
// 模拟的数据格式大致如下:
// data = `{
// 	"cf688ac1af5f43b38f586aefcc8ff135": {
// 	  "msgtype": "devicedata",
// 	  "at": 1503646078,
// 	  "debugmode": "off",
// 	  "path": "cf688ac1af5f43b38f586aefcc8ff135/ec8211db5d6944eb9057aa12b46894eb/0292e9e9fd4645c8af19ac26747839f9/colin7/087a64568e1a4edda4b53a8d7bf8b3f9",
// 	  "data": {
// 		"msgtype": "devicedata",
// 		"debugmode": "off",
// 		"at": 1503646078,
// 		"type": "fan"
// 		"tag": [
// 		  "quality"
// 		],
// 		"datastream": [
// 		  {
// 			"id": "random1",
// 			"value": 3.982,
// 			"quality": 192
// 		  },
// 		  {
// 			"id": "random2",
// 			"value": 9.726,
// 			"quality": 192
// 		  }
// 		]
// 	  }
// 	}
//   }`
type DeviceMockTool struct {
	deviceIDs map[string]int
	ap        sarama.AsyncProducer
	sp        sarama.SyncProducer
}

// NewDeviceMockTool 创建一个模拟工具实例
func NewDeviceMockTool() (*DeviceMockTool, error) {
	dmt := DeviceMockTool{}

	dids := map[string]int{}
	for i := 0; i < *numbers; i++ {
		dids[uuid.NewV4().String()] = i
	}
	dmt.deviceIDs = dids

	sarama.Logger = logger
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	if *isSyncSend {

		config.Producer.RequiredAcks = sarama.WaitForAll
		sp, err := sarama.NewSyncProducer(strings.Split(*kafkaAddr, ","), config)
		if err != nil {
			return nil, err
		}

		dmt.sp = sp
	} else {
		config.Producer.Timeout = 5 * time.Second

		ap, err := sarama.NewAsyncProducer(strings.Split(*kafkaAddr, ","), config)
		if err != nil {
			return nil, err
		}

		dmt.ap = ap
	}

	return &dmt, nil
}

// ShowDeviceIDs 打印出模拟的设备的deviceID方便追踪
func (tool *DeviceMockTool) ShowDeviceIDs() {
	fmt.Println("--------------设备ID-----------------")
	for did := range tool.deviceIDs {
		fmt.Println(did)
	}
	fmt.Println("------------------------------------")
}

// Start 启动模拟
func (tool *DeviceMockTool) Start() {

	tool.ShowDeviceIDs()

	for did := range tool.deviceIDs {
		wg.Add(1)
		if *isSyncSend {
			go tool.syncProducer(did)
		} else {
			go tool.asyncProducer(did)
		}
	}
	wg.Wait()
}

func (tool *DeviceMockTool) mockData(did string) (*map[string]*fullData, error) {

	points := []*point{}
	for i := 0; i < *pointNumbers; i++ {
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
	dType := *topic
	tag := []string{"quality"}
	path := "cf688ac1af5f43b38f586aefcc8ff135/ec8211db5d6944eb9057aa12b46894eb/0292e9e9fd4645c8af19ac26747839f9/colin7/087a64568e1a4edda4b53a8d7bf8b3f9"

	rowD := rowData{MsgType: msgtype, DebugMode: debugmode, At: at, Type: dType, Tag: tag, DataStream: points}
	fullD := fullData{MsgType: msgtype, At: at, DebugMode: debugmode, Path: path, Data: &rowD}

	data := map[string]*fullData{did: &fullD}

	return &data, nil
}

func (tool *DeviceMockTool) asyncProducer(did string) {
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
	}(tool.ap)

	t1 := time.NewTimer(time.Second * time.Duration(*frequency))

	for {
		select {
		case <-t1.C:
			var data *map[string]*fullData
			var err error

			data, err = tool.mockData(did)
			if err != nil {
				fmt.Printf("mock data error!, %s\n", err.Error())
				data = &map[string]*fullData{"cf688ac1af5f43b38f586aefcc8ff135": &fullData{}}
			}

			sendData, err := json.Marshal(data)
			if err != nil {
				fmt.Printf("dump to json error, %s", err.Error())
			} else {
				msg := &sarama.ProducerMessage{}
				msg.Topic = *topic
				msg.Partition = int32(tool.deviceIDs[did] % *partitions)
				msg.Value = sarama.ByteEncoder(sendData)
				msg.Key = sarama.StringEncoder("device" + string(tool.deviceIDs[did]%*partitions))

				v := "async: " + strconv.Itoa(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10000))
				fmt.Fprintln(os.Stdout, v)

				fmt.Println(msg.Partition)
				tool.ap.Input() <- msg
			}

			t1.Reset(time.Second * time.Duration(*frequency))
		}
	}

}

func (tool *DeviceMockTool) syncProducer(did string) {
	t1 := time.NewTimer(time.Second * time.Duration(*frequency))

	for {
		select {
		case <-t1.C:
			var data *map[string]*fullData
			var err error

			data, err = tool.mockData(did)
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
			msg.Topic = *topic
			msg.Partition = int32(tool.deviceIDs[did] % *partitions)
			msg.Value = sarama.ByteEncoder(sendData)
			msg.Key = sarama.StringEncoder("device" + string(tool.deviceIDs[did]%*partitions))

			partition, offset, err := tool.sp.SendMessage(msg)
			if err != nil {
				logger.Println("Failed to produce message: ", err)
			}

			logger.Printf("partition=%d, offset=%d\n", partition, offset)
			t1.Reset(time.Second * time.Duration(*frequency))
		}
	}

}

func main() {
	flag.Parse()

	dmt, err := NewDeviceMockTool()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	dmt.Start()
}
