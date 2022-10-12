package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Obj struct {
	Id   string          `json:"id"`
	Data json.RawMessage `json:"data"`
}

func main() {

	//configuracion inicial
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)

	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	//configuracion de tiempo
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	//-------------------------------- de []bytes a JSON--------------------
	byt := []byte(`{"id":"someID","data":"hello world"}`)

	var obj Obj
	if err := json.Unmarshal(byt, &obj); err != nil {
		panic(err)
	}

	fmt.Println(obj)

	//---------------------------------de JSON a []bytes
	b, _ := json.Marshal(obj.Data)
	// Convert bytes to string.
	s := string(b)
	fmt.Println(s)
	//----------------------------------------------------------------------

	//configuracion del mensaje a ingresar a kafka
	conn.WriteMessages(kafka.Message{Value: b})
	//	conn.WriteMessages(kafka.Message{Value: []byte("hello world from golang")})

}
