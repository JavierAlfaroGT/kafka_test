package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Obj2 struct {
	Lista []Obj `json:"lista"`
}

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
	//-------------------------------- de []bytes a JSON--------------------
	byt := []byte(`{"lista":[{"id":"1","data":"hello world1"},{"id":"2","data":"hello world2"},{"id":"3","data":"hello world3"},{"id":"4","data":"hello world4"}]}`)

	var obj Obj2
	if err := json.Unmarshal(byt, &obj); err != nil {
		panic(err)
	}

	fmt.Println("[]BYTE -> JSON\n", obj)

	//---------------------------------de JSON a []bytes
	b, _ := json.Marshal(obj)
	// Convert bytes to string.
	s := string(b)
	fmt.Println("------------------------------\nJSON -> []BYTE \n", s)

	//----------------------------------------------------------------------

	//configuracion del mensaje a ingresar a kafka
	conn.WriteMessages(kafka.Message{Value: b})
	//	conn.WriteMessages(kafka.Message{Value: []byte("hello world from golang")})

}
