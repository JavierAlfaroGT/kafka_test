package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	//configuracion inicial
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)

	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	//configuracion de tiempo
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	//configuracion del mensaje a ingresar a kafka
	conn.WriteMessages(kafka.Message{Value: []byte("hello world from golang")})
}
