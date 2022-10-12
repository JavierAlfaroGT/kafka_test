//simula ser un consumer en tiempo real

package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

//-------------------------------------

func main() {

	conf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "topic_test",
		GroupID:  "g1",
		MaxBytes: 10,
	}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("error: ", err)
			continue
		}
		fmt.Println("Msg: ", string(m.Value))
	}

}
