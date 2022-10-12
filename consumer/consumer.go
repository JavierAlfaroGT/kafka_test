//lee todo el batch
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
	}

	//configuracion de tiempo
	conn.SetWriteDeadline(time.Now().Add(time.Second * 8))

	//configuracion de tiempo
	conn.SetWriteDeadline(time.Now().Add(time.Second * 8))

	//configuracion de lectura de mensajes ingresados a kafka
	batch := conn.ReadBatch(1e3, 1e6) //1e3=1000

	// bytes := make([]byte, 1e6) //ojo con el tam 1e9 no lo soporta xd

	for {
		bytes := make([]byte, 1e3) //ojo con el tam 1e9 no lo soporta xd
		_, err := batch.Read(bytes)
		if err == nil {
			//fmt.Println("mierda: ",err)
			//fmt.Println("error error erro TuT")
			//break
			fmt.Println("Msg:", string(bytes))
			time.Sleep(1 * time.Second)
		} else {
			break
		}
		//fmt.Println("Msg:",string(bytes))

	}
}
