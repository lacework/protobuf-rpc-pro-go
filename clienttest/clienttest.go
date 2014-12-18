package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spullara/protobuf-rpc-pro-go/protobufclient"
	"time"
)

func main() {
	client, err := protobufclient.Connect("localhost", 53853)
	checkError(err)

	output := &protobufclient.Output{}
	err = client.Call("lacework.InputOutput", "transform", &protobufclient.Input{Name: proto.String("Sam")}, output)
	checkError(err)

	fmt.Println(output.GetText())

	start := time.Now()
	semaphore := make(chan int, 100)
	for i := 0; i < 4; i++ {
		semaphore <- i
	}
	for i := 0; i < 100000; i++ {
		take := <-semaphore
		go func() {
			output := &protobufclient.Output{}
			err = client.Call("lacework.InputOutput", "transform", &protobufclient.Input{Name: proto.String("Sam")}, output)
			semaphore <- take
			checkError(err)
		}()
	}
	fmt.Println(time.Now().Sub(start))
	for i := 0; i < 4; i++ {
		<-semaphore
	}
	fmt.Println(time.Now().Sub(start))
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}
