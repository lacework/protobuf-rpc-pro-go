package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
	"protobufclient"
	"time"
)

func main() {
	client, err := protobufclient.Connect("localhost", 50862)
	checkError(err)

	output := &protobufclient.Output{}
	err = client.Call("lacework.InputOutput", "transform", &protobufclient.Input{Name: proto.String("Sam")}, output)
	checkError(err)

	fmt.Println(output.GetText())

	start := time.Now()
	for i := 0; i < 100000; i++ {
		go func() {
			output := &protobufclient.Output{}
			err = client.Call("lacework.InputOutput", "transform", &protobufclient.Input{Name: proto.String("Sam")}, output)
			checkError(err)
		}()
	}
	fmt.Println(time.Now().Sub(start))
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
