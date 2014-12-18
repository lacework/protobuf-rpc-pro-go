package protobufclient

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"sync"
	"sync/atomic"
)

type Client struct {
	currentId int32
	conn      net.Conn
	connected chan ConnectResponse
	responses map[int32]chan RpcResponse
	errors    map[int32]chan RpcError
	mutex     *sync.Mutex
}

func (m *Client) getNextId() int32 {
	return atomic.AddInt32(&m.currentId, 1)
}

func (m *Client) handleResponses(correlationId int32) {
	messageBuffer := &bytes.Buffer{}
	var readBuffer [512]byte
	for {
		var readLength int
		readLength, err := m.conn.Read(readBuffer[0:6])
		if err != nil {
			return
		}
		messageLength, offset := proto.DecodeVarint(readBuffer[0:])
		var toWrite int
		if int(messageLength) < readLength-offset {
			toWrite = int(messageLength) + offset
		} else {
			toWrite = readLength
		}
		messageBuffer.Write(readBuffer[offset:toWrite])
		for messageBuffer.Len() < int(messageLength) {
			max := int(messageLength) - messageBuffer.Len()
			readLength, err2 := m.conn.Read(readBuffer[0:max])
			if err2 != nil {
				return
			}
			messageBuffer.Write(readBuffer[0:readLength])
		}
		wirepayload := &WirePayload{}
		unmarshallerr := proto.Unmarshal(messageBuffer.Bytes(), wirepayload)
		if unmarshallerr != nil {
			m.conn.Close()
			return
		}
		if wirepayload.ConnectResponse != nil {
			m.connected <- *wirepayload.ConnectResponse
		}
		if wirepayload.RpcResponse != nil {
			response := *wirepayload.RpcResponse
			m.responses[*response.CorrelationId] <- response
		}
		if wirepayload.RpcError != nil {
			error := *wirepayload.RpcError
			m.errors[*error.CorrelationId] <- error
		}
		messageBuffer.Truncate(0)
	}
}

func Connect(host string, port int32) (*Client, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	connected := make(chan ConnectResponse)
	client := &Client{currentId: 0, conn: conn, connected: connected}
	client.mutex = &sync.Mutex{}
	correlationId := client.getNextId()
	go client.handleResponses(correlationId)
	var localHost string
	var localPort int32
	fmt.Sscanf(conn.LocalAddr().String(), "%s:%d", &localHost, &localPort)
	connectRequest := &ConnectRequest{
		CorrelationId:  &correlationId,
		ClientHostName: &localHost,
		ClientPort:     &localPort,
		ClientPID:      proto.String("goclient"),
		Compress:       proto.Bool(false),
	}
	client.send(&WirePayload{ConnectRequest: connectRequest})
	var connectResponse = <-connected
	if *connectResponse.CorrelationId != correlationId {
		return nil, errors.New("Invalid correlation ID")
	}
	client.responses = make(map[int32]chan RpcResponse)
	client.errors = make(map[int32]chan RpcError)
	return client, nil
}

func (m *Client) send(payload *WirePayload) error {
	bytes, err2 := proto.Marshal(payload)
	if err2 != nil {
		return err2
	}
	m.mutex.Lock()
	m.conn.Write(proto.EncodeVarint(uint64(len(bytes))))
	_, err3 := m.conn.Write(bytes)
	m.mutex.Unlock()
	if err3 != nil {
		return err3
	}
	return nil
}

func (m *Client) call(call *RpcRequest) (*RpcResponse, *RpcError) {
	responseChannel := make(chan RpcResponse)
	errorChannel := make(chan RpcError)
	correlationId := *call.CorrelationId
	m.setCorrelationIds(correlationId, responseChannel, errorChannel)
	m.send(&WirePayload{RpcRequest: call})
	select {
	case response := <-responseChannel:
		m.setCorrelationIds(correlationId, nil, nil)
		return &response, nil
	case error := <-errorChannel:
		m.setCorrelationIds(correlationId, nil, nil)
		return nil, &error
	}
}

func (m *Client) setCorrelationIds(correlationId int32, responseChannel chan RpcResponse, errorChannel chan RpcError) {
	m.mutex.Lock()
	m.responses[correlationId] = responseChannel
	m.errors[correlationId] = errorChannel
	m.mutex.Unlock()
}

func (m *Client) Call(serviceIdentifier string, methodIdentifier string, rpcMessage proto.Message, rpcResponse proto.Message) error {
	value, err := proto.Marshal(rpcMessage)
	if err != nil {
		return err
	}
	response, rpcError := m.call(&RpcRequest{
		CorrelationId:     proto.Int32(m.getNextId()),
		ServiceIdentifier: &serviceIdentifier,
		MethodIdentifier:  &methodIdentifier,
		RequestBytes:      value,
	})
	if rpcError != nil {
		return errors.New(rpcError.GetErrorMessage())
	}
	err = proto.Unmarshal(response.GetResponseBytes(), rpcResponse)
	if err != nil {
		return err
	}
	return nil
}
