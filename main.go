package main

import (
	"fmt"
	"net"
)

func main() {

	fmt.Println("Listening on port :6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()
	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println("error reading from client: ", err.Error())
			return
		}

		fmt.Println(value)

		// ignore request and send back a PONG
		conn.Write([]byte("+OK\r\n"))
	}
}
