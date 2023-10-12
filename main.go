package main

import (
	"fmt"
	"net"
	"strings"
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

		if value.typ != "array" {
			fmt.Println("expected array, got ", value.typ)
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("empty array")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		// use the writer here
		writer := NewWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("unknown command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := handler(args)
		writer.Write(result)
	}
}
