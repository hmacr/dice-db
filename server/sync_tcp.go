package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/hmacr/dice-db/config"
	"github.com/hmacr/dice-db/core"
)

func readCommand(c io.ReadWriter) (*core.RedisCmd, error) {
	// TODO: max read in one shot is 512 bytes
	// To allow input > 512 bytes, then repeated read until we get
	buf := make([]byte, 512)
	n, err := c.Read(buf)
	if err != nil {
		return nil, err
	}

	tokens, err := core.DecodeArrayString(buf[:n])
	if err != nil {
		return nil, err
	}

	return &core.RedisCmd{
		Cmd:  strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	}, nil
}

func respondError(err error, c io.ReadWriter) {
	c.Write([]byte(fmt.Sprintf("-%s\r\n", err)))
}

func respond(cmd *core.RedisCmd, c io.ReadWriter) {
	err := core.EvalAndRespond(cmd, c)
	if err != nil {
		respondError(err, c)
	}
}

func RunSyncTCPServer() {
	log.Printf("starting a synchronous TCP server on %s:%d\n", config.Host, config.Port)

	var connections int = 0

	// listening to the configured host:port
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		panic(err)
	}

	for {
		// blocking call: waiting for the new client to connect
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		// increement the number of concurrent connections
		connections += 1
		log.Printf("client connected with address: %s, concurrent connections: %d\n", conn.RemoteAddr(), connections)

		for {
			// over the socket, continuously read the command and print it out
			cmd, err := readCommand(conn)
			if err != nil {
				conn.Close()
				connections -= 1
				log.Printf("client disconnected: %s, concurrent connections: %d\n", conn.RemoteAddr(), connections)
				if err == io.EOF {
					break
				}
				log.Printf("error reading command: %v\n", err)
			}
			respond(cmd, conn)
		}
	}
}
