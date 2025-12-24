package server

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/hmacr/dice-db/config"
)

func readCommand(conn net.Conn) (string, error) {
	buf := make([]byte, 512)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func respond(cmd string, conn net.Conn) error {
	_, err := conn.Write([]byte(cmd))
	return err
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
			log.Printf("command: %s\n", cmd)
			if err = respond(cmd, conn); err != nil {
				log.Printf("error writing back: %v\n", err)
			}
		}
	}
}
