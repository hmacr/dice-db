package server

import (
	"log"
	"net"
	"syscall"
	"time"

	"github.com/hmacr/dice-db/config"
	"github.com/hmacr/dice-db/core"
)

var connections int = 0
var cronFrequency time.Duration = 1 * time.Second
var lastCronExecTime time.Time = time.Now()

const maxConnections int = 20_000

/*

1. Create an KQUEUE using `kqueue` system call for receiving I/O signals from Kernel.
2. Create a server socket/FD for accepting client connections.
3. Add server socket to the KQUEUE for EVFILT_READ signal using `kevent` system call.
4. Wait for EPOLL signal using `kevent` system call.
5. When new connection is received, create connection socket/FD and add it to KQUEUE (we need to listen to input events).
6. When new input is received on any of the connection FD, it means some client has sent the command, so handle the command.

*/

func RunAsyncTCPServer() error {
	log.Printf("starting an asynchronous TCP server on %s:%d\n", config.Host, config.Port)

	// Create KQUEUE event objects to hold events
	var events []syscall.Kevent_t = make([]syscall.Kevent_t, maxConnections)

	// Create a socket
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(serverFD)

	// Make the socket operate in non-blocking mode
	err = syscall.SetNonblock(serverFD, true)
	if err != nil {
		return err
	}

	ipv4 := net.ParseIP(config.Host)
	socketAddress := syscall.SockaddrInet4{
		Addr: [4]byte{ipv4[0], ipv4[1], ipv4[2], ipv4[3]},
		Port: config.Port,
	}
	err = syscall.Bind(serverFD, &socketAddress)
	if err != nil {
		return err
	}

	// Start listening
	err = syscall.Listen(serverFD, maxConnections)
	if err != nil {
		return err
	}

	// AsyncIO from here onwards!

	// Create KQUEUE instance
	kqueueFD, err := syscall.Kqueue()
	if err != nil {
		return err
	}
	defer syscall.Close(kqueueFD)

	// Specify the events we want to get signals about and set the socket on which these hints are associated
	var sockerServerEvent syscall.Kevent_t = syscall.Kevent_t{
		Ident:  uint64(serverFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}

	// Listen to read events on the Server itself
	_, err = syscall.Kevent(kqueueFD, []syscall.Kevent_t{sockerServerEvent}, nil, nil)
	if err != nil {
		return err
	}

	for {
		// Run cron job for active deletion of expired keys
		if time.Now().After(lastCronExecTime.Add(cronFrequency)) {
			core.DeleteExpiredKeys()
			lastCronExecTime = time.Now()
		}

		// See if any FD is ready for IO
		n, err := syscall.Kevent(kqueueFD, nil, events, nil)
		if err != nil {
			continue
		}

		for i := 0; i < n; i++ {
			eventFD := int(events[i].Ident)

			// If the socket server itself is ready for IO
			if eventFD == serverFD {
				// Accept incoming connection from a client
				clientFD, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Printf("connection err: %v\n", err)
					continue
				}

				// Increase the number of concurrent connections count
				connections++

				// Make the socket operate in non-blocking mode
				syscall.SetNonblock(clientFD, true)

				// Specify the events we want to get signals about and set the socket on which these hints are associated
				var socketClientEvent syscall.Kevent_t = syscall.Kevent_t{
					Ident:  uint64(clientFD),
					Filter: syscall.EVFILT_READ,
					Flags:  syscall.EV_ADD,
				}

				// Add this new TCP connection to be monitored
				_, err = syscall.Kevent(kqueueFD, []syscall.Kevent_t{socketClientEvent}, nil, nil)
				if err != nil {
					log.Printf("connection err: %v\n", err)
					continue
				}
			} else {
				comm := core.FDComm{Fd: eventFD}
				cmd, err := readCommand(comm)
				if err != nil {
					syscall.Close(eventFD)
					connections--
					continue
				}
				respond(cmd, comm)
			}
		}
	}
}
