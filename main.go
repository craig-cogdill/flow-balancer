package main

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"time"

	"github.com/craigcogdill/load-balancer/pkg/balance"
	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/pcap"
)

func workerThreadOp(in any) {
	pkt, ok := in.(gopacket.Packet)
	if !ok {
		fmt.Println(fmt.Errorf("received a non-packet: %T\n", in))
		return
	}

	// do some work
	for i := 0; i < 100; i++ {
		h := sha256.New()
		h.Write(pkt.Data())
		h.Sum(nil)
	}
}

func requester(out chan<- any) {
	for {
		time.Sleep(time.Duration(rand.Int63n(int64(2 * time.Second))))
		out <- struct{}{} // fire and forget
	}
}

func main() {
	settings := balance.Settings{
		NumWorkers: 20,
		Handler:    workerThreadOp,
	}
	requests := make(chan any)

	b := balance.New(settings)
	b.Start(requests)

	handle, err := pcap.OpenOffline("/home/craig/workspace/pcaps/modbus_merged.pcap")
	if err != nil {
		panic(err)
	}
	defer handle.Close()

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())

	for pkt := range packetSource.Packets() {
		requests <- pkt
	}

	time.Sleep(10 * time.Second)
}
