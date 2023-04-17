package main

import (
	"crypto/sha256"
	"fmt"
	"sync/atomic"

	"github.com/craigcogdill/load-balancer/pkg/balance"
	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/pcap"
)

type dpiWorker struct {
	processed atomic.Int64
	failed    atomic.Int64
}

func (d *dpiWorker) process(in any) {
	pkt, ok := in.(gopacket.Packet)
	if !ok {
		fmt.Println(fmt.Errorf("received a non-packet: %T\n", in))
		d.failed.Add(1)
		return
	}

	// do some work
	for i := 0; i < 100; i++ {
		h := sha256.New()
		h.Write(pkt.Data())
		h.Sum(nil)
	}
	d.processed.Add(1)
}

func main() {
	dpiWorker := &dpiWorker{}
	settings := balance.Settings{
		NumWorkers:      24,
		Handler:         dpiWorker.process,
		WorkerQueueSize: 50000,
	}
	requests := make(chan gopacket.Packet)

	b := balance.New(settings)
	b.Start(requests)

	handle, err := pcap.OpenOffline("/home/craig/workspace/pcaps/modbus_merged.pcap")
	// handle, err := pcap.OpenOffline("/home/craig/workspace/pcaps/dns.pcap")
	if err != nil {
		panic(err)
	}
	defer handle.Close()

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())

	processed := 0
	for pkt := range packetSource.Packets() {
		requests <- pkt
		processed++
	}
	fmt.Println("======= Finished sending =========")

	for {
		if (b.Stats.Processed() +
			b.Stats.FailedBusy() +
			b.Stats.FailedHash() +
			b.Stats.FailedType()) == int64(processed) {
			break
		}
	}

	fmt.Printf("\nSent:   %v\n", processed)

	fmt.Printf("\nBalancer:\n")
	fmt.Printf("-- processed:   %v\n", b.Stats.Processed())
	fmt.Printf("-- failed type: %v\n", b.Stats.FailedType())
	fmt.Printf("-- failed hash: %v\n", b.Stats.FailedHash())
	fmt.Printf("-- failed busy: %v\n", b.Stats.FailedBusy())

	fmt.Printf("\nDPI Worker:\n")
	fmt.Printf("-- processed:  %v\n", dpiWorker.processed.Load())
	fmt.Printf("-- failed:     %v\n", dpiWorker.failed.Load())
}
