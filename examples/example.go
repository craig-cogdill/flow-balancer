package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sync/atomic"

	balance "github.com/craigcogdill/load-balancer"
	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/pcap"
)

type dpiWorker struct {
	totalProcessed atomic.Int64
	failed         atomic.Int64
}

func (d *dpiWorker) process(in any) {
	pkt, ok := in.(gopacket.Packet)
	if !ok {
		fmt.Println(fmt.Errorf("received a non-packet: %T\n", in))
		d.failed.Add(1)
		return
	}

	if pkt == nil {
		d.failed.Add(1)
	}

	// simulate some work
	for i := 0; i < 100; i++ {
		h := sha256.New()
		h.Write(pkt.Data())
		h.Sum(nil)
	}
	d.totalProcessed.Add(1)
}

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		panic("usage: balancer <path-to-pcap>")
	}

	dpiWorker := &dpiWorker{}
	requests := make(chan gopacket.Packet)

	settings := balance.Default
	settings.NumWorkers = 12
	b := balance.New(dpiWorker.process, settings)
	b.Start(requests)

	pcapHandle, err := pcap.OpenOffline(args[0])
	if err != nil {
		panic(err)
	}
	defer pcapHandle.Close()

	packetSource := gopacket.NewPacketSource(pcapHandle, pcapHandle.LinkType())

	processed := 0
	for pkt := range packetSource.Packets() {
		requests <- pkt
		processed++
	}
	// fmt.Println("======= Finished sending =========")
	b.Stop() // blocking

	fmt.Printf("\nSent:   %v\n", processed)
	fmt.Printf("\nBalancer:\n")
	balancerProcessed := b.Stats.Processed()
	fmt.Printf("-- processed:   %v\n", balancerProcessed)
	fmt.Printf("-- failed type: %v\n", b.Stats.FailedType())
	fmt.Printf("-- failed hash: %v\n", b.Stats.FailedHash())
	fmt.Printf("-- failed busy: %v\n", b.Stats.FailedBusy())
	workerProcessed := b.Stats.ProcessedPerWorker()
	fmt.Printf("-- per worker:\n")
	for uuid, packetsProcessed := range workerProcessed {
		fmt.Printf(
			"-- -- %v:\t%v  (%0.1f%%)\n",
			uuid,
			packetsProcessed,
			float64(packetsProcessed)/float64(balancerProcessed)*100,
		)
	}
	fmt.Printf("\nDPI Worker:\n")
	fmt.Printf("-- processed:  %v\n", dpiWorker.totalProcessed.Load())
	fmt.Printf("-- failed:     %v\n", dpiWorker.failed.Load())
}
