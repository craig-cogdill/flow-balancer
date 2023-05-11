package balance

import (
	"sync"
	"testing"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/stretchr/testify/assert"
)

var udpDNSPacketBytes = []byte{
	0x00, 0xc0, 0x9f, 0x32, 0x41, 0x8c, 0x00, 0xe0, 0x18, 0xb1, 0x0c, 0xad, 0x08, 0x00, 0x45, 0x00,
	0x00, 0x3c, 0x00, 0x00, 0x40, 0x00, 0x40, 0x11, 0x65, 0x43, 0xc0, 0xa8, 0xaa, 0x08, 0xc0, 0xa8,
	0xaa, 0x14, 0x80, 0x1b, 0x00, 0x35, 0x00, 0x28, 0xa5, 0xcd, 0x7f, 0x39, 0x01, 0x00, 0x00, 0x01,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x77, 0x77, 0x77, 0x06, 0x6e, 0x65, 0x74, 0x62, 0x73,
	0x64, 0x03, 0x6f, 0x72, 0x67, 0x00, 0x00, 0x1c, 0x00, 0x01,
}

var udpDNSPacketBytes2 = []byte{
	0x00, 0x12, 0xa9, 0x00, 0x32, 0x23, 0x00, 0x60, 0x08, 0x45, 0xe4, 0x55, 0x08, 0x00, 0x45, 0x00,
	0x00, 0x54, 0x87, 0xf0, 0x00, 0x00, 0x80, 0x11, 0x6a, 0xa2, 0xc0, 0xa8, 0xaa, 0x38, 0xd9, 0x0d,
	0x04, 0x18, 0x06, 0xac, 0x00, 0x35, 0x00, 0x40, 0x7c, 0x51, 0xf1, 0x61, 0x01, 0x00, 0x00, 0x01,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x5f, 0x6c, 0x64, 0x61, 0x70, 0x04, 0x5f, 0x74, 0x63,
	0x70, 0x02, 0x64, 0x63, 0x06, 0x5f, 0x6d, 0x73, 0x64, 0x63, 0x73, 0x0b, 0x75, 0x74, 0x65, 0x6c,
	0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x73, 0x05, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x00, 0x00, 0x21,
	0x00, 0x01,
}

func Test_Init(t *testing.T) {
	t.Run("can init", func(t *testing.T) {
		assert.NotNil(t, New(func(in any) {}, Default))
	})

	t.Run("return nil when handler empty", func(t *testing.T) {
		assert.Nil(t, New(nil, Default))
	})
}

func getTestSettings(numWorkers int) Settings {
	s := Default
	s.NumWorkers = numWorkers
	return s
}

var noop = func(ignored any) {}

func Test_StartAndStop(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		c := make(chan gopacket.Packet)
		b := New(noop, getTestSettings(1))
		assert.NotNil(t, b)
		assert.NotPanics(t, func() {
			b.Start(c)
		})
	})

	t.Run("stop with no start", func(t *testing.T) {
		b := New(noop, getTestSettings(1))
		assert.NotNil(t, b)
		assert.NotPanics(t, func() {
			b.Stop()
		})
	})

	t.Run("start and stop", func(t *testing.T) {
		c := make(chan gopacket.Packet)
		b := New(noop, getTestSettings(1))
		assert.NotNil(t, b)
		assert.NotPanics(t, func() {
			b.Start(c)
		})
		assert.NotPanics(t, func() {
			b.Stop()
		})
	})
}

func Test_Functionality(t *testing.T) {
	noop := func(ignored any) {}

	t.Run("handler fn called for every incoming packet - single-threaded", func(t *testing.T) {
		pkts := make(chan gopacket.Packet)
		b := New(noop, getTestSettings(1))
		assert.NotNil(t, b)
		b.Start(pkts)

		runs := 1000
		for i := 0; i < runs; i++ {
			pkts <- gopacket.NewPacket(udpDNSPacketBytes, layers.LayerTypeEthernet, gopacket.Default)
		}

		b.Stop() // blocking

		assert.Equal(t, uint64(runs), b.Stats.Processed())
		assert.Zero(t, b.Stats.Failed())
	})

	t.Run("handler fn called for every incoming packet - multi-threaded", func(t *testing.T) {
		pkts := make(chan gopacket.Packet)
		b := New(noop, getTestSettings(2))
		assert.NotNil(t, b)
		b.Start(pkts)

		var wg sync.WaitGroup
		wg.Add(2)

		runs := 100
		sendPkts := func(pktBytes []byte) {
			for i := 0; i < runs; i++ {
				pkts <- gopacket.NewPacket(pktBytes, layers.LayerTypeEthernet, gopacket.Default)
			}
			wg.Done()
		}

		// These two packets have unique 5-tuples, which should guarantee that worker 1 receives
		// the first packet of udpDNSPacketBytes and worker 2 receives the first packet of
		// udpDNSPacketBytes2. This is because after receiving 1 packet, worker 1 should be moved
		// to the bottom of the heap. Worker 2 will then get the first packet of udpDNSPacketBytes2.
		// The hashes of each packet will then be associated with the workers, and each one should
		// process 100 of that packet during the test.
		go sendPkts(udpDNSPacketBytes)
		go sendPkts(udpDNSPacketBytes2)
		wg.Wait()

		b.Stop() // blocking

		assert.Equal(t, uint64(runs*2), b.Stats.Processed())
	})
}
