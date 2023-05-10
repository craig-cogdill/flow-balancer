package balance

import (
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

func Test_Init(t *testing.T) {
	t.Run("can init", func(t *testing.T) {
		assert.NotNil(t, New(func(in any) {}, Default))
	})

	t.Run("return nil when handler empty", func(t *testing.T) {
		assert.Nil(t, New(nil, Default))
	})
}

func getTestSettings() Settings {
	s := Default
	s.NumWorkers = 1
	return s
}

var noop = func(ignored any) {}

func Test_StartAndStop(t *testing.T) {
	t.Run("start", func(t *testing.T) {
		c := make(chan gopacket.Packet)
		b := New(noop, getTestSettings())
		assert.NotNil(t, b)
		assert.NotPanics(t, func() {
			b.Start(c)
		})
	})

	t.Run("stop with no start", func(t *testing.T) {
		b := New(noop, getTestSettings())
		assert.NotNil(t, b)
		assert.NotPanics(t, func() {
			b.Stop()
		})
	})

	t.Run("start and stop", func(t *testing.T) {
		c := make(chan gopacket.Packet)
		b := New(noop, getTestSettings())
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
	t.Run("handler fn called for every incoming packet - single-threaded", func(t *testing.T) {
		noop := func(ignored any) {}
		pkts := make(chan gopacket.Packet)
		b := New(noop, getTestSettings())
		assert.NotNil(t, b)
		b.Start(pkts)

		runs := 100
		for i := 0; i < runs; i++ {
			pkts <- gopacket.NewPacket(udpDNSPacketBytes, layers.LayerTypeEthernet, gopacket.Default)
		}
		b.Stop() // blocking

		assert.Equal(t, uint64(runs), b.Stats.Processed())
	})
}
