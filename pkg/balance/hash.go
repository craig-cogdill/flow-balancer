package balance

import (
	"encoding/binary"
	"errors"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/satta/gommunityid"
)

var cid gommunityid.CommunityID

func init() {
	var err error
	cid, err = gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		panic("unable to initialize community ID")
	}
}

func hash(pkt gopacket.Packet) (string, error) {
	nl := pkt.NetworkLayer()
	if nl == nil {
		return "", errors.New("no transport layer")
	}
	tl := pkt.TransportLayer()
	if tl == nil {
		return "", errors.New("no transport layer")
	}

	var transportID uint8 = 0
	switch tl.LayerType() {
	case layers.LayerTypeICMPv4:
		transportID = gommunityid.ProtoICMP
	case layers.LayerTypeTCP:
		transportID = gommunityid.ProtoTCP
	case layers.LayerTypeUDP:
		transportID = gommunityid.ProtoUDP
	case layers.LayerTypeICMPv6:
		transportID = gommunityid.ProtoICMP6
	case layers.LayerTypeSCTP:
		transportID = gommunityid.ProtoSCTP
	}

	return cid.CalcHex(
		gommunityid.MakeFlowTuple(
			nl.NetworkFlow().Src().Raw(),
			nl.NetworkFlow().Dst().Raw(),
			binary.BigEndian.Uint16(tl.TransportFlow().Src().Raw()),
			binary.BigEndian.Uint16(tl.TransportFlow().Dst().Raw()),
			transportID,
		),
	), nil
}
