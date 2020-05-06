package protocol

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/yedamao/encoding"
)

var (
	longtextRefIndex = rand.New(rand.NewSource(time.Now().UnixNano())).Uint32()
	LongtextPrefixBS = []byte{0x05, 0x00, 0x03}
)

type LongTextMessage struct {
	Total  byte
	Ref    byte
	MsgFmt uint8
	Pdus   []Pdu
}

type Pdu struct {
	Content string
	No      byte
	Data    []byte
}

func SplitLongText(content string) *LongTextMessage {
	// use UCS2 by default
	ref := byte(atomic.AddUint32(&longtextRefIndex, 1))

	bs := encoding.UTF82UCS2([]byte(content))
	bsLen := len(bs)
	if bsLen <= 140 {
		return &LongTextMessage{
			Total:  1,
			Ref:    ref,
			MsgFmt: UCS2,
			Pdus: []Pdu{{
				Content: content,
				No:      1,
				Data:    bs,
			}},
		}
	}

	// 分包， 每个包增加 6bytes头
	pkgSize := 140 - 6
	total := bsLen / pkgSize
	if bsLen%pkgSize > 0 {
		total += 1
	}
	pdus := make([]Pdu, 0, total)
	for i := 0; i < total; i++ {
		t := bytes.Buffer{}
		t.Write(getPduPrefix(ref, byte(total), byte(i+1)))

		begin := i * pkgSize
		end := (i + 1) * pkgSize
		if end > bsLen {
			end = bsLen
		}
		t.Write(bs[begin:end])
		pdus = append(pdus, Pdu{
			Content: string(encoding.UCS22UTF8(bs[begin:end])),
			No:      byte(i + 1),
			Data:    t.Bytes(),
		})
	}

	return &LongTextMessage{
		Total:  byte(total),
		Ref:    ref,
		MsgFmt: UCS2,
		Pdus:   pdus,
	}
}

func getPduPrefix(ref, total, no byte) []byte {
	bf := bytes.Buffer{}
	bf.Write(LongtextPrefixBS)
	bf.WriteByte(ref)
	bf.WriteByte(total)
	bf.WriteByte(no)
	return bf.Bytes()
}
