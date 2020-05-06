package conn

import (
	"errors"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/a2dict/cmpp_conn/cmpp/protocol"
)

var sequenceIDRdn = rand.New(rand.NewSource(time.Now().UnixNano()))

type SequenceFunc func() uint32

// CmppConn 在Conn上扩展
// 1. Submit、Deliver等方法
// 2. 请求计数
// 3. 异步响应处理
type CmppConn struct {
	Conn

	currentSequenceID   uint32
	responseSequenceID  uint32
	latestResponseInSec int64
	opHandlerStorage    *OpHandlerStorage

	spId string // SP的企业代码
}

func NewCmppConn(
	addr string, sourceAddr, sharedSecret string,
) (*CmppConn, error) {

	initSequenceID := sequenceIDRdn.Uint32()

	s := &CmppConn{
		currentSequenceID:  initSequenceID,
		responseSequenceID: initSequenceID,
		opHandlerStorage:   NewOpHandlerStorage(64),
		spId:               sourceAddr,
	}

	if err := s.connect(addr); err != nil {
		return nil, err
	}

	// 登陆
	if err := s.Connect(sourceAddr, sharedSecret); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *CmppConn) connect(addr string) error {
	connection, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	s.Conn = *NewConn(connection)

	return nil
}

func (s *CmppConn) Connect(sourceAddr, sharedSecret string) error {
	op, err := protocol.NewConnect(s.newSeqNum(), sourceAddr, sharedSecret)
	if err != nil {
		return err
	}
	if err = s.Write(op); err != nil {
		return err
	}

	// Read block
	var resp protocol.Operation
	if resp, err = s.Read(); err != nil {
		return err
	}

	if resp.GetHeader().Command_Id != protocol.CMPP_CONNECT_RESP {
		return errors.New("Connect Resp Wrong RequestID")
	}

	return resp.Ok()
}

func (s *CmppConn) Terminate() error {

	op, err := protocol.NewTerminate(s.newSeqNum())
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) TerminateResp(sequenceID uint32) error {

	op, err := protocol.NewTerminateResp(sequenceID)
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) ActiveTest() error {

	op, err := protocol.NewActiveTest(s.newSeqNum())
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) ActiveTestResp(sequenceID uint32) error {

	op, err := protocol.NewActiveTestResp(sequenceID)
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) Cancel(msgId uint64) error {

	op, err := protocol.NewCancel(s.newSeqNum(), msgId)
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) DeliverResp(sequenceID uint32, msgId uint64, result uint8) error {

	op, err := protocol.NewDeliverResp(sequenceID, msgId, protocol.OK)
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) Write(op protocol.Operation) error {
	return s.WriteWithHandler(op, nil)
}

func (s *CmppConn) WriteWithHandler(op protocol.Operation, handler OpHandler) error {
	if handler != nil {
		s.opHandlerStorage.Put(op.GetHeader().Sequence_Id, handler)
	} else {
		s.opHandlerStorage.Del(op.GetHeader().Sequence_Id)
	}
	atomic.SwapUint32(&s.currentSequenceID, op.GetHeader().Sequence_Id)
	return s.Conn.Write(op)
}

func (s *CmppConn) Read() (protocol.Operation, error) {
	op, err := s.Conn.Read()
	if err != nil {
		return nil, err
	}

	sequenceID := op.GetHeader().Sequence_Id

	commandID := op.GetHeader().Command_Id
	if commandID == protocol.CMPP_SUBMIT_RESP ||
		commandID == protocol.CMPP_ACTIVE_TEST_RESP ||
		commandID == protocol.CMPP_TERMINATE_RESP {
		atomic.SwapUint32(&s.responseSequenceID, sequenceID)
	}
	atomic.SwapInt64(&s.latestResponseInSec, time.Now().Unix())

	// TODO: pooling
	if handler := s.opHandlerStorage.Get(sequenceID); handler != nil {
		go handler(op)
	}

	return op, nil
}

func (s *CmppConn) SubmitWithHandler(
	pkTotal, pkNumber, needReport, msgLevel uint8,
	serviceId string, feeUserType uint8, feeTerminalId string,
	msgFmt uint8, feeType, feeCode, srcId string,
	destTermId []string, content []byte, handler OpHandler,
) (uint32, error) {

	var (
		TP_udhi    uint8  = 0
		sequenceID uint32 = s.newSeqNum()
	)

	if pkTotal > 1 {
		TP_udhi = 1
	}

	op, err := protocol.NewSubmit(
		sequenceID,
		pkTotal, pkNumber, needReport, msgLevel,
		serviceId, feeUserType, feeTerminalId,
		0, TP_udhi, msgFmt,
		s.spId, feeType, feeCode, "", "", srcId,
		destTermId, content,
	)

	if err != nil {
		return sequenceID, err
	}

	return sequenceID, s.WriteWithHandler(op, handler)
}

func (s *CmppConn) Submit(
	pkTotal, pkNumber, needReport, msgLevel uint8,
	serviceId string, feeUserType uint8, feeTerminalId string,
	msgFmt uint8, feeType, feeCode, srcId string,
	destTermId []string, content []byte,
) (uint32, error) {
	return s.SubmitWithHandler(pkTotal, pkNumber, needReport, msgLevel,
		serviceId, feeUserType, feeTerminalId, msgFmt,
		feeType, feeCode, srcId, destTermId, content, nil)
}

func (s *CmppConn) Query(time, serviceId string) error {

	var (
		queryTye  uint8
		queryCode string
	)
	if "" != serviceId {
		queryTye = 1
		queryCode = serviceId
	}

	op, err := protocol.NewQuery(
		s.newSeqNum(), time, queryTye, queryCode)
	if err != nil {
		return err
	}

	return s.Write(op)
}

func (s *CmppConn) SequenceDiff() uint32 {
	d1 := s.currentSequenceID - s.responseSequenceID
	d2 := s.responseSequenceID - s.currentSequenceID
	if d1 < d2 {
		return d1
	}
	return d2
}

func (s *CmppConn) Idle() time.Duration {
	latestResponse := time.Unix(s.latestResponseInSec, 0)
	now := time.Now()
	return now.Sub(latestResponse)
}

func (s *CmppConn) newSeqNum() uint32 {
	return atomic.AddUint32(&s.currentSequenceID, 1)
}
