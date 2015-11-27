package main

import (
	"code.google.com/p/goprotobuf/proto"
	"flag"
	"fmt"
	. "github.com/6xiao/GoQuant/DataType"
	"github.com/6xiao/go/Common"
	"io"
	"math/rand"
	"net"
	"time"
	"unsafe"
)

var (
	flgName    = flag.String("name", "ST", "module name")
	flgTrade   = flag.String("trade", "TQ", "QuoteChannelName")
	flgQuote   = flag.String("quote", "QUOTEtq", "QuoteChannelName")
	flgSplit   = flag.String("split", "OS", "order split")
	flgAccount = flag.String("account", "00000048", "account")
	flgTicker  = flag.String("ticker", "IF1501", "ticker name")
	flgMc      = flag.String("mc", "127.0.0.1:5381", "message channel ip:port")
)

const (
	buflen = 256
	back   = 16
)

func Register(conn net.Conn, name string) {
	sockbuf := make([]byte, 1024)
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	mh.Msglen = uint64(MsgLen) - HeadLen
	mh.Sender = MakeID(name)
	fmt.Println("Register:", name, mh.Sender)
	conn.Write(sockbuf[:MsgLen])
}

func SendTo(conn net.Conn, sender, recver, copyer uint64, tp ConstDefine, buf []byte) {
	sockbuf := make([]byte, MsgLen+RecLen+uint32(len(buf)))
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	rh := (*RecordHead)(unsafe.Pointer(&sockbuf[MsgLen]))
	bodybuf := sockbuf[MsgLen+RecLen:]
	mh.Msglen = uint64(len(sockbuf)) - HeadLen
	mh.Sender = sender
	mh.Recver = recver
	mh.Copyer = copyer
	rh.HashCode = Common.Hash(buf)
	rh.RecType = uint32(tp)
	rh.Length = uint32(len(buf))
	copy(bodybuf, buf)
	conn.Write(sockbuf)
}

func Recv(conn net.Conn) {
	for {
		nethead := [HeadLen]byte{}
		if _, e := io.ReadFull(conn, nethead[:]); e != nil {
			fmt.Println(conn.RemoteAddr().String(), "Error recv head : ", e)
			break
		}

		msglen := *(*uint64)(unsafe.Pointer(&nethead))
		msgbuf := make([]byte, HeadLen+msglen)

		if _, e := io.ReadFull(conn, msgbuf[HeadLen:]); e != nil {
			fmt.Println(conn.RemoteAddr().String(), "Error recv body : ", e)
			break
		}

		mh := (*MessageHead)(unsafe.Pointer(&msgbuf[0]))
		mh.Msglen = msglen
		if uint32(len(msgbuf)) < MsgLen+RecLen {
			fmt.Println("error of message head")
			continue
		}

		rh := (*RecordHead)(unsafe.Pointer(&msgbuf[MsgLen]))
		if uint32(len(msgbuf)) < MsgLen+RecLen+rh.Length {
			fmt.Println("error of message length")
			continue
		}

		bodybuf := msgbuf[MsgLen+RecLen : MsgLen+RecLen+rh.Length]
		if rh.HashCode != Common.Hash(bodybuf) {
			fmt.Println("error of message hashcode")
			continue
		}

		switch rh.RecType {
		case uint32(ConstDefine_E_DepthMarketData):
			dmd := &DepthMarketData{}
			err := proto.Unmarshal(bodybuf, dmd)
			if err != nil || dmd.GetTickerName() != *flgTicker {
				break
			}

			fmt.Println(dmd.GetTime(), dmd.GetTickerName(), dmd.GetVolume(), dmd.GetLastPrice())
			order := dmd.GetOrder()
			for i, ord := range order {
				fmt.Println(i, ord.GetTime(), ord.GetAccount(), ord.GetTickerName(),
					ord.GetVolume(), ord.GetPrice(), ord.GetLastFilled(),
					ord.GetTotalFilled(), ord.GetExplain(), ord.GetOrderID(),
					ord.GetIsForce(), ord.GetIsToday())
			}
			position := dmd.GetPosition()
			for i, pos := range position {
				fmt.Println(i, pos.GetAccount(), pos.GetTickerName(), pos.GetMargin(),
					"longAvgPrice/q/v/c:", pos.GetLongAvgPrice(), pos.GetQueuingLongVolume(),
					pos.GetValidLongVolume(), pos.GetClosingLongVolume(),
					"shotAvgPrice/q/v/c:", pos.GetShortAvgPrice(), pos.GetQueuingShortVolume(),
					pos.GetValidShortVolume(), pos.GetClosingShortVolume())
			}

		case uint32(ConstDefine_E_RspSendOrder):
			so := &RspSendOrder{}
			err := proto.Unmarshal(bodybuf, so)
			if err != nil {
				fmt.Println("error : unmarshal message RspSendOrder")
				break
			}
			fmt.Println("OnRecvRspSendOrder:")
			if ord := so.GetLastReport(); ord != nil {
				fmt.Println(ord.GetTime(), ord.GetAccount(), ord.GetTickerName(),
					ord.GetVolume(), ord.GetPrice(), ord.GetLastFilled(), ord.GetTotalFilled(),
					ord.GetExplain(), ord.GetOrderID(), ord.GetIsForce(), ord.GetIsToday())
			}

		case uint32(ConstDefine_E_ReqCancelOrder):
			co := &ReqCancelOrder{}
			err := proto.Unmarshal(bodybuf, co)
			if err != nil {
				fmt.Println("error : unmarshal message ReqCancelOrder")
				break
			}
			fmt.Println("OnRecvReqCancelOrder:")
			fmt.Println(co.GetAccount(), co.GetTickerName(), co.GetOrderID())

		case uint32(ConstDefine_E_RspCancelOrder):
			co := &RspCancelOrder{}
			err := proto.Unmarshal(bodybuf, co)
			if err != nil {
				fmt.Println("error : unmarshal message RspCancelOrder")
				break
			}
			fmt.Println("OnRecvRspCancelOrder:")
			if ord := co.GetLastReport(); ord != nil {
				fmt.Println(ord.GetTime(), ord.GetAccount(), ord.GetTickerName(),
					ord.GetVolume(), ord.GetPrice(), ord.GetLastFilled(), ord.GetTotalFilled(),
					ord.GetExplain(), ord.GetOrderID(), ord.GetIsForce(), ord.GetIsToday())
			}

		case uint32(ConstDefine_E_RspOrdersInfo):
			oi := &RspOrdersInfo{}
			err := proto.Unmarshal(bodybuf, oi)
			if err != nil {
				fmt.Println("error : unmarshal message RspOrdersInfo")
				break
			}
			fmt.Println("OnRecvRspOrdersInfo")
			if order := oi.GetOrder(); order != nil {
				for i, ord := range order {
					fmt.Println(i, ord.GetTime(), ord.GetAccount(), ord.GetTickerName(),
						ord.GetVolume(), ord.GetPrice(), ord.GetLastFilled(),
						ord.GetTotalFilled(), ord.GetExplain(), ord.GetOrderID(),
						ord.GetIsForce(), ord.GetIsToday())
				}
			}

		case uint32(ConstDefine_E_RspPosition):
			p := &RspPosition{}
			err := proto.Unmarshal(bodybuf, p)
			if err != nil {
				fmt.Println("error : unmarshal message RspPosition")
				break
			}
			fmt.Println("OnRecvRspPosition")
			if position := p.GetPosition(); position != nil {
				for i, pos := range position {
					fmt.Println(i, pos.GetAccount(), pos.GetTickerName(), pos.GetMargin(),
						"longAvgPrice/q/v/c:", pos.GetLongAvgPrice(), pos.GetQueuingLongVolume(),
						pos.GetValidLongVolume(), pos.GetClosingLongVolume(),
						"shotAvgPrice/q/v/c:", pos.GetShortAvgPrice(), pos.GetQueuingShortVolume(),
						pos.GetValidShortVolume(), pos.GetClosingShortVolume())
				}
			}

		case uint32(ConstDefine_E_ReqPosition):
			p := &ReqPosition{}
			err := proto.Unmarshal(bodybuf, p)
			if err != nil {
				fmt.Println("error : unmarshal message ReqPosition")
				break
			}
			fmt.Println("OnRecvReqPosition")
			fmt.Println(p.GetAccount(), p.GetTickerName())

		case uint32(ConstDefine_E_RspAccount):
			a := &RspAccount{}
			err := proto.Unmarshal(bodybuf, a)
			if err != nil {
				fmt.Println("error : unmarshal message RspAccount")
				break
			}
			fmt.Println("OnRecvRspAccount/v/m:", a.GetAccount(), a.GetAvailable(), a.GetMargin())
			order := a.GetOrders()
			for i, ord := range order {
				fmt.Println(i, ord.GetTime(), ord.GetAccount(), ord.GetTickerName(),
					ord.GetVolume(), ord.GetPrice(), ord.GetLastFilled(),
					ord.GetTotalFilled(), ord.GetExplain(), ord.GetOrderID(),
					ord.GetIsForce(), ord.GetIsToday())
			}
			position := a.GetPositions()
			for i, pos := range position {
				fmt.Println(i, pos.GetAccount(), pos.GetTickerName(), pos.GetMargin(),
					"longAvgPrice/q/v/c:", pos.GetLongAvgPrice(), pos.GetQueuingLongVolume(),
					pos.GetValidLongVolume(), pos.GetClosingLongVolume(),
					"shotAvgPrice/q/v/c:", pos.GetShortAvgPrice(), pos.GetQueuingShortVolume(),
					pos.GetValidShortVolume(), pos.GetClosingShortVolume())
			}

		case uint32(ConstDefine_E_RspAlgoOrder):
			ao := &RspAlgoOrder{}
			err := proto.Unmarshal(bodybuf, ao)
			if err != nil {
				fmt.Println("error : unmarshal message RspAlgoOrder")
				break
			}
			fmt.Println("OnRecvRspAlgoOrder")

		case uint32(ConstDefine_E_RspTickerInfo):
			ti := &RspTickerInfo{}
			err := proto.Unmarshal(bodybuf, ti)
			if err != nil {
				fmt.Println("error : unmarshal message RspTickerInfo")
				break
			}
			fmt.Println("OnRecvRspTickerInfo")
			for i, ts := range ti.GetTradingSession() {
				fmt.Println(i, ts.GetStartTime(), ts.GetEndTime())
			}
		}
	}
}

func Request(conn net.Conn, ticker, account string) {
	sender, recver := MakeID(*flgTrade), MakeID(*flgName)

	{
		req := ReqTickerInfo{TickerName: &ticker}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqTickerInfo, buf)
		time.Sleep(time.Second)
	}

	{
		req := ReqTickerInfo{}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqTickerInfo, buf)
		time.Sleep(time.Second)
	}

	{
		status := OrderStatus_OrderStatus_New
		req := ReqOrdersInfo{TickerName: &ticker, Account: &account, ReqStatus: &status}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqOrdersInfo, buf)
		time.Sleep(time.Second)
	}

	{
		status := OrderStatus_OrderStatus_PartFill
		req := ReqOrdersInfo{TickerName: &ticker, Account: &account, ReqStatus: &status}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqOrdersInfo, buf)
		time.Sleep(time.Second)
	}

	{
		status := OrderStatus_OrderStatus_Filled
		req := ReqOrdersInfo{TickerName: &ticker, Account: &account, ReqStatus: &status}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqOrdersInfo, buf)
		time.Sleep(time.Second)
	}

	{
		status := OrderStatus_OrderStatus_New
		req := ReqOrdersInfo{Account: &account, ReqStatus: &status}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqOrdersInfo, buf)
		time.Sleep(time.Second)
	}

	{
		status := OrderStatus_OrderStatus_PartFill
		req := ReqOrdersInfo{Account: &account, ReqStatus: &status}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqOrdersInfo, buf)
		time.Sleep(time.Second)
	}

	{
		status := OrderStatus_OrderStatus_Filled
		req := ReqOrdersInfo{Account: &account, ReqStatus: &status}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqOrdersInfo, buf)
		time.Sleep(time.Second)
	}

	{
		req := ReqPosition{TickerName: &ticker, Account: &account}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqPosition, buf)
		time.Sleep(time.Second)
	}

	{
		req := ReqPosition{Account: &account}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqPosition, buf)
		time.Sleep(time.Second)
	}

	{
		req := ReqAccount{Account: &account}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqAccount, buf)
		time.Sleep(time.Second)
	}

	{
		req := ReqCancelOrder{TickerName: &ticker, Account: &account}
		buf, _ := proto.Marshal(&req)
		SendTo(conn, sender, recver, 0, ConstDefine_E_ReqCancelOrder, buf)
		time.Sleep(time.Second)
	}
}

func TestAlgoOrder(ticker, account, strategy, exchange, quote, algo string) []byte {
	rso := AlgoInfo{}
	rso.TickerName = &ticker
	rso.Account = &account
	rso.StrategyName = &strategy
	rso.ExchangeName = &exchange
	rso.QuoteName = &quote
	rso.AlgoName = &algo
	rso.LongVolume = new(uint64)
	*rso.LongVolume = uint64(0)
	rso.MaxLongPrice = new(float64)
	*rso.MaxLongPrice = 9999
	rso.ShortVolume = new(uint64)
	*rso.ShortVolume = uint64(rand.Int63n(10))
	rso.MinShortPrice = new(float64)
	*rso.MinShortPrice = 999

	if buf, err := proto.Marshal(&ReqAlgoOrder{Algo: &rso}); err == nil {
		return buf
	}

	return nil
}

func Test() {
	conn, err := net.Dial("tcp", *flgMc)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	buf := TestAlgoOrder(*flgTicker, *flgAccount, *flgName, *flgTrade, *flgQuote, "MARKET")
	SendTo(conn, MakeID(*flgName), MakeID(*flgSplit), 0, ConstDefine_E_ReqAlgoOrder, buf)
}

func main() {
	flag.Parse()
	flag.Usage()

	conn, err := net.Dial("tcp", *flgMc)
	if err != nil {
		fmt.Println(err)
		return
	}

	go Register(conn, *flgName)
	go Recv(conn)
	go Register(conn, *flgQuote)

	for {
		time.Sleep(time.Second)
		Test()
		time.Sleep(time.Second)
		Request(conn, "IF", *flgAccount)
		time.Sleep(time.Second)
		Request(conn, *flgTicker, *flgAccount)
		time.Sleep(time.Second)
	}

	select {}
}
