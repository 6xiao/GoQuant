package main

import (
	"code.google.com/p/goprotobuf/proto"
	"flag"
	"fmt"
	. "github.com/6xiao/GoQuant/DataType"
	"github.com/6xiao/go/Common"
	"io"
	"net"
	"time"
	"unsafe"
)

var (
	flgName      = flag.String("name", "VE", "module name")
	flgQuote     = flag.String("quote", "QUOTEve", "QuoteChannelName")
	flgMc        = flag.String("mc", "192.168.3.13:5381", "message channel ip:port")
	flgReqTick   = flag.String("reqtick", "IF0000", "test request tick")
	flgStartDate = flag.Int("startdate", 0, "the date of start load data")
	flgStopDate  = flag.Int("overdate", 20100430, "the date of end")
	flgMargin    = flag.Float64("margin", 0.08, "ticker margin")
	flgCast      = flag.Float64("cast", 0.00003, "ticker trading cast")
	flgInitEqu   = flag.Float64("initequ", 10000000, "init equlity")
)

const (
	buflen = 256
	back   = 16
)

var (
	sendChan         = make(chan []byte)
	orderChan        = make(chan *OrderReq)
	cancelChan       = make(chan *CancelReq)
	quoteChan        = make(chan *DepthMarketData)
	reqtickChan      = make(chan *DepthMarketData, 1)
	reqklineChan     = make(chan *RspKLine, 1)
	tickBuffer       = make(chan []byte, 1024*1024)
	deals            = []DealInfo{}
	days             = 0
	ticks            = 0
	cancelOpenTimes  = 0
	cancelCloseTimes = 0
	openTimes        = 0
	closeTimes       = 0
)

type OrderReq struct {
	sender uint64
	tm     uint64
	id     string
	status OrderStatus
	order  *ReqSendOrder
}

type CancelReq struct {
	sender uint64
	tm     uint64
	cancel *ReqCancelOrder
}

type DealInfo struct {
	isopen bool
	ticker string
	time   uint64
	volume int
	price  float64
	cast   float64
}

func makeOrderInfo(or *OrderReq, exp string) *OrderInfo {
	rso := or.order
	today := true
	return &OrderInfo{TickerName: rso.TickerName, Account: rso.Account, Time: &or.tm,
		Explain: &exp, IsForce: new(bool), IsToday: &today, Volume: rso.Volume,
		Price: rso.Price, Direct: rso.Direct, OpenClose: rso.OpenClose, OrderID: &or.id, Status: &or.status}
}

func Send(conn net.Conn) {
	for {
		buf := <-sendChan
		if buf == nil {
			conn.Close()
			return
		}
		conn.Write(buf)
	}
}

func SendTo(recver uint64, copyer uint64, tp ConstDefine, buf []byte) {
	sockbuf := make([]byte, MsgLen+RecLen+uint32(len(buf)))
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	rh := (*RecordHead)(unsafe.Pointer(&sockbuf[MsgLen]))
	bodybuf := sockbuf[MsgLen+RecLen:]
	mh.Msglen = uint64(len(sockbuf)) - HeadLen
	mh.Sender = MakeID(*flgName)
	mh.Recver = recver
	mh.Copyer = copyer
	rh.HashCode = Common.Hash(buf)
	rh.RecType = uint32(tp)
	rh.Length = uint32(len(buf))
	copy(bodybuf, buf)
	sendChan <- sockbuf
}

func Recv(conn net.Conn) {
	defer func() { sendChan <- nil }()

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
		//fmt.Println("msgSender -> Recver:", mh.Sender, mh.Recver)
		if uint32(len(msgbuf)) < MsgLen+RecLen {
			continue
		}

		rh := (*RecordHead)(unsafe.Pointer(&msgbuf[MsgLen]))

		if uint32(len(msgbuf)) < MsgLen+RecLen+rh.Length {
			continue
		}

		bodybuf := msgbuf[MsgLen+RecLen : MsgLen+RecLen+rh.Length]
		if rh.HashCode != Common.Hash(bodybuf) {
			continue
		}

		switch rh.RecType {
		case uint32(ConstDefine_E_DepthMarketData):
			dmd := &DepthMarketData{}
			proto.Unmarshal(bodybuf, dmd)
			quoteChan <- dmd

		case uint32(ConstDefine_E_ReqCancelOrder):
			co := &ReqCancelOrder{}
			proto.Unmarshal(bodybuf, co)
			cancelChan <- &CancelReq{mh.Sender, mh.SendTime, co}

		case uint32(ConstDefine_E_ReqOrdersInfo):
			buf, _ := proto.Marshal(&RspOrdersInfo{})
			SendTo(mh.Sender, 0, ConstDefine_E_RspOrdersInfo, buf)

		case uint32(ConstDefine_E_ReqPosition):
			buf, _ := proto.Marshal(&RspPosition{})
			SendTo(mh.Sender, 0, ConstDefine_E_RspPosition, buf)

		case uint32(ConstDefine_E_ReqAccount):
			buf, _ := proto.Marshal(&RspAccount{})
			SendTo(mh.Sender, 0, ConstDefine_E_RspAccount, buf)

		case uint32(ConstDefine_E_ReqSendOrder):
			so := &ReqSendOrder{}
			proto.Unmarshal(bodybuf, so)
			fmt.Println("order...")
			orderChan <- &OrderReq{mh.Sender, 0, "", OrderStatus_OrderStatus_NIL, so}

		case uint32(ConstDefine_E_HeartBeat):
			if len(reqtickChan) > 0 {
				quoteChan <- <-reqtickChan
			}
			if len(reqklineChan) > 0 {
				rk := <-reqklineChan
				buf, _ := proto.Marshal(rk)
				SendTo(MakeID(*flgQuote), 0, ConstDefine_E_RspKLine, buf)
			}

		case uint32(ConstDefine_E_RspKLine):
			go func() {
				rk := &RspKLine{}
				proto.Unmarshal(bodybuf, rk)
				klines := rk.GetBar_Minute()
				if nil == klines {
					return
				}
				fmt.Println(time.Now(), " GetKLine:", len(klines))
				for _, kl := range klines {
					rk.Bar_Minute = []*TOHLCV{kl}
					reqklineChan <- rk
				}
			}()

		case uint32(ConstDefine_E_RspTick):
			tickBuffer <- bodybuf
		}
	}
}

type PosDirect struct {
	ValidVolume   uint64
	ClosingVolume uint64
	QueuingVolume uint64
	AvgPrice      float64
}

func NewPosDirect() *PosDirect {
	return &PosDirect{0, 0, 0, 0.0}
}

func (this *PosDirect) PreOpen(volume uint64) {
	openTimes++
	this.QueuingVolume += volume
}

func (this *PosDirect) Open(volume uint64, price float64) bool {
	if volume > this.QueuingVolume {
		fmt.Println("error:volume > queuingvolume")
		return false
	}

	cp := float64(this.ValidVolume+this.ClosingVolume)*this.AvgPrice + float64(volume)*price
	this.QueuingVolume -= volume
	this.ValidVolume += volume
	this.AvgPrice = cp / float64(this.ValidVolume+this.ClosingVolume)
	return true
}

func (this *PosDirect) CancelOpen(volume uint64) bool {
	cancelOpenTimes++
	if this.QueuingVolume >= volume {
		this.QueuingVolume -= volume
		return true
	}
	return false
}

func (this *PosDirect) CancelClose(volume uint64) bool {
	cancelCloseTimes++
	if this.ClosingVolume >= volume {
		this.ClosingVolume -= volume
		this.ValidVolume += volume
		return true
	}
	return false
}

func (this *PosDirect) PreClose(volume uint64) bool {
	closeTimes++
	if this.ValidVolume >= volume {
		this.ValidVolume -= volume
		this.ClosingVolume += volume
		return true
	}
	return false
}

func (this *PosDirect) Close(volume uint64, price float64) float64 {
	avgprice := this.AvgPrice

	cp := float64(this.ValidVolume+this.ClosingVolume)*this.AvgPrice - float64(volume)*price
	this.ClosingVolume -= volume
	if this.ValidVolume+this.ClosingVolume == 0 {
		this.AvgPrice = 0.0
	} else {
		this.AvgPrice = cp / float64(this.ValidVolume+this.ClosingVolume)
	}

	return avgprice
}

type PosTicker struct {
	long  *PosDirect
	short *PosDirect
}

func NewPosTicker(ticker string) *PosTicker {
	return &PosTicker{NewPosDirect(), NewPosDirect()}
}

func (this *PosTicker) GetPosition() *PositionInfo {
	return &PositionInfo{LongAvgPrice: &this.long.AvgPrice, ValidLongVolume: &this.long.ValidVolume,
		ClosingLongVolume: &this.long.ClosingVolume, QueuingLongVolume: &this.long.QueuingVolume,
		ShortAvgPrice: &this.short.AvgPrice, ValidShortVolume: &this.short.ValidVolume,
		ClosingShortVolume: &this.short.ClosingVolume, QueuingShortVolume: &this.short.QueuingVolume}
}

func (this *PosTicker) SumPosition() uint64 {
	return this.long.ValidVolume + this.long.ClosingVolume + this.long.QueuingVolume +
		this.short.ValidVolume + this.short.ClosingVolume + this.short.QueuingVolume
}

func Println(a ...interface{}) {
	fmt.Println(a...)
}

func (this *PosTicker) PreSellOpen(volume uint64, price float64) {
	Println("开空委托：", volume, "张", price)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	this.short.PreOpen(volume)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
}

func (this *PosTicker) SellOpen(volume uint64, price float64) bool {
	Println("开空成交:", volume, price)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	suc := this.short.Open(volume, price)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	Println("开空成交:", suc, price)
	return suc
}

func (this *PosTicker) PreBuyOpen(volume uint64, price float64) {
	Println("开多委托：", volume, "张", price)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	this.long.PreOpen(volume)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
}

func (this *PosTicker) BuyOpen(volume uint64, price float64) bool {
	Println("开多成交:", volume, price)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	suc := this.long.Open(volume, price)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	Println("开多成交:", suc, price)
	return suc
}

func (this *PosTicker) PreSellClose(volume uint64, price float64) bool {
	Println("平多委托：", volume, "张", price)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	suc := this.long.PreClose(volume)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	Println("平多委托：", suc, price)
	return suc
}

func (this *PosTicker) SellClose(volume uint64, price float64) float64 {
	Println("平多成交：", volume, price)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	avp := this.long.Close(volume, price)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	if this.long.AvgPrice < 0.1 {
		Println("")
	}
	return price - avp
}

func (this *PosTicker) PreBuyClose(volume uint64, price float64) bool {
	Println("平空委托：", volume, "张", price)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	suc := this.short.PreClose(volume)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	Println("平空委托：", suc, price)
	return suc
}

func (this *PosTicker) BuyClose(volume uint64, price float64) float64 {
	Println("平空成交：", volume, price)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	avp := this.short.Close(volume, price)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	if this.short.AvgPrice < 0.1 {
		Println("")
	}
	return avp - price
}

func (this *PosTicker) CancelBuyOpen(volume uint64) bool {
	Println("撤开多单：", volume, "张")
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	suc := this.long.CancelOpen(volume)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	Println("开多撤单：", suc)
	return suc
}

func (this *PosTicker) CancelSellOpen(volume uint64) bool {
	Println("撤开空单：", volume, "张")
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	suc := this.short.CancelOpen(volume)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	Println("撤开空单：", suc)
	return suc
}

func (this *PosTicker) CancelBuyClose(volume uint64) bool {
	Println("撤平空单：", volume, "张")
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	suc := this.short.CancelClose(volume)
	Println("空仓待成交、持仓、待平、均价：",
		this.short.QueuingVolume, this.short.ValidVolume, this.short.ClosingVolume, this.short.AvgPrice)
	Println("撤平空单：", suc)
	return suc
}

func (this *PosTicker) CancelSellClose(volume uint64) bool {
	Println("撤平多单：", volume, "张")
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	suc := this.long.CancelClose(volume)
	Println("多仓待成交、持仓、待平、均价：",
		this.long.QueuingVolume, this.long.ValidVolume, this.long.ClosingVolume, this.long.AvgPrice)
	Println("撤平多单：", suc)
	return suc
}

type InfoAccount struct {
	money  float64
	report map[string]*OrderInfo
	order  map[string]*OrderReq
	pos    map[string]*PosTicker
}

func NewInfoAccount() *InfoAccount {
	return &InfoAccount{*flgInitEqu, make(map[string]*OrderInfo), make(map[string]*OrderReq), make(map[string]*PosTicker)}
}

func (this *InfoAccount) GetReport(orderid string) *OrderInfo {
	if v, ok := this.report[orderid]; ok {
		return v
	}
	return nil
}

func (this *InfoAccount) MoneyEnough(volume uint64, price float64) bool {
	var pos uint64
	for _, pt := range this.pos {
		pos += pt.SumPosition()
	}

	return this.money-(float64(pos+volume)*300*price*(*flgMargin)) > 0
}

func (this *InfoAccount) GetCancelableReports(tm uint64) []*OrderInfo {
	ords := []*OrderInfo{}
	day := tm / 1000000000

	for k, rpt := range this.report {
		if rpt.GetTime()/1000000000 < day {
			defer delete(this.report, k)
		}
		if rpt.GetStatus() == OrderStatus_OrderStatus_New ||
			rpt.GetStatus() == OrderStatus_OrderStatus_PartFill {
			ords = append(ords, rpt)
		}
	}
	return ords
}

func (this *InfoAccount) Order(lastprice float64, or *OrderReq) {
	volume := or.order.GetVolume()
	ticker := or.order.GetTickerName()
	pos := this.GetPos(ticker)

	switch or.order.GetOpenClose() {
	case OrderOpenClose_OrderOpenClose_Close:
		switch or.order.GetDirect() {
		case OrderDirect_OrderDirect_Sell:
			if pos.PreSellClose(volume, lastprice) {
				or.status = OrderStatus_OrderStatus_New
				this.order[or.id] = or
				this.report[or.id] = makeOrderInfo(or, "new pre-sell-close")
			} else {
				or.status = OrderStatus_OrderStatus_Rejected
				this.report[or.id] = makeOrderInfo(or, "err: can't sell-close position")
			}
		case OrderDirect_OrderDirect_Buy:
			if pos.PreBuyClose(volume, lastprice) {
				or.status = OrderStatus_OrderStatus_New
				this.order[or.id] = or
				this.report[or.id] = makeOrderInfo(or, "new pre-buy-close")
			} else {
				or.status = OrderStatus_OrderStatus_Rejected
				this.report[or.id] = makeOrderInfo(or, "err: can't buy-close position")
			}
		default:
			or.status = OrderStatus_OrderStatus_Rejected
			this.report[or.id] = makeOrderInfo(or, "Unknown close-direct")
		}

	case OrderOpenClose_OrderOpenClose_Open:
		if !this.MoneyEnough(volume, lastprice) {
			or.status = OrderStatus_OrderStatus_Rejected
			this.report[or.id] = makeOrderInfo(or, "margin great then money")
			return
		}

		switch or.order.GetDirect() {
		case OrderDirect_OrderDirect_Sell:
			pos.PreSellOpen(volume, lastprice)
			or.status = OrderStatus_OrderStatus_New
			this.order[or.id] = or
			this.report[or.id] = makeOrderInfo(or, "new pre-sell-open")

		case OrderDirect_OrderDirect_Buy:
			pos.PreBuyOpen(volume, lastprice)
			or.status = OrderStatus_OrderStatus_New
			this.order[or.id] = or
			this.report[or.id] = makeOrderInfo(or, "new pre-buy-open")
		default:
			or.status = OrderStatus_OrderStatus_Rejected
			this.report[or.id] = makeOrderInfo(or, "Unknown open-direct")
		}
	default:
		or.status = OrderStatus_OrderStatus_Rejected
		this.report[or.id] = makeOrderInfo(or, "Unknown openclose")
	}
}

func (this *InfoAccount) QuoteMatch(tm uint64, price float64, ticker string) {
	day := tm / 1000000000
	for ord, req := range this.order {
		if req.status != OrderStatus_OrderStatus_New &&
			req.status != OrderStatus_OrderStatus_PartFill {
			continue
		}

		if req.order.GetTickerName() != ticker {
			continue
		}

		if req.tm/1000000000 < day {
			defer delete(this.order, ord)
			continue
		}

		direct := req.order.GetDirect()
		if (direct == OrderDirect_OrderDirect_Sell && price < req.order.GetPrice()) ||
			(direct == OrderDirect_OrderDirect_Buy && price > req.order.GetPrice()) {
			continue
		}

		volume := req.order.GetVolume()
		openclose := req.order.GetOpenClose()
		pos := this.GetPos(ticker)
		ois := OrderStatus_OrderStatus_Filled
		exp := "filled"
		rpt := this.report[ord]

		cast := *flgCast * price * 300
		switch openclose {
		case OrderOpenClose_OrderOpenClose_Open:
			switch direct {
			case OrderDirect_OrderDirect_Sell:
				if !pos.SellOpen(volume, price) {
					fmt.Println(ord, req.tm, "error:can't sell-open")
					continue
				}
				deals = append(deals, DealInfo{true, ticker, tm, -1 * int(volume), price, cast})
			case OrderDirect_OrderDirect_Buy:
				if !pos.BuyOpen(volume, price) {
					fmt.Println(ord, req.tm, "error:can't buy-open")
					continue
				}
				deals = append(deals, DealInfo{true, ticker, tm, int(volume), price, cast})
			}

		case OrderOpenClose_OrderOpenClose_Close:
			switch direct {
			case OrderDirect_OrderDirect_Sell:
				pos.SellClose(volume, price)
				deals = append(deals, DealInfo{false, ticker, tm, -1 * int(volume), price, cast})
			case OrderDirect_OrderDirect_Buy:
				pos.BuyClose(volume, price)
				deals = append(deals, DealInfo{false, ticker, tm, int(volume), price, cast})
			}
		}
		this.money -= cast // cast

		req.status = ois
		rpt.Status = &req.status
		rpt.Explain = &exp
		rpt.Price = &price
		rpt.LastFilled = req.order.Volume
		if rpt.TotalFilled == nil {
			rpt.TotalFilled = new(uint64)
		}
		*rpt.TotalFilled += *rpt.LastFilled
		rpt.LastMatchTime = &tm
		rso := &RspSendOrder{LastReport: rpt}
		if buf, err := proto.Marshal(rso); err == nil {
			SendTo(req.sender, MakeID("TRADE"), ConstDefine_E_RspSendOrder, buf)
			SendTo(MakeID("REPORT"), MakeID("VIEW"), ConstDefine_E_RspSendOrder, buf)
		}
	}
}

func (this *InfoAccount) Cancel(tm uint64, rco *ReqCancelOrder) (bool, string) {
	orderid := rco.GetOrderID()
	odr, ok := this.order[orderid]
	rpt, ok2 := this.report[orderid]
	if !ok || !ok2 {
		return false, "error:can't find orderid"
	}
	if odr.order.GetTickerName() != rco.GetTickerName() {
		return false, "error:tickname != tickname"
	}
	if odr.status == OrderStatus_OrderStatus_Canceled {
		return false, "error:don't cancel again"
	}
	if odr.status == OrderStatus_OrderStatus_Filled {
		return false, "error:can't cancel filled order"
	}
	if tm/1000000000 != odr.tm/1000000000 {
		return false, "error:outdate order"
	}

	pos := this.GetPos(odr.order.GetTickerName())
	switch odr.order.GetDirect() {
	case OrderDirect_OrderDirect_Sell:
		switch odr.order.GetOpenClose() {
		case OrderOpenClose_OrderOpenClose_Open:
			if !pos.CancelSellOpen(pos.short.QueuingVolume) {
				return false, "error:cancel-sell-open"
			}
		case OrderOpenClose_OrderOpenClose_Close:
			if !pos.CancelSellClose(pos.long.ClosingVolume) {
				return false, "error:cancel-sell-close"
			}
		default:
			return false, "error:unknown open-close"
		}

	case OrderDirect_OrderDirect_Buy:
		switch odr.order.GetOpenClose() {
		case OrderOpenClose_OrderOpenClose_Open:
			if !pos.CancelBuyOpen(pos.long.QueuingVolume) {
				return false, "error:cancel-buy-open"
			}
		case OrderOpenClose_OrderOpenClose_Close:
			if !pos.CancelBuyClose(pos.short.ClosingVolume) {
				return false, "error:cancel-buy-close"
			}
		default:
			return false, "error:unknown open-close"
		}

	default:
		return false, "error:unknown direct"
	}

	if odr.status == OrderStatus_OrderStatus_PartFill {
		odr.status = OrderStatus_OrderStatus_PartFillPartCancel
		rpt.Status = &odr.status
		return true, "PartFillPartCancel"
	}

	if odr.status == OrderStatus_OrderStatus_New {
		odr.status = OrderStatus_OrderStatus_Canceled
		rpt.Status = &odr.status
		return true, "Canceled"
	}

	return false, "unknown cancel command"
}

func (this *InfoAccount) GetPos(ticker string) *PosTicker {
	if v, ok := this.pos[ticker]; ok {
		return v
	}

	v := NewPosTicker(ticker)
	this.pos[ticker] = v
	return v
}

type AccountDepot struct {
	account map[string]*InfoAccount
}

func NewAccountDepot() *AccountDepot {
	return &AccountDepot{make(map[string]*InfoAccount)}
}

func (this *AccountDepot) QuoteMatch(tm uint64, price float64, ticker string) {
	for _, v := range this.account {
		v.QuoteMatch(tm, price, ticker)
	}
}

func (this *AccountDepot) GetCancelableReports(tm uint64) []*OrderInfo {
	ords := []*OrderInfo{}
	for _, v := range this.account {
		ords = append(ords, v.GetCancelableReports(tm)...)
	}

	return ords
}

func (this *AccountDepot) GetReport(account, orderid string) *OrderInfo {
	info := this.GetAccountInfo(account)
	return info.GetReport(orderid)
}

func (this *AccountDepot) AddNewOrder(lastprice float64, or *OrderReq) {
	account := or.order.GetAccount()
	info := this.GetAccountInfo(account)
	info.Order(lastprice, or)
}

func (this *AccountDepot) CancelOrder(tm uint64, rco *ReqCancelOrder) (bool, string) {
	account := rco.GetAccount()
	info := this.GetAccountInfo(account)
	return info.Cancel(tm, rco)
}

func (this *AccountDepot) GetAccountInfo(account string) *InfoAccount {
	if v, ok := this.account[account]; ok {
		return v
	}

	v := NewInfoAccount()
	this.account[account] = v
	return v
}

func OrderProc() {
	orderid := 1
	pos := NewAccountDepot()
	var lastprice float64
	lasttime := Common.NumberNow()
	lastday := lasttime

	for {
		ois := OrderStatus_OrderStatus_Rejected

		select {
		case q := <-quoteChan:
			ticks++
			ticker := q.GetTickerName()
			lastprice = q.GetLastPrice()
			lasttime = q.GetTime()
			day := lasttime / 1000000000
			if day != lastday {
				days++
				for acc, accInfo := range pos.account {
					if tp := accInfo.GetPos(ticker).GetPosition(); tp != nil {
						if tp.GetLongAvgPrice() > 0.1 || tp.GetShortAvgPrice() > 0.1 {
							Println(lastday, acc, "当日有未平仓位", tp.GetLongAvgPrice(), tp.GetShortAvgPrice())
							Println("多仓持仓、待平：", tp.GetValidLongVolume(), tp.GetClosingLongVolume())
							Println("空仓持仓、待平：", tp.GetValidShortVolume(), tp.GetClosingShortVolume())
							time.Sleep(time.Hour * 100)
						}
					}
				}
				fmt.Println("\r\nNewDay:", day, "\r\n")
				lastday = day
			}

			pos.QuoteMatch(lasttime, lastprice, ticker)
			q.Order = append(q.Order, pos.GetCancelableReports(lasttime)...)

			for account, accInfo := range pos.account {
				if tp := accInfo.GetPos(ticker).GetPosition(); tp != nil {
					tp.TickerName = &ticker
					tp.Account = &account
					q.Position = append(q.Position, tp)
					if tp.GetQueuingLongVolume()+tp.GetClosingLongVolume() > 0 ||
						tp.GetQueuingShortVolume()+tp.GetClosingShortVolume() > 0 {
						if len(q.Order) == 0 {
							fmt.Println("missing order report")
							time.Sleep(time.Minute)
						}
					}
				}
			}

			if buf, err := proto.Marshal(q); err == nil {
				SendTo(MakeID(*flgQuote), 0, ConstDefine_E_DepthMarketData, buf)
			}

		case or := <-orderChan:
			or.status = ois
			or.tm = lasttime
			or.id = fmt.Sprint(orderid)
			orderid++

			pos.AddNewOrder(lastprice, or)
			oi := pos.GetReport(or.order.GetAccount(), or.id)
			if oi == nil {
				oi = makeOrderInfo(or, "error:Can't GetReportByOrderID")
			}

			rso := &RspSendOrder{LastReport: oi}
			if buf, err := proto.Marshal(rso); err == nil {
				SendTo(or.sender, MakeID("TRADE"), ConstDefine_E_RspSendOrder, buf)
				SendTo(MakeID("REPORT"), MakeID("VIEW"), ConstDefine_E_RspSendOrder, buf)
			}

		case c := <-cancelChan:
			ordid := c.cancel.GetOrderID()
			suc, exp := pos.CancelOrder(lasttime, c.cancel)
			oi := pos.GetReport(c.cancel.GetAccount(), ordid)
			if suc || oi == nil {
				oi = &OrderInfo{Explain: &exp, Status: &ois}
			}

			rco := &RspCancelOrder{LastReport: oi}
			if buf, err := proto.Marshal(rco); err == nil {
				SendTo(c.sender, MakeID("TRADE"), ConstDefine_E_RspCancelOrder, buf)
				SendTo(MakeID("REPORT"), MakeID("VIEW"), ConstDefine_E_RspCancelOrder, buf)
			}
		}
	}
}

func Calc() {
	var maxEqu, minEqu float64
	var sumWin, sumLos float64
	var equ, lstEqu, maxBack float64
	var winCount, losCount, openCount, closeCount int
	var alwaysWin, alwaysLos int
	var position, buyVol, sellVol int
	var onceMaxWin, onceMaxLos float64
	var cast float64
	fmt.Println("统计:")

	for _, deal := range deals {
		equ -= deal.price * float64(deal.volume)
		position += deal.volume
		cast += deal.cast

		if deal.isopen {
			openCount++
		} else {
			closeCount++
		}

		if deal.volume > 0 {
			buyVol += deal.volume
		} else {
			sellVol -= deal.volume
		}

		if position == 0 {
			if equ > maxEqu {
				maxEqu = equ
			}
			if equ < minEqu {
				minEqu = equ
			}
			if maxEqu-equ > maxBack {
				maxBack = maxEqu - equ
				fmt.Println("\r\n更新最大回撤:", Roll(maxBack), "@", deal.time, Roll((maxBack*30000)/(*flgInitEqu+maxEqu*300)), "%")
			}

			if lstEqu > equ {
				t := lstEqu - equ
				if onceMaxLos < t {
					onceMaxLos = t
				}
				losCount++
				alwaysLos++
				alwaysWin = 0
				sumLos += t
			} else if lstEqu < equ {
				t := equ - lstEqu
				if onceMaxWin < t {
					onceMaxWin = t
				}
				winCount++
				alwaysWin++
				alwaysLos = 0
				sumWin += t
			}
			lstEqu = equ

			if alwaysLos > 0 {
				fmt.Println(deal.time, deal.price, deal.volume, "连续亏损：", alwaysLos)
			}
			if alwaysWin > 0 {
				fmt.Println(deal.time, deal.price, deal.volume, "连续盈利：", alwaysWin)
			}
		} else {
			fmt.Println(deal.time, deal.price, deal.volume)
		}
	}
	if sellVol != buyVol {
		equ -= float64(sellVol-buyVol) * deals[len(deals)-1].price
		fmt.Println("统计有未平仓位")
	}

	fmt.Println("")
	fmt.Println("使用的合约:", *flgReqTick, "通道名称:", *flgName)
	fmt.Println("开始时间:", *flgStartDate, "结束时间:", *flgStopDate)
	fmt.Println("测试天数:", days, "测试tick数:", ticks, "\r\n")

	fmt.Println("总买量:", buyVol, "总卖量:", sellVol)
	fmt.Println("开仓次数:", openCount, "平仓次数:", closeCount)
	fmt.Println("开仓单:", openTimes, "平仓单:", closeTimes)
	fmt.Println("撤开仓:", cancelOpenTimes, "撤平仓:", cancelCloseTimes, "\r\n")

	fmt.Println("平均每张合约收益:", Roll(equ/float64(openCount)))
	fmt.Println("单次最大盈(点):", Roll(onceMaxWin), "单次最大亏(点):", Roll(onceMaxLos), "\r\n")

	fmt.Println("最大收益(点):", Roll(maxEqu), "最小收益(点):", Roll(minEqu))
	if winCount+losCount > 0 {
		fmt.Println("最大回撤(点):", Roll(maxBack))
		fmt.Println("计盈次数：", winCount, "计亏次数:", losCount, "持平:", openCount-winCount-losCount)
		fmt.Println("盈利汇总(点):", Roll(sumWin), "亏损汇总(点):", Roll(sumLos))
		fmt.Println("单次平均盈(点):", Roll(sumWin/float64(winCount)), "单次平均亏(点):", Roll(sumLos/float64(losCount)))
		fmt.Println("胜率:", 100*winCount/(winCount+losCount), "%")
	}
	fmt.Println("")
	fmt.Println("最大权益:", Roll(*flgInitEqu+maxEqu*300), "最小权益:", Roll(*flgInitEqu+minEqu*300))
	fmt.Println("毛收益:", Roll(equ*300), "斩获点数:", Roll(equ))
	fmt.Println("手续费成本:", Roll(cast), "元,折合", Roll(cast/300), "点")
	fmt.Println("平均手续费:", Roll(cast/float64(buyVol+sellVol)), "元")
	fmt.Println("净收益:", Roll(equ*300-cast))
	fmt.Println("初始权益:", Roll(*flgInitEqu), "期末权益(扣减佣金):", Roll(*flgInitEqu+equ*300-cast))
}

func PlaybackTick() {
	for i := 0; true; i++ {
		select {
		case tb := <-tickBuffer:
			rt := &RspTick{}
			proto.Unmarshal(tb, rt)
			ticks := rt.GetTicks()
			if nil != ticks {
				for _, tk := range ticks {
					reqtickChan <- tk
				}
			}

			if len(ticks) < 65536 {
				Calc()
			}
		}
	}
}

func ReqQuote() {
	beg := uint64(*flgStartDate) * 1000000000
	end := uint64(*flgStopDate) * 1000000000
	if len(*flgReqTick) > 4 {
		rt := ReqTick{TickerName: flgReqTick, StartTime: &beg, EndTime: &end}
		buffer, _ := proto.Marshal(&rt)
		SendTo(MakeID("HD"), 0, ConstDefine_E_ReqTick, buffer)
		fmt.Println(*flgReqTick, "ReqTick:", beg, "to", end)
	}
}

func Register(conn net.Conn) {
	sockbuf := make([]byte, 1024)
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	mh.Msglen = uint64(MsgLen) - HeadLen
	mh.Sender = MakeID(*flgName)
	fmt.Println("Register:", *flgName, mh.Sender)
	sendChan <- sockbuf[:MsgLen]
}

func main() {
	flag.Parse()
	flag.Usage()

	conn, err := net.Dial("tcp", *flgMc)
	if err != nil {
		fmt.Println(err)
		return
	}

	go OrderProc()
	go Send(conn)
	go Register(conn)
	go Recv(conn)
	go ReqQuote()
	go PlaybackTick()

	select {}
}
