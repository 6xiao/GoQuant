package main

import (
	"code.google.com/p/goprotobuf/proto"
	"flag"
	"fmt"
	. "github.com/6xiao/GoQuant/DataType"
	"github.com/6xiao/go/Common"
	"io"
	"log"
	"math/rand"
	"net"
	"time"
	"unsafe"
)

var (
	flgName = flag.String("name", "OS", "module name")
	flgMc   = flag.String("mc", "192.168.3.13:5381", "message channel ip:port")
	flgTest = flag.Bool("test", false, "is test? use ve?")
	algos   = make(map[string]*AlgoInfo)
)

const (
	buflen      = 256
	back        = 16
	MODE_FAST   = "FAST"
	MODE_MARKET = "MARKET"
)

func Register(conn net.Conn, name string) {
	sockbuf := make([]byte, 1024)
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	mh.Msglen = uint64(MsgLen) - HeadLen
	mh.Sender = MakeID(name)
	log.Println("Register:", name, mh.Sender)
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

func SendOrder(ticker, account string, buy, open bool, price, lastprice float64, volume uint64) []byte {
	dir := OrderDirect_OrderDirect_Sell
	if buy {
		dir = OrderDirect_OrderDirect_Buy
	}

	oc := OrderOpenClose_OrderOpenClose_Close
	if open {
		oc = OrderOpenClose_OrderOpenClose_Open
	}

	rso := ReqSendOrder{}
	rso.TickerName = &ticker
	rso.Account = &account
	rso.Direct = &dir
	rso.OpenClose = &oc
	rso.Price = &price
	rso.LastPrice = &price
	rso.Volume = &volume

	log.Println("SendOrder:", ticker, account, "buy:", buy, "open:", open, price, volume)

	if buf, err := proto.Marshal(&rso); err == nil {
		return buf
	} else {
		log.Println("error:", err)
	}

	return nil
}

func CancelOrder(ticker, account, order_id string) []byte {
	rco := ReqCancelOrder{}
	rco.TickerName = &ticker
	rco.Account = &account
	rco.OrderID = &order_id

	log.Println("CancelOrder:", ticker, account, order_id)

	if buf, err := proto.Marshal(&rco); err == nil {
		return buf
	} else {
		log.Println("error:", err)
	}

	return nil
}

func Recv(conn net.Conn) {
	for {
		nethead := [HeadLen]byte{}
		if _, e := io.ReadFull(conn, nethead[:]); e != nil {
			log.Println(conn.RemoteAddr().String(), "error recv head : ", e)
			break
		}

		msglen := *(*uint64)(unsafe.Pointer(&nethead))
		msgbuf := make([]byte, HeadLen+msglen)

		if _, e := io.ReadFull(conn, msgbuf[HeadLen:]); e != nil {
			log.Println(conn.RemoteAddr().String(), "error recv body : ", e)
			break
		}

		mh := (*MessageHead)(unsafe.Pointer(&msgbuf[0]))
		mh.Msglen = msglen
		if uint32(len(msgbuf)) < MsgLen+RecLen {
			log.Println(conn.RemoteAddr().String(), "error recv half-head")
			continue
		}

		rh := (*RecordHead)(unsafe.Pointer(&msgbuf[MsgLen]))

		if uint32(len(msgbuf)) < MsgLen+RecLen+rh.Length {
			log.Println(conn.RemoteAddr().String(), "error recv half-body")
			continue
		}

		bodybuf := msgbuf[MsgLen+RecLen : MsgLen+RecLen+rh.Length]
		if rh.HashCode != Common.Hash(bodybuf) {
			log.Println(conn.RemoteAddr().String(), "error hash")
			continue
		}

		switch rh.RecType {
		default:
			log.Println("recv other msg", mh.Sender)

		case uint32(ConstDefine_E_DepthMarketData):
			dmd := &DepthMarketData{}
			err := proto.Unmarshal(bodybuf, dmd)
			if err == nil {
				fmt.Println(dmd.GetTime(), dmd.GetTickerName(), dmd.GetVolume(), dmd.GetLastPrice())
			}

		case uint32(ConstDefine_E_ReqAlgoOrder):
			ao := &ReqAlgoOrder{}
			err := proto.Unmarshal(bodybuf, ao)
			if err != nil {
				log.Println("error:", err)
				break
			}

			if ai := ao.GetAlgo(); ai != nil {
				tk := ai.GetTickerName()
				st := ai.GetStrategyName()
				ac := ai.GetAccount()
				ex := ai.GetExchangeName()
				qt := ai.GetQuoteName()
				an := ai.GetAlgoName()
				lv := ai.GetLongVolume()
				sv := ai.GetShortVolume()
				log.Println("AlgoOrder:", tk, st, ac, ex, qt, an, lv, sv)

				if MakeID(st) != mh.Sender {
					log.Println("error: ", st, "not assign sender ", mh.Sender)
					break
				}

				key := fmt.Sprint(tk, "_", st, "_", ac, "_", ex, "_", qt, "_", an)
				if v, ok := algos[key]; ok {
					*v = *ai
				} else if lv+sv > 0 {
					algos[key] = ai
					switch ai.GetAlgoName() {
					case MODE_FAST:
						go Spliter(key, MODE_FAST)

					case MODE_MARKET:
						go Spliter(key, MODE_MARKET)
					}
				}

				rao := &RspAlgoOrder{Algo: ai}
				if buf, err := proto.Marshal(rao); err == nil {
					SendTo(conn, mh.Recver, mh.Sender, mh.Copyer, ConstDefine_E_RspSendOrder, buf)
				} else {
					log.Panicln("error:", err)
				}
			}
		}
	}
}

func GetOrders(ticker, account string, dmd *DepthMarketData) ([]*OrderInfo, []*OrderInfo, []*OrderInfo, []*OrderInfo) {
	buyOpen := []*OrderInfo{}
	buyClose := []*OrderInfo{}
	sellOpen := []*OrderInfo{}
	sellClose := []*OrderInfo{}

	order := dmd.GetOrder()
	for i, ord := range order {
		if ord.GetTickerName() != ticker || ord.GetAccount() != account {
			continue
		}

		switch ord.GetDirect() {
		case OrderDirect_OrderDirect_Buy:
			switch ord.GetOpenClose() {
			case OrderOpenClose_OrderOpenClose_Open:
				buyOpen = append(buyOpen, ord)

			case OrderOpenClose_OrderOpenClose_Close:
				buyClose = append(buyClose, ord)
			}

		case OrderDirect_OrderDirect_Sell:
			switch ord.GetOpenClose() {
			case OrderOpenClose_OrderOpenClose_Open:
				sellOpen = append(sellOpen, ord)

			case OrderOpenClose_OrderOpenClose_Close:
				sellClose = append(sellClose, ord)
			}
		}

		log.Println("orders:", i, ord.GetTime(), ord.GetAccount(),
			ord.GetTickerName(), ord.GetVolume(), ord.GetPrice(),
			ord.GetLastFilled(), ord.GetTotalFilled(), ord.GetExplain(),
			ord.GetOrderID(), ord.GetIsForce(), ord.GetIsToday())
		log.Println("length:", len(buyOpen), len(buyClose), len(sellOpen), len(sellClose))
	}

	return buyOpen, buyClose, sellOpen, sellClose
}

func GetPositionNumber(ticker, account string, dmd *DepthMarketData) (uint64, uint64, uint64, uint64, uint64, uint64) {
	longQueue := uint64(0)
	longPos := uint64(0)
	longFlat := uint64(0)
	shortQueue := uint64(0)
	shortPos := uint64(0)
	shortFlat := uint64(0)

	position := dmd.GetPosition()
	for _, pos := range position { // long position
		if pos.GetTickerName() != ticker || pos.GetAccount() != account {
			continue
		}

		longQueue = pos.GetQueuingLongVolume()
		longPos = pos.GetValidLongVolume()
		longFlat = pos.GetClosingLongVolume()
		shortQueue = pos.GetQueuingShortVolume()
		shortPos = pos.GetValidShortVolume()
		shortFlat = pos.GetClosingShortVolume()
	}

	return longQueue, longPos, longFlat, shortQueue, shortPos, shortFlat
}

func Spliter(key, mode string) {
	defer delete(algos, key)

	algo, ok := algos[key]
	if !ok {
		log.Println("Can't get algo from key:", key)
		return
	}

	log.Println("start routine by key:", key)

	ticker := algo.GetTickerName()
	account := algo.GetAccount()
	strategy := algo.GetStrategyName()
	exchange := algo.GetExchangeName()

	conn, err := net.Dial("tcp", *flgMc)
	if err != nil {
		log.Println("error:", err)
		return
	}
	defer conn.Close()

	Register(conn, algo.GetQuoteName())

	if *flgTest {
		hb := &HeartBeat{}
		if buf, err := proto.Marshal(hb); err == nil {
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_HeartBeat, buf)
		}
	}

	for {
		nethead := [HeadLen]byte{}
		if _, e := io.ReadFull(conn, nethead[:]); e != nil {
			log.Println(conn.RemoteAddr().String(), "error recv head : ", e)
			break
		}

		msglen := *(*uint64)(unsafe.Pointer(&nethead))
		msgbuf := make([]byte, HeadLen+msglen)

		if _, e := io.ReadFull(conn, msgbuf[HeadLen:]); e != nil {
			log.Println(conn.RemoteAddr().String(), "error recv body : ", e)
			break
		}

		mh := (*MessageHead)(unsafe.Pointer(&msgbuf[0]))
		mh.Msglen = msglen
		if uint32(len(msgbuf)) < MsgLen+RecLen {
			log.Println(conn.RemoteAddr().String(), "error recv half-head")
			continue
		}

		rh := (*RecordHead)(unsafe.Pointer(&msgbuf[MsgLen]))

		if uint32(len(msgbuf)) < MsgLen+RecLen+rh.Length {
			log.Println(conn.RemoteAddr().String(), "error recv half-body")
			continue
		}

		bodybuf := msgbuf[MsgLen+RecLen : MsgLen+RecLen+rh.Length]
		if rh.HashCode != Common.Hash(bodybuf) {
			log.Println(conn.RemoteAddr().String(), "error hash")
			continue
		}

		if rh.RecType != uint32(ConstDefine_E_DepthMarketData) {
			continue
		}

		dmd := &DepthMarketData{}
		err := proto.Unmarshal(bodybuf, dmd)
		if err != nil || dmd.GetTickerName() != ticker {
			continue
		}

		buyOpen, buyClose, sellOpen, sellClose := GetOrders(ticker, account, dmd)
		_, longPos, _, _, shortPos, _ := GetPositionNumber(ticker, account, dmd)
		lastPrice := dmd.GetLastPrice()
		buyPrice := dmd.GetLastPrice()
		sellPrice := dmd.GetLastPrice()
		longVolume := algo.GetLongVolume()
		shortVolume := algo.GetShortVolume()

		if longVolume+shortVolume == 0 {
			return
		}

		switch mode {
		case MODE_FAST:
			buyPrice = algo.GetMaxLongPrice()
			sellPrice = algo.GetMinShortPrice()

		case MODE_MARKET:
		}

		if longPos < longVolume {
			for i, ord := range sellClose {
				if ord == nil {
					continue
				}

				log.Println("longPos < longVolume:", longPos, longVolume, "cancel sell-close order:", ord.GetOrderID())
				co := CancelOrder(ticker, account, ord.GetOrderID())
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
				longPos += ord.GetVolume()
				sellClose[i] = nil
			}
		}

		for i, ord := range buyOpen {
			if ord == nil {
				continue
			}

			if !Equ(ord.GetPrice(), buyPrice) && ord.GetPrice() < buyPrice {
				log.Println("ord.Price < buyprice, cancel buy-open order:", ord.GetOrderID())
			} else if longPos >= longVolume {
				log.Println("longPos >= longVolume:", longPos, longVolume, "cancel buy-open order:", ord.GetOrderID())
			} else if longPos+ord.GetVolume() > longVolume {
				log.Println("too big order, cancel buy-open order:", ord.GetOrderID())
			} else {
				longPos += ord.GetVolume()
				log.Println("keep order:", ord.GetOrderID())
				continue
			}

			co := CancelOrder(ticker, account, ord.GetOrderID())
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
			buyOpen[i] = nil
		}

		if buyPrice < algo.GetMaxLongPrice() || Equ(algo.GetMaxLongPrice(), buyPrice) {
			for ; longPos < longVolume; longPos++ {
				so := SendOrder(ticker, account, true, true, buyPrice, lastPrice, 1)
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqSendOrder, so)
			}
		}

		if longPos > longVolume {
			for i, ord := range buyOpen {
				if ord == nil {
					continue
				}

				log.Println("longPos > longVolume:", longPos, longVolume, "cancel buy-open order:", ord.GetOrderID())
				co := CancelOrder(ticker, account, ord.GetOrderID())
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
				buyOpen[i] = nil
			}
		}

		for i, ord := range sellClose {
			if ord == nil {
				continue
			}

			if ord.GetPrice() > sellPrice && !Equ(ord.GetPrice(), sellPrice) {
				log.Println("ord.Price > sellprice, cancel sell-close order:", ord.GetOrderID())
			} else if longPos <= longVolume {
				log.Println("longPos <= longVolume:", longPos, longVolume, "cancel sell-close order:", ord.GetOrderID())
			} else if longPos < longVolume+ord.GetVolume() {
				log.Println("too big order, cancel sell-close order:", ord.GetOrderID())
			} else {
				longVolume += ord.GetVolume()
				log.Println("keep order:", ord.GetOrderID())
				continue
			}

			co := CancelOrder(ticker, account, ord.GetOrderID())
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
			sellClose[i] = nil
		}

		for ; longPos > longVolume; longVolume++ {
			so := SendOrder(ticker, account, false, false, sellPrice, lastPrice, 1)
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqSendOrder, so)
		}

		if shortPos < shortVolume {
			for i, ord := range buyClose {
				if ord == nil {
					continue
				}

				log.Println("shortPos < shortVolume:", shortPos, shortVolume, "cancel buy-close order:", ord.GetOrderID())
				co := CancelOrder(ticker, account, ord.GetOrderID())
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
				shortPos += ord.GetVolume()
				buyClose[i] = nil
			}
		}

		for i, ord := range sellOpen {
			if ord == nil {
				continue
			}

			if ord.GetPrice() > sellPrice && !Equ(ord.GetPrice(), sellPrice) {
				log.Println("ord.Price > sellPrice, cancel sell-open order:", ord.GetOrderID())
			} else if shortPos >= shortVolume {
				log.Println("shortPos >= shortVolume:", shortPos, shortVolume, "cancel sell-open order:", ord.GetOrderID())
			} else if shortPos+ord.GetVolume() > shortVolume {
				log.Println("too big order, cancel sell-open order:", ord.GetOrderID())
			} else {
				shortPos += ord.GetVolume()
				log.Println("keep order:", ord.GetOrderID())
				continue
			}

			co := CancelOrder(ticker, account, ord.GetOrderID())
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
			sellOpen[i] = nil
		}

		if sellPrice > algo.GetMinShortPrice() || Equ(sellPrice, algo.GetMinShortPrice()) {
			for ; shortPos < shortVolume; shortPos++ {
				so := SendOrder(ticker, account, false, true, sellPrice, lastPrice, 1)
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqSendOrder, so)
			}
		}

		if shortPos > shortVolume {
			for i, ord := range sellOpen {
				if ord == nil {
					continue
				}

				log.Println("shortPos > shortVolume, cancel sell-open order", ord.GetOrderID())
				co := CancelOrder(ticker, account, ord.GetOrderID())
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
				sellOpen[i] = nil
			}
		}

		for i, ord := range buyClose {
			if ord == nil {
				continue
			}

			if ord.GetPrice() < buyPrice && !Equ(buyPrice, ord.GetPrice()) {
				log.Println("ord.Price < buyprice, cancel buy-close order:", ord.GetOrderID())
			} else if shortPos <= shortVolume {
				log.Println("shortPos <= shortVolume:", shortPos, shortVolume, "cancel buy-close order:", ord.GetOrderID())
			} else if shortPos < shortVolume+ord.GetVolume() {
				log.Println("too big order, cancel buy-close order:", ord.GetOrderID())
			} else {
				shortVolume += ord.GetVolume()
				log.Println("keep order:", ord.GetOrderID())
				continue
			}

			co := CancelOrder(ticker, account, ord.GetOrderID())
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqCancelOrder, co)
			buyClose[i] = nil
		}

		for ; shortPos > shortVolume; shortVolume++ {
			so := SendOrder(ticker, account, true, false, buyPrice, lastPrice, 1)
			SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_ReqSendOrder, so)
		}

		if *flgTest {
			time.Sleep(time.Second)
			hb := &HeartBeat{}
			if buf, err := proto.Marshal(hb); err == nil {
				SendTo(conn, MakeID(strategy), MakeID(exchange), 0, ConstDefine_E_HeartBeat, buf)
			}
		}
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

	buf := TestAlgoOrder("IF0000", "00000048", "ST", "VE", "QUOTEve", "FAST")
	SendTo(conn, MakeID("ST"), MakeID(*flgName), 0, ConstDefine_E_ReqAlgoOrder, buf)
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

	time.Sleep(time.Second)

	for *flgTest {
		Test()
		time.Sleep(time.Second * 10)
	}
	select {}
}
