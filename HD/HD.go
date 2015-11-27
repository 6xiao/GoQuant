package main

import (
	"code.google.com/p/goprotobuf/proto"
	"database/sql"
	"flag"
	"fmt"
	. "github.com/6xiao/GoQuant/DataType"
	"github.com/6xiao/go/Common"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"net"
	"time"
	"unsafe"
)

var (
	flgName   = flag.String("name", "HD", "module name")
	flgMc     = flag.String("mc", "127.0.0.1:5381", "message channel ip:port")
	flgDB     = flag.String("db", "mysql", "DataBase type")
	flgDBlink = flag.String("dblink", "root:@tcp(127.0.0.1:3306)/?charset=utf8", "DataBase link")
)

var (
	sendChan  = make(chan []byte)
	quoteChan = make(chan *DepthMarketData, 1024*1024)
	klineChan = make(chan *KlineReq, 1024*1024)
	tickChan  = make(chan *TickReq, 1024*1024)
)

type KlineReq struct {
	sender uint64
	copyer uint64
	tm     uint64
	kline  *ReqKLine
}

type TickReq struct {
	sender uint64
	copyer uint64
	tm     uint64
	tick   *ReqTick
}

func Send(conn net.Conn) {
	for {
		buf := <-sendChan
		if buf == nil {
			conn.Close()
			return
		}
		conn.Write(buf)
		buf = nil
	}
}

func SendTo(id uint64, copyer uint64, tp ConstDefine, buf []byte) {
	sockbuf := make([]byte, MsgLen+RecLen+uint32(len(buf)))
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	rh := (*RecordHead)(unsafe.Pointer(&sockbuf[MsgLen]))
	bodybuf := sockbuf[MsgLen+RecLen:]
	mh.Msglen = uint64(len(sockbuf)) - HeadLen
	mh.Sender = MakeID(*flgName)
	mh.Recver = id
	mh.Copyer = copyer
	if buf != nil {
		rh.HashCode = Common.Hash(buf)
		rh.RecType = uint32(tp)
		rh.Length = uint32(len(buf))
		copy(bodybuf, buf)
	}
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

		case uint32(ConstDefine_E_ReqKLine):
			rkl := &ReqKLine{}
			proto.Unmarshal(bodybuf, rkl)
			klineChan <- &KlineReq{mh.Sender, mh.Copyer, mh.SendTime, rkl}

		case uint32(ConstDefine_E_ReqTick):
			rt := &ReqTick{}
			proto.Unmarshal(bodybuf, rt)
			tickChan <- &TickReq{mh.Sender, mh.Copyer, mh.SendTime, rt}
		}
	}
}

func ReqProc(db *sql.DB) {
	for {
		select {
		case rt := <-tickChan:
			beg := rt.tick.GetStartTime()
			end := rt.tick.GetEndTime()
			tick := rt.tick.GetTickerName()
			fmt.Println("tick req: ", beg, " to ", end)
			fmt.Println(rt.tick)
			ss := "SELECT `Time`,`Volume`,`LastPrice`,`Turnover`,`Settlement` FROM tick.%s WHERE `Time` BETWEEN %v AND %v ORDER BY `Time`"
			rows, err := db.Query(fmt.Sprintf(ss, tick, beg, end))
			if err != nil {
				fmt.Println(err)
				break
			}

			for i := 0; i < 1024*1024; i++ {
				rsp := &RspTick{TickerName: &tick, StartTime: &beg, EndTime: &end, Ticks: make([]*DepthMarketData, 0)}
				for len(rsp.Ticks) < 65536 && rows.Next() {
					r := new(DepthMarketData)
					r.TickerName = new(string)
					r.TickerName = &tick
					r.Time = new(uint64)
					r.Volume = new(uint64)
					r.LastPrice = new(float64)
					r.Turnover = new(float64)
					r.Settlement = new(float64)
					rows.Scan(r.Time, r.Volume, r.LastPrice, r.Turnover, r.Settlement)
					end = r.GetTime()
					rsp.Ticks = append(rsp.Ticks, r)
				}

				if buf, err := proto.Marshal(rsp); err == nil {
					SendTo(rt.sender, rt.copyer, ConstDefine_E_RspTick, buf)
				} else {
					fmt.Println(err)
				}

				if len(rsp.Ticks) < 65536 {
					break
				}
			}
			rows.Close()

		case rkl := <-klineChan:
			beg := rkl.kline.GetStartTime()
			end := rkl.kline.GetEndTime()
			tick := rkl.kline.GetTickerName()
			fmt.Println("klinereq: ", beg, " to ", end)
			fmt.Println(rkl.kline)
			if rkl.kline.GetReq_Minute() {
				cy := rkl.kline.GetNum_Minutes()
				rsp := &RspKLine{TickerName: &tick, StartTime: &beg, EndTime: &end, Num_Minutes: &cy, Bar_Minute: make([]*TOHLCV, 0)}
				ss := "SELECT * FROM minute.%s WHERE `BeginTime` BETWEEN %v AND %v ORDER BY `BeginTime`"
				rows, err := db.Query(fmt.Sprintf(ss, tick, beg, end))
				if err != nil {
					fmt.Println(err)
					break
				}

				var mg *TOHLCV
				for i := uint64(0); rows.Next(); i++ {
					r := new(TOHLCV)
					r.StartTime = new(uint64)
					r.EndTime = new(uint64)
					r.OpenPrice = new(float64)
					r.HighPrice = new(float64)
					r.LowPrice = new(float64)
					r.ClosePrice = new(float64)
					r.Volume = new(uint64)
					r.TotalVolume = new(uint64)
					r.OpenInterest = new(uint64)
					r.Turnover = new(float64)
					r.Settlement = new(float64)
					rows.Scan(r.StartTime, r.EndTime, r.OpenPrice, r.HighPrice, r.LowPrice,
						r.ClosePrice, r.Volume, r.TotalVolume, r.OpenInterest, r.Turnover, r.Settlement)
					end = r.GetEndTime()

					if i == cy {
						rsp.Bar_Minute = append(rsp.Bar_Minute, mg)
						i = 0
					}

					if i == 0 {
						mg = r
					} else if i < cy {
						if mg.GetHighPrice() < r.GetHighPrice() {
							mg.HighPrice = r.HighPrice
						}
						if mg.GetLowPrice() > r.GetLowPrice() {
							mg.LowPrice = r.LowPrice
						}
						mg.ClosePrice = r.ClosePrice
						mg.TotalVolume = r.TotalVolume
						mg.OpenInterest = r.OpenInterest
						mg.Turnover = r.Turnover
						*mg.Volume = r.GetVolume() + mg.GetVolume()
					}
				}
				rows.Close()
				rsp.Bar_Minute = append(rsp.Bar_Minute, mg)

				if buf, err := proto.Marshal(rsp); err == nil {
					SendTo(rkl.sender, rkl.copyer, ConstDefine_E_RspKLine, buf)
				} else {
					fmt.Println(err)
				}
			}
		}
	}
}

func CreateKlineTable(db *sql.DB, ticker string) error {
	create := "CREATE TABLE IF NOT EXISTS minute.%s (" +
		"`BeginTime` bigint(20) UNSIGNED NOT NULL," +
		"`EndTime` bigint(20) UNSIGNED NOT NULL," +
		"`Open` double NOT NULL," +
		"`High` double NOT NULL," +
		"`Low` double NOT NULL," +
		"`Close` double NOT NULL," +
		"`Volume` bigint(20) UNSIGNED NOT NULL," +
		"`TotalVolume` bigint(20) UNSIGNED NOT NULL," +
		"`OpenInterest` bigint(20) UNSIGNED NOT NULL," +
		"`Turnover` double NOT NULL," +
		"`Settlement` double NOT NULL" +
		") ENGINE=MyISAM DEFAULT CHARSET=utf8"

	_, err := db.Exec(fmt.Sprintf(create, ticker))

	return err
}

func CreateTickerTable(db *sql.DB, ticker string) error {
	create := "CREATE TABLE IF NOT EXISTS tick.%s (" +
		"`Time` bigint(20) UNSIGNED NOT NULL," +
		"`Volume` bigint(20) UNSIGNED NOT NULL," +
		"`OpenInterest` bigint(20) UNSIGNED NOT NULL," +
		"`BuyVloume` bigint(20) UNSIGNED NOT NULL," +
		"`SellVolume` bigint(20) UNSIGNED NOT NULL," +
		"`BuyPrice` double NOT NULL," +
		"`SellPrice` double NOT NULL," +
		"`LastPrice` double NOT NULL," +
		"`Turnover` double NOT NULL," +
		"`Settlement` double NOT NULL," +
		"KEY `tm` (`Time`)" +
		") ENGINE=MyISAM DEFAULT CHARSET=utf8"

	_, err := db.Exec(fmt.Sprintf(create, ticker))

	return err
}

func InsertTicker(db *sql.DB, ticker string, dmd *DepthMarketData) {
	bv := uint64(0)
	if v := dmd.GetBuyVolume(); v != nil {
		bv = v[0]
	}
	bp := float64(0.0)
	if v := dmd.GetBuyPrice(); v != nil {
		bp = v[0]
	}
	sv := uint64(0)
	if v := dmd.GetSellVolume(); v != nil {
		sv = v[0]
	}
	sp := float64(0.0)
	if v := dmd.GetSellPrice(); v != nil {
		sp = v[0]
	}

	insert := "INSERT INTO tick.%s VALUES(%v,%v,%v,%v,%v,%.4f,%.4f,%.4f,%.4f,%.4f)"
	s := fmt.Sprintf(insert, ticker, dmd.GetTime(), dmd.GetVolume(), dmd.GetOpenInterest(),
		bv, sv, bp, sp, dmd.GetLastPrice(), dmd.GetTurnover(), dmd.GetSettlement())
	_, err := db.Exec(s)
	if err != nil {
		fmt.Println(err)
	}
}

func InsertMinute(db *sql.DB, ticker string, min uint64, lastvolume uint64) {
	beg := min * 100 * 1000
	end := beg + 59999

	ss := "SELECT `Volume`,`OpenInterest`,`LastPrice`,`Turnover`,`Settlement` FROM tick.%s WHERE `Time` BETWEEN %v AND %v ORDER BY `Time`"
	rows, err := db.Query(fmt.Sprintf(ss, ticker, beg, end))
	if err != nil {
		fmt.Println(err)
		return
	}

	var o, h, l, c, to, se float64
	var v, oi uint64
	for rows.Next() {
		rows.Scan(&v, &oi, &c, &to, &se)
		if o == 0 {
			o = c
		}
		if c > h {
			h = c
		}
		if l == 0 || l > c {
			l = c
		}
	}
	rows.Close()

	insert := "INSERT INTO minute.%s VALUES(%v,%v,%.4f,%.4f,%.4f,%.4f,%v,%v,%v,%v,%v)"
	s := fmt.Sprintf(insert, ticker, beg, end, o, h, l, c, v-lastvolume, v, oi, to, se)
	fmt.Println(s)
	_, err = db.Exec(s)
	if err != nil {
		fmt.Println(err)
	}

	var cy uint64 = 1
	r := new(TOHLCV)
	r.StartTime = &beg
	r.EndTime = &end
	r.OpenPrice = &o
	r.HighPrice = &h
	r.LowPrice = &l
	r.ClosePrice = &c
	r.Volume = new(uint64)
	*r.Volume = v - lastvolume
	rsp := &RspKLine{TickerName: &ticker, StartTime: &beg, EndTime: &end, Num_Minutes: &cy, Bar_Minute: []*TOHLCV{r}}
	if buf, err := proto.Marshal(rsp); err == nil {
		SendTo(MakeID("QUOTE"), MakeID("KLINE"), ConstDefine_E_RspKLine, buf)
	} else {
		fmt.Println(err)
	}
}

func GetLastTime(db *sql.DB, filed1, filed2, schema, table string) (uint64, uint64) {
	s := fmt.Sprintf("SELECT %s,%s FROM %s.%s ORDER BY %s DESC LIMIT 1", filed1, filed2, schema, table, filed1)
	rows, err := db.Query(s)
	if err != nil {
		fmt.Println(s, err)
		return 0, 0
	}

	var tm, v uint64
	for rows.Next() {
		rows.Scan(&tm, &v)
	}
	rows.Close()

	return tm, v
}

func GetFirstTimeVolume(db *sql.DB, table string, tmi uint64) (uint64, uint64) {
	s := fmt.Sprintf("SELECT Time, Volume FROM tick.%s WHERE Time > %v ORDER BY Time LIMIT 1", table, tmi)
	rows, err := db.Query(s)
	if err != nil {
		fmt.Println(s, err)
		return 0, 0
	}

	var tm, vlm uint64
	for rows.Next() {
		rows.Scan(&tm, &vlm)
	}
	rows.Close()

	return tm, vlm
}

func GetTablesName(db *sql.DB) []string {
	var res []string

	s := fmt.Sprintf("SELECT TABLE_NAME FROM information_schema.TABLES where TABLE_SCHEMA ='tick'")
	rows, err := db.Query(s)
	if err != nil {
		fmt.Println(s, err)
		return res
	}

	for rows.Next() {
		var name string
		rows.Scan(&name)
		res = append(res, name)
	}
	rows.Close()

	return res
}

func KlineProc(db *sql.DB) {
	for {
		tables := GetTablesName(db)

		now := time.Now()
		for _, tbnm := range tables {
			err := CreateKlineTable(db, tbnm)
			if err != nil {
				fmt.Println(err)
				return
			}

			for {
				ktm, lkv := GetLastTime(db, "EndTime", "TotalVolume", "minute", tbnm)
				ttm, _ := GetLastTime(db, "Time", "Volume", "tick", tbnm)
				if ktm >= ttm { // trans done
					break
				}

				ftm, fv := GetFirstTimeVolume(db, tbnm, ktm)
				if lkv == 0 || lkv > fv {
					lkv = fv
				}

				if now.Hour() > 15 { // out time
					InsertMinute(db, tbnm, ftm/100/1000, lkv)
					continue
				}

				first := ftm / 100 / 1000
				last := ttm / 100 / 1000
				if first != last { // enough minute
					InsertMinute(db, tbnm, ftm/100/1000, lkv)
					fmt.Println(tbnm, "insert minute", first, last)
				} else {
					break
				}
			}
		}

		if now.Weekday() == time.Saturday ||
			now.Weekday() == time.Sunday ||
			now.Hour() < 9 || now.Hour() > 15 { // only IF
			time.Sleep(time.Minute * 10)
		} else {
			time.Sleep(time.Second * 10)
		}
	}
}

func QuoteProc(db *sql.DB) {
	table := make(map[string]bool)

	for {
		q := <-quoteChan
		tk := q.GetTickerName()
		if "IF" != tk[:2] { // only IF
			continue
		}

		if !table[tk] {
			err := CreateTickerTable(db, tk)
			if err != nil {
				fmt.Println(err)
				continue
			} else {
				table[tk] = true
			}
		}

		InsertTicker(db, tk, q)
	}
}

func Register(conn net.Conn) {
	sockbuf := make([]byte, 1024)
	mh := (*MessageHead)(unsafe.Pointer(&sockbuf[0]))
	mh.Msglen = uint64(MsgLen) - HeadLen

	mh.Sender = MakeID(*flgName)
	sendChan <- sockbuf[:MsgLen]
}

func main() {
	flag.Parse()
	flag.Usage()

	db, err := sql.Open(*flgDB, *flgDBlink)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	fmt.Println(*flgDB, *flgDBlink, "connected db")
	db2, _ := sql.Open(*flgDB, *flgDBlink)
	db3, _ := sql.Open(*flgDB, *flgDBlink)

	conn, err := net.Dial("tcp", *flgMc)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	fmt.Println(*flgMc, "connected mc")

	go ReqProc(db)
	go QuoteProc(db2)
	go KlineProc(db3)
	go Send(conn)
	go Register(conn)
	go Recv(conn)

	select {}
}
