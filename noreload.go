// 実況板流し読み
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"code.google.com/p/go.net/websocket"
	"github.com/tanaton/get2ch-go"
	"code.google.com/p/go-charset/charset"
	_ "code.google.com/p/go-charset/data"
)

const (
	DAT_DIR				= "/2ch/dat"
	CONFIG_JSON_PATH	= "noreload.json"
	MAX_PROCS			= 64
	// 2秒毎
	IGNITION_TIME		= 2 * time.Second
	BOARD_CYCLE_TIME	= 30 * time.Second
	SERVER_CYCLE_TIME	= 120 * time.Minute
	WS_DEADLINE_TIME	= 10 * time.Second
)

type Nich struct {
	server			string
	board			string
	thread			string
}

type Board struct {
	n				Nich
	res				int
	speed			float64
}

type DispatchHandler struct {
	mwh				map[string]websocket.Handler
	mwhu			sync.RWMutex
	logger			*log.Logger
	slch			<-chan *map[string]string
}

type WsConnection struct {
	listen			map[*websocket.Conn]chan error
	mux				sync.RWMutex
	board			string
	dh				*DispatchHandler
}

type Config struct {
	v				map[string]interface{}
	wh				string
	wp				int
	ReadTimeoutSec	int
	WriteTimeoutSec	int
	MaxHeaderBytes	int
}

var g_reg_bbs *regexp.Regexp = regexp.MustCompile("(.+\\.2ch\\.net|.+\\.bbspink\\.com)/(.+)<>")
var g_reg_dat *regexp.Regexp = regexp.MustCompile("^([0-9]{9,10})\\.dat<>.* \\(([0-9]+)\\)$")
var g_reg_title *regexp.Regexp = regexp.MustCompile("^.*?<>.*?<>.*?<>.*?<>(.*?)\n")

func main() {
	c := readConfig()
	slch := getServerCh()
	logger := log.New(os.Stdout, "", log.Ldate | log.Ltime | log.Lmicroseconds)
	myHandler := &DispatchHandler{
		mwh		: make(map[string]websocket.Handler),
		logger	: logger,
		slch	: slch,
	}
	server := &http.Server{
		Addr			: fmt.Sprintf("%s:%d", c.wh, c.wp),
		Handler			: myHandler,
		ReadTimeout		: time.Duration(c.ReadTimeoutSec) * time.Second,
		WriteTimeout	: time.Duration(c.WriteTimeoutSec) * time.Second,
		MaxHeaderBytes	: c.MaxHeaderBytes,
	}
	// サーバ起動
	logger.Fatal(server.ListenAndServe())
}

func (dh *DispatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := dh.checkPath(r.URL.Path)
	if err == nil {
		dh.match(b).ServeHTTP(w, r)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (dh *DispatchHandler) checkPath(path string) (string, error) {
	l := strings.Split(path, "/")
	if len(l) != 2 { return "", errors.New("path error") }
	sl := <-dh.slch
	if _, ok := (*sl)[l[1]]; !ok { return "", errors.New("path error") }
	return l[1], nil
}

func (dh *DispatchHandler) match(b string) websocket.Handler {
	dh.mwhu.RLock()
	defer dh.mwhu.RUnlock()
	wh, ok := dh.mwh[b]
	if !ok {
		wh = websocket.Handler(createWebHandler(b, dh))
		dh.mwh[b] = wh
	}
	return wh
}

func createWebHandler(board string, dh *DispatchHandler) func(ws *websocket.Conn) {
	wsc := &WsConnection{
		listen	: make(map[*websocket.Conn]chan error),
		board	: board,
		dh		: dh,
	}
	go wsc.timeCallback()

	return func(ws *websocket.Conn) {
		ech := make(chan error, 1)
		defer ws.Close()
		defer close(ech)
		wsc.dh.logger.Printf("connect!")

		wsc.mux.Lock()
		wsc.listen[ws] = ech
		wsc.mux.Unlock()

		err := <-ech
		if err != nil {
			wsc.dh.logger.Printf("%s", err.Error())
		}

		wsc.mux.Lock()
		delete(wsc.listen, ws)
		wsc.mux.Unlock()
	}
}

func (wsc *WsConnection) timeCallback() {
	// 同時送信数の制限
	sync := make(chan bool, MAX_PROCS)
	defer wsc.dispose(sync)
	// 一定期間で発火するインターバルタイマの生成
	tick := time.Tick(IGNITION_TIME)
	nch, fire := getBoardCh(wsc.board, wsc.dh.slch)

	for _ = range tick {
		if wsc.isConnection() == false {
			// 接続がない時は終了する
			break
		}
		n := <-nch
		data, resno, err := getData(n)
		if err != nil {
			wsc.dh.logger.Printf("Error:%s", err.Error())
			continue
		} else if data == nil {
			// 更新無し
			continue
		} else if resno >= 1000 {
			fire <- true
			<-nch
		}
		dl := time.Now().Add(WS_DEADLINE_TIME)
		wsc.mux.RLock()
		for socket, ech := range wsc.listen {
			socket.SetDeadline(dl)
			sync <- true
			go writeData(data, socket, ech, sync)
		}
		wsc.mux.RUnlock()
	}
}

func (wsc *WsConnection) isConnection() bool {
	wsc.mux.RLock()
	defer wsc.mux.RUnlock()
	return len(wsc.listen) != 0
}

func (wsc *WsConnection) dispose(sync chan bool) {
	wsc.dh.mwhu.Lock()
	defer wsc.dh.mwhu.Unlock()
	delete(wsc.dh.mwh, wsc.board)
	close(sync)
}

func writeData(data []byte, con *websocket.Conn, ech chan error, sync <-chan bool) {
	_, werr := con.Write(data)
	if werr != nil {
		ech <- werr
	}
	<-sync
}

func getData(n Nich) ([]byte, int, error) {
	var str, title string
	var resno int
	get := get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR))
	//if wss.sh != "" { get.SetSalami(wss.sh, wss.sp) }
	moto, err := get.Cache.GetData(n.server, n.board, n.thread)
	if err != nil { moto = nil }
	err = get.SetRequest(n.server, n.board, n.thread)
	if err != nil { return nil, 0, err }
	data, err := get.GetData()
	if err != nil { return nil, 0, err }

	if moto == nil {
		str, resno = sendData(data, 0)
	} else {
		mlen := len(moto)
		if len(data) > mlen {
			resno = bytes.Count(moto, []byte{'\n'})
			str, resno = sendData(data[mlen:], resno)
		} else {
			// 更新無し
			return nil, 0, nil
		}
	}
	if t := g_reg_title.FindSubmatch(data); len(t) == 2 {
		title = string(t[1])
	}
	sdata, err := sjisToUtf8([]byte(n.board + "/" + n.thread + " " + title + "\n" + str))
	if err != nil { return nil, 0, err }
	return sdata, resno, nil
}

func sendData(data []byte, resno int) (string, int) {
	line := strings.Split(string(data), "\n")
	for i, _ := range line[0:len(line) - 1] {
		resno++
		line[i] = fmt.Sprintf("%d<>%s", resno, line[i])
	}
	return strings.Join(line, "\n"), resno
}

func getServerCh() <-chan *map[string]string {
	ch := make(chan *map[string]string, 4)
	go func() {
		tch := time.Tick(SERVER_CYCLE_TIME)
		sl := getServer()
		for {
			select {
			case <-tch:
				sl = getServer()
			default:
				ch <- &sl
			}
		}
	}()
	return ch
}

func getServer() map[string]string {
	sl := make(map[string]string)
	get := get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR))
	data, err := get.GetServer()
	if err != nil {
		data, err = get.Cache.GetData("", "", "")
		if err != nil { panic(err.Error()) }
	}
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if d := g_reg_bbs.FindStringSubmatch(it); len(d) == 3 {
			sl[d[2]] = d[1]
		}
	}
	return sl
}

func getBoardCh(board string, slch <-chan *map[string]string) (<-chan Nich, chan<- bool) {
	ch := make(chan Nich)
	fire := make(chan bool, 1)
	go func() {
		var nich Nich
		f := func() {
			sl := <-slch
			if s, ok := (*sl)[board]; ok {
				nich = getBoard(s, board)
			}
		}
		tch := time.Tick(BOARD_CYCLE_TIME)
		f()
		for {
			select {
			case <-tch:
				f()
			case <-fire:
				f()
			default:
				ch <- nich
			}
		}
	}()
	return ch, fire
}

func getBoard(s, board string) (n Nich) {
	now := float64(time.Now().Unix())
	get := get2ch.NewGet2ch(get2ch.NewFileCache(DAT_DIR))
//	if wss.sh != "" { get.SetSalami(wss.sh, wss.sp) }
//	err := get.SetRequest(s, wss.board, "")
	err := get.SetRequest(s, board, "")
	if err != nil { return }
	data, err := get.GetData()
	if err != nil {
		log.Printf(err.Error() + "\n")
		return
	}
	bl := make([]Board, 0, 1)
	list := strings.Split(string(data), "\n")
	for _, it := range list {
		if d := g_reg_dat.FindStringSubmatch(it); len(d) == 3 {
			n.server = s
			n.board = board
			n.thread = d[1]
			num, e1 := strconv.Atoi(d[1])
			res, e2 := strconv.Atoi(d[2])

			if e1 == nil && e2 == nil && res > 0 {
				tmp := (now - float64(num)) / float64(res)
				if tmp > 0.0 {
					// ゼロ除算防止
					bl = append(bl, Board{
						n		: n,
						res		: res,
						speed	: (86400.0 / tmp),
					})
				}
			}
		}
	}
	bl = Qsort(bl, func(a, b Board) float64 {
		return b.speed - a.speed
	})
	for _, bline := range bl {
		if bline.res >= 5 && bline.res <= 1000 {
			n = bline.n
			break
		}
	}
	return n
}

func Qsort(list []Board, cmp func(a, b Board) float64) []Board {
	ret := make([]Board, len(list))
	copy(ret, list)
	stack := make([]int, 0, 2)
	stack = append(stack, 0)
	stack = append(stack, len(list) - 1)
	for len(stack) != 0 {
		tail := stack[len(stack) - 1]
		stack = stack[0:len(stack) - 1]
		head := stack[len(stack) - 1]
		stack = stack[0:len(stack) - 1]
		pivot := ret[head + ((tail - head) >> 1)]
		i := head - 1
		j := tail + 1
		for {
			for i++; cmp(ret[i], pivot) < 0.0; i++ {}
			for j--; cmp(ret[j], pivot) > 0.0; j-- {}
			if i >= j { break }
			tmp := ret[i]
			ret[i] = ret[j]
			ret[j] = tmp
		}
		if head < (i - 1) {
			stack = append(stack, head)
			stack = append(stack, i - 1)
		}
		if (j + 1) < tail {
			stack = append(stack, j + 1)
			stack = append(stack, tail)
		}
	}
	return ret
}

func sjisToUtf8(data []byte) ([]byte, error) {
	r, err := charset.NewReader("cp932", bytes.NewReader(data))
	if err != nil { return nil, err }
	result, err := ioutil.ReadAll(r)
	return result, err
}

func readConfig() *Config {
	c := &Config{v: make(map[string]interface{})}
	argc := len(os.Args)
	var path string
	if argc == 2 {
		path = os.Args[1]
	} else {
		path = CONFIG_JSON_PATH
	}
	c.read(path)
	return c
}

func (c *Config) read(filename string) {
	data, err := ioutil.ReadFile(filename)
	if err != nil { return }
	err = json.Unmarshal(data, &c.v)
	if err != nil { return }

	c.wh = c.getDataString("WSHost", "localhost")
	c.wp = c.getDataInt("WSPort", 8000)
	c.ReadTimeoutSec = c.getDataInt("ReadTimeoutSec", 10)
	c.WriteTimeoutSec = c.getDataInt("WriteTimeoutSec", 10)
	c.MaxHeaderBytes = c.getDataInt("MaxHeaderBytes", 1024 * 10)
}

func (c *Config) getDataInt(h string, def int) (ret int) {
	ret = def
	if it, ok := c.v[h]; ok {
		if f, err := it.(float64); err {
			ret = int(f)
		}
	}
	return
}

func (c *Config) getDataString(h, def string) (ret string) {
	ret = def
	if it, ok := c.v[h]; ok {
		if ret, ok = it.(string); !ok {
			ret = def
		}
	}
	return
}

