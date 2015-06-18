package main

import (
    "io/ioutil"
	"log"
	"net"
	"ewq"
    "math/rand"
    "time"
    "fmt"
)

type Request struct {
	info string
}

func initialize() interface{} {
	return nil
}

func cleanup(interface{}) {
	return
}

func work(context interface{}, q *ewq.EWQ, requestQueue <-chan *ewq.RequestHolder, exitSignal <-chan bool) {
	for {
	select{
	case <- exitSignal:
		return
	case req_holder := <- requestQueue:
		req := req_holder.GetRequest().(*Request)
		sleep_time := rand.Int31n(50)
		interval, _ := time.ParseDuration(fmt.Sprintf("%dms", sleep_time))
		fmt.Printf("Time:%s SLEEP:%dms, REQ:%s\n", time.Now(), sleep_time, req.info)
		time.Sleep(interval)
	}
	}
}

func logger(info string) {
	fmt.Printf("PL:%s: %s\n", time.Now(), info)
}

func main() {
	// Listen on TCP port 2000 on all interfaces.
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	config := ewq.Config{
		MaxRequestQueueLen:		800,
		AlertRequestQueueLen: 	100,
		MaxWorkerNum:			1000,
		BaseWorkerNum:			100,
		ScheduleIntervalMS:		50,
		ElasticIntervalS:		60,
	}

	handler := ewq.Handler{
		Initializer: 	initialize,
		Work:			work,
		Cleanup:		cleanup,
		Log:			logger,
	}

	q, err := ewq.NewEWQ(config, handler)
	if err != nil {
		fmt.Println("new failed, err:%s", err)
		return
	}

	defer q.Close()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			data, err := ioutil.ReadAll(c)
			fmt.Printf("err:%v\n", err)
			req := &Request{
				info: string(data),
			}
			q.PushRequest(req, string(data))
			c.Close()
		}(conn)
	}
}
