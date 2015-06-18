package ewq
import (
    "time"
    "sync"
    "sync/atomic"
    "fmt"
    "errors"
)


var (
	ErrFull	= errors.New("Queue Is Full")
	ErrClosed = errors.New("Queue Is Closed")
)


type Config struct {
	MaxRequestQueueLen		int64
	AlertRequestQueueLen	int64
	MaxWorkerNum 			int64
	BaseWorkerNum 			int64
	// Unit: MS
	ScheduleIntervalMS 		int64
	ElasticIntervalMS		int64
}

type WorkerInitialize	func() interface{}
type WorkerCleanup		func(context interface{})
type WorkerWork			func(context interface{}, q *EWQ, requestQueue <-chan *RequestHolder, exitSignal <-chan bool)
type Logger				func(string)

type Handler struct {
	Initializer WorkerInitialize
	Work 		WorkerWork
	Cleanup 	WorkerCleanup
	Log 		Logger
}

type EWQ struct {
	config Config
	handler Handler

	// channel
	closeSignal				chan bool
	createToken				chan bool
	exitToken				chan bool
	requestQueue			chan *RequestHolder

	// counting & status
	countActiveWorker		int64
	countTargetWorker		int64
	countProcessingRequest 	int64

	closed				    bool
	workerGroup				sync.WaitGroup
	systemGroup				sync.WaitGroup
}

func NewEWQ(config Config, handler Handler) (*EWQ, error) {

	if config.MaxRequestQueueLen <= 0 ||
		config.AlertRequestQueueLen <= 0 ||
		config.MaxWorkerNum <= 0 ||
		config.BaseWorkerNum <= 0 ||
		config.ScheduleIntervalMS <= 0 ||
		config.ElasticIntervalMS <= 0 ||
		handler.Initializer == nil ||
		handler.Work == nil ||
		handler.Cleanup == nil {
		return nil, errors.New("invalid parameters")
	}

	if config.AlertRequestQueueLen > config.MaxRequestQueueLen ||
		config.BaseWorkerNum > config.MaxWorkerNum ||
		config.ScheduleIntervalMS > config.ElasticIntervalMS {
		return nil, errors.New("restrict violation")
	}


	q := &EWQ{
		config: 	config,
		handler: 	handler,

		closeSignal:	make(chan bool),
		createToken:	make(chan bool, config.MaxWorkerNum*2),
		exitToken:		make(chan bool,	config.MaxWorkerNum*2),
		requestQueue:	make(chan *RequestHolder, config.MaxRequestQueueLen),

		countProcessingRequest:	0,
		countActiveWorker:		0,
		countTargetWorker:		0,
		closed:					false,
	}

	// worker factory
	q.systemGroup.Add(1)
	go q.workerFactory()

	// create base worker
	for idx := int64(0); idx < config.BaseWorkerNum; idx += 1 {
		q.createToken <- true
	}

	q.systemGroup.Add(1)
	go q.workerMonitor()

	q.systemGroup.Add(1)
	go q.idleWorkerCloser()

	return q, nil
}

func (q *EWQ) PushRequest(request interface{}, reqId string) error {

	if q.closed {
		return ErrClosed
	}

	if int64(len(q.requestQueue)) > q.config.AlertRequestQueueLen {
        q.log(fmt.Sprintf("Reach Alert Level:%d\n", len(q.requestQueue)))
		for count := 0; count < 2; count += 1 {
			select {
			case q.createToken <- true:
			default:
			}
		}
	}

	req := &RequestHolder {
		request: request,
		requestId: reqId,
		createTime:	time.Now(),
	}

	select{
	case q.requestQueue <- req:
		q.log(fmt.Sprintf("Requst:%s Queued", reqId))
		return nil
	default:
		return ErrFull
	}
}

func (q *EWQ) Stat() string {
	return fmt.Sprintf("EWQ QueuedRequest:%d ActiveWorker:%d", len(q.requestQueue), q.countActiveWorker)
}

func (q *EWQ) Close() {
	q.closed = true
	go q.sendCloseToken()
	q.workerGroup.Wait()
}

func (q *EWQ) MarkRequestProcessStart(req *RequestHolder) {
	atomic.AddInt64(&(q.countProcessingRequest), 1)
	req.markProcessStart()
}

func (q *EWQ) MarkRequestProcessEnd(req *RequestHolder) {
	atomic.AddInt64(&(q.countProcessingRequest), -1)
	req.markProcessFinish()
}

func (q *EWQ) workerFactory() {
	defer q.systemGroup.Done()
	q.log("Worker Factory Start")

	LOOP:
	for {
	select {
	case <- q.closeSignal:
		break LOOP
	case <- q.createToken:
		if q.countActiveWorker < q.config.MaxWorkerNum {
			q.log("Worker Factory Create New Worker")
			q.workerGroup.Add(1)
			atomic.AddInt64(&(q.countActiveWorker), 1)
			atomic.AddInt64(&(q.countTargetWorker), 1)
			go q.workerWrapper()		
		}
	} // end select
	} // end for

	q.log("Worker Factory Exit")
}

func (q *EWQ) workerMonitor() {
	defer q.systemGroup.Done()
	q.log("Worker Monitor Start")

	interval, _ := time.ParseDuration("5s")
	LOOP:
	for {
		select{
	case <- q.closeSignal:
		break LOOP
	case <- time.After(interval):
		q.log(q.Stat())
    }
	}

	q.log("Worker Monitor Exit")
}

func (q *EWQ) idleWorkerCloser() {
	defer q.systemGroup.Done()
	q.log("IdleWorkerCloser Start")

	interval, _ := time.ParseDuration(fmt.Sprintf("%dms", q.config.ScheduleIntervalMS))
	//elasticInterval := time.ParseDuration(fmt.Sprintf("%dms", q.config.ElasticIntervalMS))
	highLevelTime := time.Now()
	tenSeconds, _ := time.ParseDuration("10s")
    q.log(fmt.Sprintf("ScheduleInterval:%s, TotalInterval:%s\n", interval, tenSeconds))

	LOOP:
	for {
		select{
		case <- q.closeSignal:
			break LOOP
		case <- time.After(interval):
			if int64(len(q.requestQueue)) > q.config.AlertRequestQueueLen {
				highLevelTime = time.Now()
			    continue
			}

			if q.countTargetWorker > q.config.BaseWorkerNum && highLevelTime.Add(tenSeconds).Before(time.Now()) {
				closeCount := (q.config.MaxWorkerNum - q.config.BaseWorkerNum) / 30
				if closeCount >= q.countTargetWorker - q.config.BaseWorkerNum {
					closeCount = q.countTargetWorker - q.config.BaseWorkerNum
				}
				q.log(fmt.Sprintf("Close Worker, Num:%d", closeCount))
			    atomic.AddInt64(&(q.countTargetWorker), -closeCount)
				for count := int64(0); count < closeCount; count += 1 {
					q.exitToken <- true
				}
                highLevelTime = time.Now()
			}
        }
	}
	q.log("IdleWorkerCloser End")
}

func (q *EWQ) workerWrapper() {
	defer q.workerGroup.Done()
	defer atomic.AddInt64(&(q.countActiveWorker), -1)

	var context interface{} = nil
	if q.handler.Initializer != nil {
		context = q.handler.Initializer()
	}

	q.log("Work Start")
	q.handler.Work(context, q, q.requestQueue, q.exitToken)
	q.log("Work End")

	if q.handler.Cleanup != nil {
		q.handler.Cleanup(context)
	}
}

func (q *EWQ) sendCloseToken() {
	q.systemGroup.Wait()

	activeWorkerNum := q.countActiveWorker
	for idx := int64(0); idx < activeWorkerNum; idx += 1 {
		q.exitToken <- true
	}
}

func (q *EWQ) log(info string) {
	if q.handler.Log != nil {
		q.handler.Log(info)
	}
}
 


