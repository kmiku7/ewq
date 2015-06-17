package ewq


type EWQ struct {
	maxQueuedRequestNum		int64
	alertQueuedRequestNum	int64
	baseWorkerNum			int64
	maxWorkerNum			int64
	//	unit: ms
	scheduleInterval		int64
	elasticInterval			int64

	// channel
	closeSignal				chan bool
	createToken				chan bool
	exitToken				chan bool
	requestQueue			chan *requestHolder

	// counting & status
	countQueuedRequest		int64
	countProcessingRequest	int64
	countCreatedWorker		int64
	countActiveWorker		int64

	closed					int64
}


type requestHolder {
	request 	interface{}
	requestId	string
	createTime	time.Time
	finishTime	time.Time
}

func (q *EWQ) Stat() string {

}