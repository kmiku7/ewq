package ewq

import(
    "time"
)

type RequestHolder struct {
	request 	interface{}
	requestId	string
	createTime	time.Time
	processTime	time.Time
	finishTime	time.Time
}

func (r *RequestHolder) markProcessStart() {
	r.processTime = time.Now()
}

func (r *RequestHolder) markProcessFinish() {
	r.finishTime = time.Now()
}

func (r *RequestHolder) GetRequest() interface{} {
    return r.request
}
