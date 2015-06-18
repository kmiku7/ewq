package main


import (
    "net"
    "fmt"
    "time"
)



func main() {

    addr := "127.0.0.1:8000"

    interval, _ := time.ParseDuration("100ms")
    requestNum := 1000
    loop_idx := 0
    interval_100ms, _ := time.ParseDuration("100ms")

    for {
        select {
        case <- time.After(interval):
            go func(loopIdx int) {
                for idx := 0; idx < requestNum; idx += 1{
                    go func() {
                        conn, err := net.DialTimeout("tcp", addr, interval_100ms)
                        if err != nil {
                            fmt.Printf("dial err:%s\n", err)
                            return
                        }
                        defer conn.Close()

                        content := fmt.Sprintf("(%d,%d) REQUEST:%s", loopIdx, idx, time.Now())
                        content_send := []byte(content)
                        conn.SetWriteDeadline(time.Now().Add(interval_100ms))
                        write_len, err := conn.Write(content_send)
                        if err != nil || write_len != len(content_send) {
                            fmt.Printf("write data failed:%s, expect:%d, ret:%d\n", err, len(content_send), write_len)
                            return
                        }
                    }()
                }
            }(loop_idx)
            loop_idx += 1
        }
    }

}
