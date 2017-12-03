package Queue

import (
	"fmt"
	"time"
	"net/http"
)

type ThrottledRequest struct {
  DelayedRequest *http.Request
  ReturnChannel chan *http.Request
}

type Queue struct {
	Chan chan ThrottledRequest
}

func MakeQueue (limit int) Queue {
	return Queue{Chan: make(chan ThrottledRequest, limit)}
}

// Add http requests to queue
func (q *Queue) ProcessRequest(request *http.Request) (*http.Response, error) {
	client := &http.Client{}
  ch := make(chan *http.Request, 1)
  q.Chan <- ThrottledRequest{ request, ch }
  requestPostDelay := <- ch
  return client.Do(requestPostDelay)
}

// Pulls from the request queue every [delay] milliseconds
func (q *Queue) RunQueue(delay time.Duration) {
  ticker := time.NewTicker(delay * time.Millisecond)
  quit := make(chan struct{})
  go func() {
    for {
      select {
        case <- ticker.C:
          select {
            case tr, ok := <- q.Chan:
               if ok {
                 tr.ReturnChannel <- tr.DelayedRequest
               } else {
                 fmt.Println("Channel closed!")
               }
            default:
          }
        case <- quit:
           ticker.Stop()
           return
      }
    }
  }()
}