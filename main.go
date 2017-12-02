package main

import (
      "net/http"
      "log"
      "fmt"
      "time"
      "math"
      "io/ioutil"
      "encoding/json"
      "strconv"
      "github.com/gorilla/mux"
      "gopkg.in/mgo.v2"
      "gopkg.in/mgo.v2/bson"
 )

func routesList(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "text/html")
  fmt.Fprint(w, `
    <h2>Routes</h2>
    <p><em>'GET' /data/{market}</em> - current data from market</p>  
  `)
}

// Queueing functions

type ThrottledRequest struct {
  DelayedRequest *http.Request
  ReturnChannel chan *http.Request
}

var RequestQueue = make(chan ThrottledRequest, 100)

// Pulls from the request queue every [delay] milliseconds
func runQueue(delay time.Duration) {
  ticker := time.NewTicker(delay * time.Millisecond)
  quit := make(chan struct{})
  go func() {
    for {
      select {
        case <- ticker.C:
          select {
            case tr, ok := <- RequestQueue:
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

func processApiRequest(request *http.Request) (*http.Response, error) {
  client := &http.Client{}
  ch := make(chan *http.Request, 1)
  RequestQueue <- ThrottledRequest{ request, ch }
  requestPostDelay := <- ch
  return client.Do(requestPostDelay)

}

type Candle struct {
	Time   int     `json:"time" bson:"time"`
	Open   float64 `json:"open" bson:"open"`
	Close  float64 `json:"close" bson:"close"`
	Low    float64 `json:"low" bson:"low"`
	High   float64 `json:"high" bson:"high"`
	Volume float64 `json:"volume" bson:"volume"`
}

type EmptyResponse struct {
  Message string `json:"message,omitempty"`
}

func (cndle *Candle) UnmarshalJSON(data []byte) error {
	var candleData []float64
	err := json.Unmarshal(data, &candleData)
	if err != nil {
		return err
	}
	cndle.Time = int(candleData[0])
	cndle.Low = candleData[3]
	cndle.High = candleData[4]
	cndle.Open = candleData[1]
	cndle.Close = candleData[2]
	cndle.Volume = candleData[5]
	return nil
}

func getDataHelper(mpair string) ([]Candle, EmptyResponse) {
	resp, err := http.Get("https://api.gdax.com/products/" + mpair + "/candles")
	if err != nil {
		log.Fatal(err)
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		var arr []Candle
		err = json.Unmarshal(body, &arr)
		if err != nil {
      var msg EmptyResponse
      json.Unmarshal(body, &msg)
      return nil, msg
		} else {
			return arr, EmptyResponse{}
		}
  }
  return nil, EmptyResponse{}
}

func getCurrentDataHandler(w http.ResponseWriter, r *http.Request) {
  param := mux.Vars(r)["market"]
  fmt.Println("Contacting GDAX for market " + param)
  theData, noData := getDataHelper(param)
  w.Header().Set("Content-Type", "application/json")
  if theData != nil {
    json.NewEncoder(w).Encode(theData)
  } else {
    json.NewEncoder(w).Encode(noData)
  }
}

func historicDataHelper(market string, start string, end string, granularity string) []Candle {
  req, err := http.NewRequest("GET", "https://api.gdax.com/products/" + market + "/candles", nil)
  if err != nil {
    panic(err)
  }
  q := req.URL.Query()
  q.Add("start", start)
  q.Add("end", end)
  q.Add("granularity", granularity)
  req.URL.RawQuery = q.Encode()

  // Send request to queue
  resp, err := processApiRequest(req)

  if err != nil {
		panic(err)
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		var arr []Candle
    json.Unmarshal(body, &arr)
    return arr
  }
}

func createEmptyCandle(time int, baseCandle Candle) Candle {
  empty := Candle{ time, baseCandle.Open, baseCandle.Close, baseCandle.Low, baseCandle.High, 0 }
  return empty
}

func addHistoricMarketData(s *mgo.Session, market string, candles []Candle, granNum int64, start int) {
  session := s.Copy()
  defer session.Close()

  // TODO: handle multiple exchanges
  c := session.DB("gdax_market_history").C(market)

  if (len(candles) == 0) {
    fmt.Println("No data to insert\n")
  } else {
    var lastCandle Candle
    // TODO: fix this code duplication
    var startingIndex int
    if candles[0].Time < candles[len(candles) - 1].Time {
      startingIndex = 0
    } else {
      startingIndex = len(candles) - 1
    }
    if start != candles[startingIndex].Time {
      numMissing := ((candles[startingIndex].Time - start) / int(granNum))
      fmt.Printf("Missing %d from start", numMissing)
      for i := 0; i < numMissing; i++ {
        time := start + i * int(granNum)
        empty := createEmptyCandle(time, candles[startingIndex])
        err := c.Insert(empty)
        if err != nil {
          if !mgo.IsDup(err) {
            panic(err)
          }
        }
        fmt.Printf("Added empty candle for %d\n", time)
      }
    }
    for i, candle := range candles {
      // TODO: This really needs testing
      difference := math.Abs(float64(candle.Time) - float64(lastCandle.Time))
      if (i != 0 && int64(difference) != granNum) {
        numMissing := (int(difference) / int(granNum)) - 1
        fmt.Printf("Missing %d candles \n", numMissing)
        for i := 1; i <= numMissing; i++ {
          time := lastCandle.Time + i * int(granNum)
          empty := createEmptyCandle(time, lastCandle)
          err := c.Insert(empty)
          if err != nil {
            if !mgo.IsDup(err) {
              panic(err)
            }
          }
          fmt.Printf("Added empty candle for %d\n", time)
        }
      }
      err := c.Insert(candle)
      if err != nil {
        if !mgo.IsDup(err) {
          fmt.Printf("Did not insert into %s at time %d\n", market, candle.Time)
        }
      }
      lastCandle = candle
    }
    fmt.Printf("Finished inserting data into %s upto time %d\n", market, candles[0].Time)
  }
}

// Recursive loop to get all data from start point until current time
func populateDBUntilNow(s *mgo.Session, market string, start int64, granNum int64, granularity string) {
  if start < time.Now().Unix() - granNum {
    endUnix := start + granNum * 199
    startISO := time.Unix(start, 0).Format(time.RFC3339)
    endISO := time.Unix(endUnix, 0).Format(time.RFC3339)
    fmt.Println("going from " + startISO + " to " + endISO + "\n")
    candleData := historicDataHelper(market, startISO, endISO, granularity)
    go addHistoricMarketData(s, market + "-" + granularity, candleData, granNum, int(start))
    go populateDBUntilNow(s, market, endUnix + granNum, granNum, granularity)
  }
}

// Returns api data for a given range and stores it in DB - assumes that start and end are within limit of what the api can yield
func populateDBAndGiveResult(s *mgo.Session, market string, start int64, end int64, granNum int64, granularity string) ([]Candle) {
  startISO := time.Unix(start, 0).Format(time.RFC3339)
  endISO := time.Unix(end, 0).Format(time.RFC3339)
  fmt.Println("going from " + startISO + " to " + endISO + " for market " + market + "\n")
  candleData := historicDataHelper(market, startISO, endISO, granularity)
  go addHistoricMarketData(s, market + "-" + granularity, candleData, granNum, int(start))
  return candleData
}

func getData(s *mgo.Session) func(w http.ResponseWriter, r *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {
    // TODO: write this function, test empty data inserter
    vars := mux.Vars(r)
    // Starting point: pull from mongodb
    session := s.Copy()
    defer session.Close()

    tableName := vars["market"] + "-" + vars["granularity"]
    c := session.DB("gdax_market_history").C(tableName)

    
    min, _ := strconv.Atoi(vars["start"])
    max, _ := strconv.Atoi(vars["end"])
    stepSize, _ := strconv.Atoi(vars["granularity"])
    
    for i := min; i <= max; i += stepSize * 199 {
      // TODO: handle one off errors from < vs <=
      var dbResults []Candle
      err := c.Find(bson.M{ "time": bson.M{ "$gt": i, "$lt": i + stepSize * 200 } }).All(&dbResults)
      if err != nil {
        panic(err)
      }
      if len(dbResults) != 199 {
        result := populateDBAndGiveResult(s, vars["market"], int64(i + stepSize), int64(i + stepSize * 199), int64(stepSize), vars["granularity"])
        json.NewEncoder(w).Encode(result)
      } else {
        json.NewEncoder(w).Encode(dbResults)
      }
      if f, ok := w.(http.Flusher); ok {
        f.Flush()
      }
    }
  }
}

func populateHandler(s *mgo.Session) func(w http.ResponseWriter, r *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    market := vars["market"]
    start := vars["start"]
    startTime, err := strconv.ParseInt(start, 10, 64)
    if err != nil {
      fmt.Println(err)
    }
    granularity := vars["granularity"]
    granNum, err2 := strconv.ParseInt(granularity, 10, 64)
    if err2 != nil {
      fmt.Println(err2)
    }
    currentTime := time.Now().Unix()
    sections := (currentTime - startTime) / granNum
    go populateDBUntilNow(s, market, startTime, granNum, granularity)
    fmt.Fprintf(w,
      "You have asked for %d sections of data, from %d to %d for market %s. This will take at least %d seconds to process.",
      sections, startTime, currentTime, market, sections / 190)
  }
}

func ensureIndex(s *mgo.Session) {
  session := s.Copy()
  defer session.Close()
  
  markets := [1]string{"ETH-USD-10"}
  for _, market := range markets {
    c := session.DB("gdax_market_history").C(market)

    index := mgo.Index{
      Key:        []string{"time"},
      Unique:     true,
      DropDups:   true,
      Background: true,
      Sparse:     true,
    }

    err := c.EnsureIndex(index)
    if err != nil {
      panic(err)
    }
  }
}

func Log(handler http.Handler) http.Handler {
  return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
    handler.ServeHTTP(w, r)
  })
}

 func main() {
    session, err := mgo.Dial("mongodb://mongo-0.mongo,mongo-1.mongo:27017")
    if err != nil {
      panic(err)
    }
    fmt.Println("Connected to mongo cluster")
    defer session.Close()
    session.SetMode(mgo.Monotonic, true)
    ensureIndex(session)

    runQueue(400)

    marketRe := "[A-Z]{3}-[A-Z]{3}"
    unixRe := "[0-9]{10}"
    granRe := "[0-9]{2,4}"

    router := mux.NewRouter().StrictSlash(true)
    router.HandleFunc("/", routesList)
    router.HandleFunc("/api/current/{market}", getCurrentDataHandler).Methods("GET")
    router.HandleFunc(
      fmt.Sprintf("/api/populate-db/{market:%s}/{start:%s}/{granularity:%s}", marketRe, unixRe, granRe),
      populateHandler(session),
    ).Methods("GET")
    router.HandleFunc(
      fmt.Sprintf("/api/history/{market:%s}/{start:%s}/{end:%s}/{granularity:%s}", marketRe, unixRe, unixRe, granRe),
      getData(session),
    ).Methods("GET")
    log.Fatal(http.ListenAndServe(":8086", Log(router)))
 }
