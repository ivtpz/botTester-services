package Routes

import (
	"fmt"
	"log"
	"time"
	"math"
	"net/http"
	"strconv"
	"io/ioutil"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/bradfitz/slice"
	"github.com/ivtpz/data-service/Queue"
	"github.com/ivtpz/data-service/Mongo"
)

type Handler struct {
	Db *Mongo.DataStore
	Queue *Queue.Queue
}

func (h *Handler) historicDataHelper(market string, start string, end string, granularity string) []Mongo.Candle {
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
  resp, err := h.Queue.ProcessRequest(req)

  if err != nil {
		panic(err)
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		var arr []Mongo.Candle
    json.Unmarshal(body, &arr)
    if len(arr) > 1 {
      if arr[0].Time > arr[1].Time {
        slice.Sort(arr, func (i, j int) bool {
          return arr[i].Time < arr[j].Time;
        })
      }
    }
    return arr
  }
}

func createEmptyCandle(time int, baseCandle Mongo.Candle) Mongo.Candle {
  empty := Mongo.Candle{ time, baseCandle.Open, baseCandle.Close, baseCandle.Low, baseCandle.High, 0 }
  return empty
}

func (h *Handler) addHistoricMarketData(market string, candles []Mongo.Candle, granNum int64, start int) {
  if (len(candles) == 0) {
    fmt.Println("No data to insert\n")
  } else {
    var lastCandle Mongo.Candle
    // TODO: fix this code duplication
    var startingIndex int
    if candles[0].Time < candles[len(candles) - 1].Time {
      startingIndex = 0
    } else {
      startingIndex = len(candles) - 1
    }
    if start != candles[startingIndex].Time {
      numMissing := ((candles[startingIndex].Time - start) / int(granNum))
      if numMissing > 0 {
        fmt.Printf("Missing %d from start\n", numMissing)
      }
      for i := 0; i < numMissing; i++ {
        time := start + i * int(granNum)
				empty := createEmptyCandle(time, candles[startingIndex])
				go h.Db.AddCandle(market, empty)
        fmt.Printf("Added empty candle for %d\n", time)
      }
    }
    for i, candle := range candles {
      // TODO: This really needs testing
      difference := math.Abs(float64(candle.Time) - float64(lastCandle.Time))
      if (i != 0 && int64(difference) != granNum) {
        numMissing := (int(difference) / int(granNum)) - 1
        // fmt.Printf("Missing %d candles \n", numMissing)
        for i := 1; i <= numMissing; i++ {
          time := lastCandle.Time + i * int(granNum)
          empty := createEmptyCandle(time, lastCandle)
          go h.Db.AddCandle(market, empty)
          fmt.Printf("Added empty candle for %d\n", time)
        }
      }
      go h.Db.AddCandle(market, candle)
      lastCandle = candle
    }
    fmt.Printf("Finished inserting data into %s upto time %d\n", market, candles[0].Time)
  }
}

// Recursive loop to get all data from start point until current time
func (h *Handler) populateDBUntilNow(market string, start int64, granNum int64, granularity string) {
  if start < time.Now().Unix() - granNum {
    endUnix := start + granNum * 199
    startISO := time.Unix(start, 0).Format(time.RFC3339)
    endISO := time.Unix(endUnix, 0).Format(time.RFC3339)
    fmt.Println("going from " + startISO + " to " + endISO + "\n")
    candleData := h.historicDataHelper(market, startISO, endISO, granularity)
    go h.addHistoricMarketData(market + "-" + granularity, candleData, granNum, int(start))
    go h.populateDBUntilNow(market, endUnix + granNum, granNum, granularity)
  }
}

// Returns api data for a given range and stores it in DB - assumes that start and end are within limit of what the api can yield
func (h *Handler) populateDBAndGiveResult(market string, start int64, end int64, granNum int64, granularity string) ([]Mongo.Candle) {
  startISO := time.Unix(start, 0).Format(time.RFC3339)
  endISO := time.Unix(end, 0).Format(time.RFC3339)
  fmt.Println("going from " + startISO + " to " + endISO + " for market " + market + "\n")
  candleData := h.historicDataHelper(market, startISO, endISO, granularity)
  go h.addHistoricMarketData(market + "-" + granularity, candleData, granNum, int(start))
  return candleData
}

func (h *Handler) PopulateHandler(w http.ResponseWriter, r *http.Request) {
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
	go h.populateDBUntilNow(market, startTime, granNum, granularity)
	fmt.Fprintf(w,
		"You have asked for %d sections of data, from %d to %d for market %s. This will take at least %d seconds to process.",
		sections, startTime, currentTime, market, sections / 190) 
}

func (h *Handler) GetData(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    tableName := vars["market"] + "-" + vars["granularity"]
    min, _ := strconv.Atoi(vars["start"])
    max, _ := strconv.Atoi(vars["end"])
    stepSize, _ := strconv.Atoi(vars["granularity"])

    w.Header().Set("Connection", "Keep-Alive")
    w.Header().Set("Content-Type", "application/json")
    
		var finalResult []Mongo.Candle
		var currentResult []Mongo.Candle
    
    for i := min; i <= max; i += stepSize * 199 {
      currentResult = h.Db.FindCandles(tableName, i, i + stepSize * 200)
      if len(currentResult) < 198 { // Should this be 199 ?
        fmt.Printf("Only have %d results\n", len(currentResult))
        currentResult = h.populateDBAndGiveResult(vars["market"], int64(i + stepSize), int64(i + stepSize * 199), int64(stepSize), vars["granularity"])
			}
			for _, candle := range currentResult {
				finalResult = append(finalResult, candle)
			}
    }
    json.NewEncoder(w).Encode(finalResult)
}

func Log(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
    handler.ServeHTTP(w, r)
	})
}