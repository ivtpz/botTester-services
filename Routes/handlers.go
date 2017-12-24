package Routes

import (
	"fmt"
	"log"
	"time"
  "math"
  "regexp"
  "strings"
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

func (h *Handler) poloniexHistoricDataHelper(market string, start string, end string, granularity string) []Mongo.Candle {
  fmt.Println("should fetch data from poloniex")
  req, err := http.NewRequest("GET", "https://poloniex.com/public", nil)
  if err != nil {
    panic(err)
  }
  re := regexp.MustCompile("-")
  currencyPair := re.ReplaceAllString(market, "_")
  q := req.URL.Query()
  q.Add("command", "returnChartData")
  q.Add("currencyPair", currencyPair)
  q.Add("start", start)
  q.Add("end", end)
  q.Add("period", granularity)
  req.URL.RawQuery = q.Encode()

  resp, err := h.Queue.ProcessRequest(req)

  if err != nil {
    panic(err)
  }
  defer resp.Body.Close()
  var body []struct {
    Date   int     `json:"date"`
    Open   float64 `json:"open"`
    Close  float64 `json:"close"`
    Low    float64 `json:"low"`
    High   float64 `json:"high"`
    Volume float64 `json:"volume"`
  }
  json.NewDecoder(resp.Body).Decode(&body)
  // body, _ := ioutil.ReadAll(resp.Body)
  fmt.Println(body)

  var test []Mongo.Candle
  test = make([]Mongo.Candle, 0, 0)
  return test
}

func (h *Handler) gdaxHistoricDataHelper(market string, start string, end string, granularity string) []Mongo.Candle {
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

func (h *Handler) addHistoricMarketData(dbName, market string, candles []Mongo.Candle, granNum int64, start int) {
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
				go h.Db.AddCandle(dbName, market, empty)
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
          go h.Db.AddCandle(dbName, market, empty)
          fmt.Printf("Added empty candle for %d\n", time)
        }
      }
      go h.Db.AddCandle(dbName, market, candle)
      lastCandle = candle
    }
    fmt.Printf("Finished inserting data into %s upto time %d\n", market, candles[0].Time)
  }
}

// Recursive loop to get all data from start point until current time
// NOTE, only works with GDAX at the moment
func (h *Handler) populateDBUntilNow(market string, start int64, granNum int64, granularity string) {
  if start < time.Now().Unix() - granNum {
    endUnix := start + granNum * 199
    startISO := time.Unix(start, 0).Format(time.RFC3339)
    endISO := time.Unix(endUnix, 0).Format(time.RFC3339)
    fmt.Println("going from " + startISO + " to " + endISO + "\n")
    candleData := h.gdaxHistoricDataHelper(market, startISO, endISO, granularity)
    go h.addHistoricMarketData("gdax_market_history", market + "-" + granularity, candleData, granNum, int(start))
    go h.populateDBUntilNow(market, endUnix + granNum, granNum, granularity)
  }
}

// Returns api data for a given range and stores it in DB - assumes that start and end are within limit of what the api can yield
func (h *Handler) populateDBAndGiveResult(dbName string, market string, start int64, end int64, granNum int64, granularity string) ([]Mongo.Candle) {
  startISO := time.Unix(start, 0).Format(time.RFC3339)
  endISO := time.Unix(end, 0).Format(time.RFC3339)
  dbNamePieces := strings.Split(dbName, "_")
  exchange := dbNamePieces[0]
  fmt.Println("going from " + startISO + " to " + endISO + " for market " + market + " on exchange " + exchange + "\n")
  var candleData []Mongo.Candle
  switch exchange {
  case "gdax":
    candleData = h.gdaxHistoricDataHelper(market, startISO, endISO, granularity)
  case "poloniex":
    candleData = h.poloniexHistoricDataHelper(market, startISO, endISO, granularity)
  }
  go h.addHistoricMarketData(dbName, market + "-" + granularity, candleData, granNum, int(start))
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
    dbName := vars["exchange"] + "_market_history"
    tableName := vars["market"] + "-" + vars["granularity"]
    min, _ := strconv.Atoi(vars["start"])
    max, _ := strconv.Atoi(vars["end"])
    stepSize, _ := strconv.Atoi(vars["granularity"])

    w.Header().Set("Connection", "Keep-Alive")
    w.Header().Set("Content-Type", "application/json")
    
		var finalResult []Mongo.Candle
		var currentResult []Mongo.Candle
    
    for i := min; i <= max; i += stepSize * 199 {
      currentResult = h.Db.FindCandles(dbName, tableName, i, i + stepSize * 200)
      if len(currentResult) < 198 { // Should this be 199 ?
        fmt.Printf("Only have %d results\n", len(currentResult))
        currentResult = h.populateDBAndGiveResult(dbName, vars["market"], int64(i + stepSize), int64(i + stepSize * 199), int64(stepSize), vars["granularity"])
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