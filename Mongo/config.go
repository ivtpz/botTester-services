package Mongo

import (
  "fmt"
  "encoding/json"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type DataStore struct {
	Session *mgo.Session
}

type Candle struct {
	Time   int     `json:"time" bson:"time"`
	Open   float64 `json:"open" bson:"open"`
	Close  float64 `json:"close" bson:"close"`
	Low    float64 `json:"low" bson:"low"`
	High   float64 `json:"high" bson:"high"`
	Volume float64 `json:"volume" bson:"volume"`
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

func (ds *DataStore) FindCandles(tableName string, start int, end int) ([]Candle) {
  s := ds.Session.Copy()
  defer s.Close()
  c := s.DB("gdax_market_history").C(tableName)
  var result []Candle
  err := c.Find(bson.M{"time": bson.M{"$gt": start, "$lt": end, }}).All(&result)
  if err != nil {
    panic(err)
  }
  return result
}

func (ds *DataStore) AddCandle(tableName string, candle Candle) {
  s := ds.Session.Copy()
  defer s.Close()
  c := s.DB("gdax_market_history").C(tableName)
  err := c.Insert(candle)
  if err != nil && !mgo.IsDup(err) {
    panic(err)
  }
}

func (ds *DataStore) EnsureIndex() {
	session := ds.Session.Copy()
	defer session.Close()
  
  markets := [2]string{"ETH-USD-10", "ETH-USD-300"}
  for _, market := range markets {
		fmt.Printf("Checking indexes for collection %s", market)
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
