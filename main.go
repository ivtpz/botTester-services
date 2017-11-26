package main

import (
      "net/http"
      "log"
      "fmt"
      "io/ioutil"
      "encoding/json"
      "github.com/gorilla/mux"
      "gopkg.in/mgo.v2"
      // "gopkg.in/mgo.v2/bson"
 )

 type heartbeatResponse struct {
   Status string `json:"status"`
   Code int `json:"code"`
 }

 func heartbeat(w http.ResponseWriter, r *http.Request) {
   json.NewEncoder(w).Encode(heartbeatResponse{ Status: "Routes:\n" + "'GET' /data/{market} - current data from market", Code: 200 })
 }

 type Candle struct {
	Time   int     `json:"time"`
	Open   float64 `json:"open"`
	Close  float64 `json:"close"`
	Low    float64 `json:"low"`
	High   float64 `json:"high"`
	Volume float64 `json:"volume"`
}

func (cndle *Candle) UnmarshalJSON(data []byte) error {
	var stuff []float64
	err := json.Unmarshal(data, &stuff)
	if err != nil {
		return err
	}
	cndle.Time = int(stuff[0])
	cndle.Low = stuff[3]
	cndle.High = stuff[4]
	cndle.Open = stuff[1]
	cndle.Close = stuff[2]
	cndle.Volume = stuff[5]
	return nil
}

func getDataHelper(mpair string) []Candle {
	resp, err := http.Get("https://api.gdax.com/products/" + mpair + "/candles")
	if err != nil {
		log.Fatal(err)
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		var arr []Candle
		err = json.Unmarshal(body, &arr)
		if err != nil {
			log.Fatal(err)
		} else {
			return arr
		}
	}
	return nil
}

func getDataHandler(w http.ResponseWriter, r *http.Request) {
	param := mux.Vars(r)["market"]
	theData := getDataHelper(param)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(theData)
}

 func main() {
    session, err := mgo.Dial("mongodb://mongo-0.mongo,mongo-1.mongo:27017")
    if err != nil {
      panic(err)
    }
    fmt.Println("Connected to mongo cluster")
    defer session.Close()
    // c := session.DB("test").C("people")
    // err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},
    //         &Person{"Cla", "+55 53 8402 8510"})
    // if err != nil {
    //         log.Fatal(err)
    // }

    // result := Person{}
    // err = c.Find(bson.M{"name": "Ale"}).One(&result)
    // if err != nil {
    //         log.Fatal(err)
    // }

    // fmt.Println("Phone:", result.Phone)

    router := mux.NewRouter().StrictSlash(true)
    router.HandleFunc("/", heartbeat)
    router.HandleFunc("/data/{market}", getDataHandler).Methods("GET")
    log.Fatal(http.ListenAndServe(":8086", router))
 }
