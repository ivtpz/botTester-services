package main

import (
      "net/http"
      "log"
      "fmt"
      "github.com/gorilla/mux"
      "github.com/ivtpz/data-service/Mongo"
      "github.com/ivtpz/data-service/Queue"
      "github.com/ivtpz/data-service/Routes"
      "gopkg.in/mgo.v2"
 )

 // TODO: UPDATE THIS
func routesList(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "text/html")
  fmt.Fprint(w, `
    <h2>Routes</h2>
    <p><em>'GET' /data/{market}</em> - current data from market</p>  
  `)
}

func main() {
    session, err := mgo.Dial("mongodb://mongo-0.mongo,mongo-1.mongo:27017")
    if err != nil {
      panic(err)
    }
    fmt.Println("Connected to mongo cluster")
    ds := &Mongo.DataStore{Session: session.Copy()}
    defer ds.Session.Close()
    ds.Session.SetMode(mgo.Monotonic, true)
    ds.EnsureIndex()
    
    queue := Queue.MakeQueue(100)
    queue.RunQueue(400)

    handlers := Routes.Handler{Db: ds, Queue: &queue}

    marketRe := "[A-Z]{3}-[A-Z]{3}"
    unixRe := "[0-9]{10}"
    granRe := "[0-9]{2,4}"

    router := mux.NewRouter().StrictSlash(true)
    router.HandleFunc("/", routesList)
    router.HandleFunc(
      fmt.Sprintf("/api/populate-db/{market:%s}/{start:%s}/{granularity:%s}", marketRe, unixRe, granRe),
      handlers.PopulateHandler,
    ).Methods("GET")
    router.HandleFunc(
      fmt.Sprintf("/api/history/{market:%s}/{start:%s}/{end:%s}/{granularity:%s}", marketRe, unixRe, unixRe, granRe),
      handlers.GetData,
    ).Methods("GET")
    log.Fatal(http.ListenAndServe(":8086", Routes.Log(router)))
 }
