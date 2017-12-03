FROM golang:latest

RUN go get github.com/gorilla/mux
RUN go get gopkg.in/mgo.v2
RUN go get github.com/bradfitz/slice

# To use this as base for data-service build:
# docker build -t data-service/golang .