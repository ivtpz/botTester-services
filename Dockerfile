FROM golang:latest as build
RUN mkdir /app
ADD . /app/
WORKDIR /app

RUN go get github.com/gorilla/mux
RUN go get gopkg.in/mgo.v2
RUN go build -o main .
EXPOSE 8086
CMD ["/app/main"]