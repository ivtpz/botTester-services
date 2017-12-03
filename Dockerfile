FROM data-service/golang:latest as build
ADD . /go/src/github.com/ivtpz/data-service/
WORKDIR /go/src/github.com/ivtpz/data-service

RUN cd Mongo && go install
RUN cd Queue && go install
RUN cd Routes && go install
RUN go build -o main .
EXPOSE 8086
CMD ["/go/src/github.com/ivtpz/data-service/main"]