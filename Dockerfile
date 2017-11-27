FROM data-service/golang:latest as build
RUN mkdir /app
ADD . /app/
WORKDIR /app

RUN go build -o main .
EXPOSE 8086
CMD ["/app/main"]