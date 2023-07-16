FROM golang:1.18-alpine

WORKDIR /code

COPY src/ /code/

COPY go.mod /code/go.mod

COPY go.sum /code/go.sum

RUN go mod download

RUN go build -o relicate .
