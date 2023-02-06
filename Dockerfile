FROM golang:1.16-alpine

WORKDIR /app

COPY src/ /app/

COPY go.mod /app/

RUN go mod download

RUN go get github.com/gorilla/websocket

RUN cd `go list -f '{{.Dir}}' github.com/gorilla/websocket/examples/chat`

RUN go run *.go

CMD [ "./run" ]
