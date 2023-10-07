FROM golang:1.18-alpine

# Set and full working directory
WORKDIR /code

# Copy over requirements
COPY src/ /code/
COPY go.mod /code/go.mod
COPY go.sum /code/go.sum

# Download go libraries
RUN go mod download

# Build assets
RUN go build -o repl .
