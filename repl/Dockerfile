FROM golang:1.18-alpine

# Add and set user
RUN addgroup -S replgroup
RUN adduser -S repl -G replgroup
USER repl

# Set and full working directory
WORKDIR /code

# Copy over requirements
COPY src/ /code/
COPY go.mod /code/go.mod
COPY go.sum /code/go.sum

# Download go libraries
RUN go mod download

# Set command / entrypoint
ENTRYPOINT go build -o repl .
