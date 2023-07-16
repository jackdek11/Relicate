package main

import "github.com/gofiber/websocket/v2"
import "sync"

type WebSocketConnections struct {
	connections []*websocket.Conn
	mu          sync.Mutex
}