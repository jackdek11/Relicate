package main

import (
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

func main() {
	app := fiber.New()

	// Create a WebSocketConnections instance to manage connections
	connections := &WebSocketConnections{
		connections: make([]*websocket.Conn, 0),
	}

	// WebSocket route
	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		// Add the new connection to the connections collection
		connections.mu.Lock()
		connections.connections = append(connections.connections, c)
		connections.mu.Unlock()

		// Handle WebSocket connection
		log.Println("New WebSocket connection")

		// Read messages from the client
		for {
			// Read message from the client
			msgType, msg, err := c.ReadMessage()
			if err != nil {
				log.Println("Error reading message:", err)
				break
			}

			// Broadcast the message to all connected clients
			broadcastMessage(connections, msgType, msg)
		}

		// Remove the connection from the connections collection
		connections.mu.Lock()
		for i, conn := range connections.connections {
			if conn == c {
				connections.connections = append(connections.connections[:i], connections.connections[i+1:]...)
				break
			}
		}
		connections.mu.Unlock()

		// Close the WebSocket connection
		log.Println("Closing WebSocket connection")
	}))

	// Start the server
	err := app.Listen(":3000")
	if err != nil {
		log.Fatal(err)
	}
}

// Broadcasts a message to all connected clients
func broadcastMessage(connections *WebSocketConnections, messageType int, message []byte) {
	connections.mu.Lock()
	defer connections.mu.Unlock()

	for _, conn := range connections.connections {
		err := conn.WriteMessage(messageType, message)
		if err != nil {
			log.Println("Error writing message:", err)
			continue
		}
	}
}