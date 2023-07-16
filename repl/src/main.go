package main

import (
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

func main() {
	app := fiber.New()

	// WebSocket route
	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
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

			// Print the received message
			log.Printf("Received WebSocket message: %s", msg)

			// Write message back to the client
			err = c.WriteMessage(msgType, msg)
			if err != nil {
				log.Println("Error writing message:", err)
				break
			}
		}

		// Close the WebSocket connection
		log.Println("Closing WebSocket connection")
	}))

	// Start the server
	err := app.Listen(":3000")
	if err != nil {
		log.Fatal(err)
	}
}
