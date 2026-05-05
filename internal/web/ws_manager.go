package web

import (
	"encoding/json"
	"sync"
	"webBridgeBot/internal/logger"

	"github.com/gorilla/websocket"
)

type WSManager struct {
	clients     map[int64]map[string]*websocket.Conn
	clientsLock sync.RWMutex
	logger      *logger.Logger
}

func NewWSManager(log *logger.Logger) *WSManager {
	return &WSManager{
		clients: make(map[int64]map[string]*websocket.Conn),
		logger:  log,
	}
}

func (wm *WSManager) AddClient(chatID int64, connID string, conn *websocket.Conn) {
	wm.clientsLock.Lock()
	defer wm.clientsLock.Unlock()

	if wm.clients[chatID] == nil {
		wm.clients[chatID] = make(map[string]*websocket.Conn)
	}
	wm.clients[chatID][connID] = conn
	wm.logger.Printf("WebSocket client added for chat %d, conn %s", chatID, connID)
}

func (wm *WSManager) RemoveClient(chatID int64, connID string) {
	wm.clientsLock.Lock()
	defer wm.clientsLock.Unlock()

	if wm.clients[chatID] != nil {
		delete(wm.clients[chatID], connID)
		if len(wm.clients[chatID]) == 0 {
			delete(wm.clients, chatID)
		}
	}
	wm.logger.Printf("WebSocket client removed for chat %d, conn %s", chatID, connID)
}

// PublishMessage accepts map[string]interface{} for flexibility
func (wm *WSManager) PublishMessage(chatID int64, message map[string]interface{}) {
	wm.clientsLock.RLock()
	defer wm.clientsLock.RUnlock()

	clients, exists := wm.clients[chatID]
	if !exists {
		return
	}

	data, err := json.Marshal(message)
	if err != nil {
		wm.logger.Printf("Failed to marshal message: %v", err)
		return
	}

	for connID, conn := range clients {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			wm.logger.Printf("Failed to send message to conn %s: %v", connID, err)
			conn.Close()
		}
	}
}

func (wm *WSManager) GetClientCount(chatID int64) int {
	wm.clientsLock.RLock()
	defer wm.clientsLock.RUnlock()

	if clients, exists := wm.clients[chatID]; exists {
		return len(clients)
	}
	return 0
}
