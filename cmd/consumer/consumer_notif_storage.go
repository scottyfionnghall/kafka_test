package main

import (
	"kafka-notify/models"
	"sync"
)

type UserNotifications map[string][]models.Notification

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

func (ns *NotificationStore) Add(userID string,
	notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.data[userID] = append(ns.data[userID], notification)
}

func (ns *NotificationStore) Get(userID string) []models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[userID]
}
