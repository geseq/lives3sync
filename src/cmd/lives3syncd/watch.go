package main

import (
	"gopkg.in/fsnotify.v0"
	"log"
	"time"
)

type Watcher struct {
	*fsnotify.Watcher
}

func NewWatcher(src string, out chan<- string) *Watcher {
	w := &Watcher{
		Watcher: fsnotify.NewWatcher(),
	}
	w.Watch(src)
	go func() {
		for ev := range w.Event {
			if ev.IsDelete() {
				return
			}
			out <- ev.Name
		}
	}()
	go func() {
		for err := range w.Error {
			log.Printf("fsnotify error: %s", err)
		}
	}()
	return w
}
