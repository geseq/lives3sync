package main

import (
	"log"

	"gopkg.in/fsnotify.v0"
)

type Watcher struct {
	*fsnotify.Watcher
}

// NewWatcher starts watching directory `src` and writes events to `out`
func NewWatcher(src string, out chan<- string) *Watcher {
	ww, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err.Error())
	}
	w := &Watcher{
		Watcher: ww,
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
