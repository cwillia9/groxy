package groxy

import (
	"log"
)

var Logger GroxyLogger = new(DefaultLogger)

type GroxyLogger interface {
	Debug(v ...interface{})
	Info(v ...interface{})
	Warn(v ...interface{})
	Err(v ...interface{})
	Crit(v ...interface{})
}

type DefaultLogger struct{}

func (d *DefaultLogger) Debug(v ...interface{}) {
	log.Println("[groxy] DEBUG:", v)
}
func (d *DefaultLogger) Info(v ...interface{}) {
	log.Println("[groxy] INFO:", v)
}
func (d *DefaultLogger) Warn(v ...interface{}) {
	log.Println("[groxy] WARN:", v)
}
func (d *DefaultLogger) Err(v ...interface{}) {
	log.Println("[groxy] ERROR:", v)
}
func (d *DefaultLogger) Crit(v ...interface{}) {
	log.Println("[groxy] CRITICAL:", v)
}