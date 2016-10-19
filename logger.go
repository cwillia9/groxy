package groxy

import (
	"io"
	"log"
	"os"
)

type ErrLevel int

const (
	TEST ErrLevel = iota
	DEBUG
	INFO
	WARN
	ERROR
	CRITICAL
)

var Logger GroxyLogger = NewLogger(TEST, os.Stdout, os.Stdout)

type GroxyLogger interface {
	Test(v ...interface{})
	Debug(v ...interface{})
	Info(v ...interface{})
	Warn(v ...interface{})
	Err(v ...interface{})
}

type CustomLogger struct {
	TestLogger  *log.Logger
	DebugLogger *log.Logger
	InfoLogger  *log.Logger
	WarnLogger  *log.Logger
	ErrorLogger *log.Logger
}

func (l *CustomLogger) Test(v ...interface{}) {
	l.TestLogger.Println(v)
}

func (l *CustomLogger) Debug(v ...interface{}) {
	l.DebugLogger.Println(v)
}

func (l *CustomLogger) Info(v ...interface{}) {
	l.InfoLogger.Println(v)
}

func (l *CustomLogger) Warn(v ...interface{}) {
	l.WarnLogger.Println(v)
}

func (l *CustomLogger) Err(v ...interface{}) {
	l.ErrorLogger.Println(v)
}

// If you want to log to STDOUT for all errors that are of a severity of INFO or greater
// and to /dev/null with anything less severe you would call
// NewLogger(groxy.INFO, os.Stdout, ioutil.Discard)
func NewLogger(greaterThanIncluding ErrLevel, wGT io.Writer, wLT io.Writer) GroxyLogger {
	commonFlags := log.Ldate | log.Ltime | log.LUTC
	cl := new(CustomLogger)
	if greaterThanIncluding <= TEST {
		cl.TestLogger = log.New(wGT, "T:", 0)
	} else {
		cl.TestLogger = log.New(wLT, "T:", 0)
	}
	if greaterThanIncluding <= DEBUG {
		cl.DebugLogger = log.New(wGT, "[groxy] DEBUG: ", commonFlags)
	} else {
		cl.DebugLogger = log.New(wLT, "[groxy] DEBUG: ", commonFlags)
	}
	if greaterThanIncluding <= INFO {
		cl.InfoLogger = log.New(wGT, "[groxy] INFO: ", commonFlags)
	} else {
		cl.InfoLogger = log.New(wLT, "[groxy] INFO: ", commonFlags)
	}
	if greaterThanIncluding <= WARN {
		cl.WarnLogger = log.New(wGT, "[groxy] WARN: ", commonFlags)
	} else {
		cl.WarnLogger = log.New(wLT, "[groxy] WARN: ", commonFlags)
	}
	if greaterThanIncluding <= ERROR {
		cl.ErrorLogger = log.New(wGT, "[groxy] ERROR: ", commonFlags)
	} else {
		cl.ErrorLogger = log.New(wLT, "[groxy] ERROR: ", commonFlags)
	}
	return cl
}
