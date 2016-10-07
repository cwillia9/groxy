package groxy

import (
	"net/http"
	"time"
)

type SerDe interface {
	Serialize(*http.Request) ([]byte, error)
	Deserialize([]byte, http.ResponseWriter) error
	Key(*http.Request) ([]byte, error)
}

type Decider interface {
	// Decide should be a blocking function which decides
	// which of potentially several responses to actually
	// respond back to the client with. The decision will be inferred
	// by the return value of the byte slice and error.
	Decide(respchan <-chan []byte, timeout <-chan int) ([]byte, error)
}

type FifoDecider struct{}

func (f *FifoDecider) Decide(respchan <-chan []byte, timeout <-chan int) ([]byte, error) {
	select {
	case resp := <-respchan:
		return resp, nil
	case <-timeout:
		return nil, ErrTimeout
	}
}

type LifoDecider struct {
	resp         []byte
	receivedResp bool
}

func (d *LifoDecider) Decide(respchan <-chan []byte, timeout <-chan int) ([]byte, error) {
	for {
		select {
		case r := <-respchan:
			d.resp = r
			d.receivedResp = true
		case <-timeout:
			if d.resp != nil {
				return d.resp, nil
			}
			if d.resp == nil && d.receivedResp {
				return nil, nil
			}
			if !d.receivedResp {
				return nil, ErrTimeout
			}
		}
	}
}

type groxyHandler struct {
	ctx     *Context
	serde   SerDe
	decider Decider
}

func NewFifoHandler(ctx *Context, serde SerDe) http.Handler {
	return &groxyHandler{
		ctx:     ctx,
		serde:   serde,
		decider: &FifoDecider{},
	}
}

func NewLifoHandler(ctx *Context, serde SerDe) http.Handler {
	return &groxyHandler{
		ctx:     ctx,
		serde:   serde,
		decider: &LifoDecider{},
	}
}

func NewCustomHandler(ctx *Context, serde SerDe, d Decider) http.Handler {
	return &groxyHandler{
		ctx:     ctx,
		serde:   serde,
		decider: d,
	}
}

func (gh *groxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serialized, err := gh.serde.Serialize(r)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		Logger.Err("Value serialization failed:", err)
		return
	}

	respchan := make(chan []byte, 1)

	key, err := gh.serde.Key(r)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		Logger.Err("Key serialization failed:", err)
		return
	}

	gh.ctx.Input <- &Message{
		RespChan: respchan,
		Key:      key,
		Value:    serialized,
	}

	timeout := make(chan int, 1)
	go timeoutClock(gh.ctx.timeout, timeout)

	resp, err := gh.decider.Decide(respchan, timeout)
	if err != nil {
		if err == ErrTimeout {
			http.Error(w, "Timeout", http.StatusGatewayTimeout)
			Logger.Err("Timedout before responding")
			return
		}
		http.Error(w, "error", http.StatusInternalServerError)
		Logger.Err(err)
		return
	}

	err = gh.serde.Deserialize(resp, w)
	if err != nil {
		Logger.Err("Deserialize failed:", err)
		return
	}
	if resp == nil {

	}
}

func timeoutClock(ms int, timeout chan int) {
	defer func() {
		_ = recover()
	}()
	time.Sleep(time.Duration(ms) * time.Millisecond)
	timeout <- 1
}
