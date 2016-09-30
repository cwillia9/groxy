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

type groxyHandler struct {
	ctx   *Context
	serde SerDe
}

func NewHandler(ctx *Context, serde SerDe) http.Handler {
	return &groxyHandler{
		ctx:   ctx,
		serde: serde,
	}
}

func (gh *groxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serialized, err := gh.serde.Serialize(r)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		Logger.Err("Value serialization failed:", err)
		return
	}

	respchan := make(chan []byte)
	defer close(respchan)

	key, err := gh.serde.Key(r)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		Logger.Err("Key serialization failed:", err)
		return
	}

	err = gh.ctx.Produce(respchan, key, serialized)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		Logger.Err("Failed to produce message:", err)
		return
	}

	timeout := make(chan int)
	go timeoutClock(gh.ctx.timeout, timeout)
	defer close(timeout)

	select {
	case resp := <-respchan:
		err := gh.serde.Deserialize(resp, w)
		if err != nil {
			Logger.Err("Deserialize failed:", err)
		}
		return
	case <-timeout:
		http.Error(w, "Timeout", http.StatusGatewayTimeout)
		Logger.Err("Timedout before responding")
		return
	}
}

func timeoutClock(ms int, timeout chan int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	timeout <- 1
}
