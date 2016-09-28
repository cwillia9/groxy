package main

import (
	"github.com/cwillia9/groxy"
	"net/http"
	"io"
)


type serd struct {}

func (s *serd)Serialize(r *http.Request) (error, []byte) {
	val := r.FormValue("val")
	return nil, []byte(val)
}

func (s *serd)Deserialize(w http.ResponseWriter, b []byte) (error) {
	s := string(b)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8") // normal header
	w.WriteHeader(http.StatusOK)

	io.WriteString(w, s)

	return nil
}




func main() {
	ctx = groxy.NewKafkaContext(cfg)

	mux := http.NewServeMux()
	mux.HandleFunc("/", groxy.NewHandler(serd))
	http.ListenAndServe(":8000", mux)

}