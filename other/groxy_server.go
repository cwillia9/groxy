package groxy_server


struct Config {
	port int
}


interface GroxyHandler {
	Servialize(string) string
	Deserialize(string) string
}

stuct GroxyServer {
	handlers []
}





func NewGroxyHandler()