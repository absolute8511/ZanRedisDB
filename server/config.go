package server

type ServerConfig struct {
	BindInterface string
	RedisAPIPort  int
	HTTPAPIPort   int
	DataDir       string
	EngType       string
}
