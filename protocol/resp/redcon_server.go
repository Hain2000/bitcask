package resp

import (
	"github.com/Hain2000/bitcask/redis"
	"github.com/tidwall/redcon"
)

type Server struct {
	handler *Handler
}

func NewServer(store *redis.DataStructure) *Server {
	return &Server{handler: NewHandler(store)}
}

func (s *Server) Start(addr string) error {
	return redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.handler.HandleCommand(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			// 认证逻辑（可选）
			return true
		},
		func(conn redcon.Conn, err error) {
			// 连接关闭处理
		},
	)
}
