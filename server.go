package main

import (
	"fmt"
	"net"
)

type Server struct {
	Ip   string
	Port int
}

// 创建一个服务器接口
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:   ip,
		Port: port,
	}
	return server
}

func (this *Server) Handler(conn net.Conn) {
	fmt.Println("Connection successful")
}

// 启动服务器接口
func (this *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	defer listener.Close() // 函数结束时关闭监听

	for {
		conn, err := listener.Accept() // 阻塞，等待客户端连接
		if err != nil {
			fmt.Println("listen.Accept err:", err)
			continue
		}
		go this.Handler(conn)
	}
}
