package main

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

type Server struct {
	Ip   string
	Port int

	OnlineMap map[string]*User
	mapLock   sync.RWMutex

	Message chan string
}

// 创建一个服务器接口
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string),
	}
	return server
}

func (this *Server) ListenMessage() {
	for {
		msg := <-this.Message
		this.mapLock.Lock()
		for _, cli := range this.OnlineMap {
			cli.C <- msg
		}
		this.mapLock.Unlock()
	}
}

func (this *Server) Broadcast(user *User, msg string) {
	sendMsg := "[" + user.Addr + "]" + user.Name + ":" + msg
	this.Message <- sendMsg
}

func (this *Server) Handler(conn net.Conn) {
	defer conn.Close() // 处理结束时关闭连接，避免连接泄漏

	user := NewUser(conn)
	// 用户上线
	this.mapLock.Lock()
	this.OnlineMap[user.Name] = user
	this.mapLock.Unlock()

	this.Broadcast(user, "Online")
}

// 启动服务器接口
func (this *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.Ip, this.Port))
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	defer listener.Close() // 函数结束时关闭监听

	go this.ListenMessage()

	for {
		conn, err := listener.Accept() // 阻塞，等待客户端连接
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return // 监听器已关闭，退出循环，避免空转
			}
			fmt.Println("listen.Accept err:", err)
			continue
		}
		go this.Handler(conn)
	}
}
