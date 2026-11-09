package main

import (
	"bufio"
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

func (s *Server) ListenMessage() {
	for {
		msg := <-s.Message
		s.mapLock.RLock()
		for _, cli := range s.OnlineMap {
			cli.C <- msg
		}
		s.mapLock.RUnlock()
	}
}

// 广播用户发送的聊天消息，格式：[addr]name:msg
func (s *Server) Broadcast(user *User, msg string) {
	sendMsg := "[" + user.Addr + "] " + user.Name + ": " + msg
	s.Message <- sendMsg
}

// 广播系统通知（上线、下线等），格式：[System] msg
// 聊天消息总是以 [addr] 开头，用户输入的内容不会被当成系统通知
func (s *Server) Notify(msg string) {
	s.Message <- "[System] " + msg
}

func (s *Server) addUser(u *User) {
	s.mapLock.Lock()
	s.OnlineMap[u.Name] = u
	s.mapLock.Unlock()
}

func (s *Server) removeUser(u *User) {
	s.mapLock.Lock()
	delete(s.OnlineMap, u.Name)
	s.mapLock.Unlock()
}

func (s *Server) Handler(conn net.Conn) {
	defer conn.Close() // Handler 会阻塞到客户端断开才返回，此时再关闭连接

	user := NewUser(conn, s)

	user.Online()

	// 阻塞读取客户端消息，按行广播给所有在线用户
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		user.DoMessage(scanner.Text()) // 已去掉行尾的 \n 和 \r
	}
	// Scan 返回 false：客户端正常断开（EOF，Err 为 nil）或读取出错
	if err := scanner.Err(); err != nil {
		fmt.Println("conn read err:", err)
	}

	user.Offline()
}

// 启动服务器接口
func (s *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Ip, s.Port))
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	defer listener.Close() // 函数结束时关闭监听

	go s.ListenMessage()

	for {
		conn, err := listener.Accept() // 阻塞，等待客户端连接
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return // 监听器已关闭，退出循环，避免空转
			}
			fmt.Println("listen.Accept err:", err)
			continue
		}
		go s.Handler(conn)
	}
}
