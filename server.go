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

// 广播用户发送的聊天消息，格式：[addr]name:msg
func (this *Server) Broadcast(user *User, msg string) {
	sendMsg := "[" + user.Addr + "]" + user.Name + ":" + msg
	this.Message <- sendMsg
}

// 广播系统通知（上线、下线等），格式：[System] msg
// 聊天消息总是以 [addr] 开头，用户输入的内容不会被当成系统通知
func (this *Server) Notify(msg string) {
	this.Message <- "[System] " + msg
}

func (this *Server) Handler(conn net.Conn) {
	defer conn.Close() // Handler 会阻塞到客户端断开才返回，此时再关闭连接

	user := NewUser(conn)
	// 用户上线
	this.mapLock.Lock()
	this.OnlineMap[user.Name] = user
	this.mapLock.Unlock()

	this.Notify(user.Name + " is online")

	// 阻塞读取客户端消息，按行广播给所有在线用户
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text() // 已去掉行尾的 \n 和 \r
		if msg == "" {
			continue
		}
		this.Broadcast(user, msg)
	}
	// Scan 返回 false：客户端正常断开（EOF，Err 为 nil）或读取出错
	if err := scanner.Err(); err != nil {
		fmt.Println("conn read err:", err)
	}

	this.Offline(user)
}

// 用户下线：先从在线列表移除，再关闭 channel，最后通知其他用户。
// 顺序不能反：ListenMessage 持锁向 OnlineMap 里每个用户的 C 发送，
// 删除后再 close，才不会向已关闭的 channel 发送而 panic。
func (this *Server) Offline(user *User) {
	this.mapLock.Lock()
	delete(this.OnlineMap, user.Name)
	this.mapLock.Unlock()

	close(user.C) // 让 User.ListenMessage 的 range 循环结束，goroutine 退出

	this.Notify(user.Name + " is offline")
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
