package main

import "net"

type User struct {
	Name   string
	Addr   string
	C      chan string
	conn   net.Conn
	server *Server // 用户所属的服务器
}

// 创建一个用户的 API
func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Name:   userAddr,
		Addr:   userAddr,
		C:      make(chan string),
		conn:   conn,
		server: server,
	}
	go user.ListenMessage()
	return user
}

// 用户上线的方法
func (u *User) Online() {
	u.server.addUser(u)
	u.server.Notify(u.Name + " is online")
}

// 用户下线：先从在线列表移除，再关闭 channel，最后通知其他用户。
// 顺序不能反：ListenMessage 持锁向 OnlineMap 里每个用户的 C 发送，
// 删除后再 close，才不会向已关闭的 channel 发送而 panic。
func (u *User) Offline() {
	u.server.removeUser(u)
	close(u.C) // 让 User.ListenMessage 的 range 循环结束，goroutine 退出

	u.server.Notify(u.Name + " is offline")
}

// 处理当前用户发来的一行消息
func (u *User) DoMessage(msg string) {
	if msg == "" {
		return
	}
	u.server.Broadcast(u, msg)
}

// 监听当前用户的 channel 方法
func (u *User) ListenMessage() {
	for msg := range u.C { // C 被 close 后循环自动结束
		u.conn.Write([]byte(msg + "\n"))
	}
}
