package main

import (
	"net"
	"strings"
)

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
	// "who"：查询当前在线用户，结果只发给自己，不广播
	if msg == "who" {
		u.server.mapLock.RLock()
		for _, user := range u.server.OnlineMap {
			onlineMsg := "[" + user.Addr + "] " + user.Name
			u.SendMsg(onlineMsg)
		}
		u.server.mapLock.RUnlock()
		return
	}
	// "rename|新名字"：修改用户名
	if newName, ok := strings.CutPrefix(msg, "rename|"); ok {
		u.Rename(strings.TrimSpace(newName))
		return
	}
	u.server.Broadcast(u, msg)
}

// 修改用户名。检查重名、换 key、改 Name 必须在同一把写锁内完成，
// 否则两个用户可能同时抢到同一个名字，或与其他 goroutine 并发读写 map 导致进程崩溃。
// Notify 要放在解锁之后：Server.ListenMessage 投递消息时要拿读锁，持写锁发送可能死锁。
func (u *User) Rename(newName string) {
	if newName == "" {
		u.SendMsg("Usage: rename|<new name>")
		return
	}
	u.server.mapLock.Lock()
	if _, ok := u.server.OnlineMap[newName]; ok {
		u.server.mapLock.Unlock()
		u.SendMsg("Name " + "\"" + newName + "\"" + " is already taken")
		return
	}
	oldName := u.Name
	delete(u.server.OnlineMap, oldName)
	u.Name = newName
	u.server.OnlineMap[newName] = u
	u.server.mapLock.Unlock()

	u.server.Notify(oldName + " renamed to " + newName)
}

// 只给当前用户发送消息
func (u *User) SendMsg(msg string) {
	u.C <- msg
}

// 监听当前用户的 channel 方法
func (u *User) ListenMessage() {
	for msg := range u.C { // C 被 close 后循环自动结束
		u.conn.Write([]byte(msg + "\n"))
	}
}
