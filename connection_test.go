/*
 * Copyright (c) 2020 Percipia
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Contributor(s):
 * Andrew Querol <aquerol@percipia.com>
 */
package eslgo

import (
	"bufio"
	"context"
	"github.com/percipia/eslgo/command"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConn_SendCommand(t *testing.T) {
	server, client := net.Pipe()
	connection := newConnection(client, false, DefaultOptions)
	defer connection.Close()
	defer server.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	serverReader := bufio.NewReader(server)
	defer serverReader.Discard(serverReader.Buffered())

	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		response, err := connection.SendCommand(ctx, command.Auth{
			Password: "test1234",
		})
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.True(t, response.IsOk())
		assert.Equal(t, "+OK Job-UUID: c7709e9c-1517-11dc-842a-d3a3942d3d63", response.GetHeader("Reply-Text"))
		wait.Done()
	}()

	// This is sorta lazy, we should be reading until the proper deliminator of \r\n\r\n
	incomingCommand, err := serverReader.ReadString('\r')
	assert.Nil(t, err)
	assert.Equal(t, "auth test1234\r", incomingCommand)

	_, err = server.Write([]byte("Content-Type: command/reply\r\nReply-Text: +OK Job-UUID: c7709e9c-1517-11dc-842a-d3a3942d3d63\r\n\r\n"))
	assert.Nil(t, err)
	wait.Wait()
}

func TestReceiveLoop_NoRaceOnEOF(t *testing.T) {
	server, client := net.Pipe()
	connection := newConnection(client, false, DefaultOptions)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		server.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		connection.Close()
	}()

	wg.Wait()
}

// TestReceiveLoop_ReadKeepaliveDeadLink 验证读保活 deadline 能判死半开连接：
// 服务端保持 socket 打开但停止发送（OS 层无 FIN/RST 的黑洞形态），
// ReadKeepalive 到期后 receiveLoop 必须退出并触发 onDisconnect。
// 无 deadline 时（上游 v1.5.2 行为）receiveLoop 永久阻塞 → 僵尸连接。
func TestReceiveLoop_ReadKeepaliveDeadLink(t *testing.T) {
	server, client := net.Pipe()

	disconnected := make(chan struct{})
	opts := DefaultInboundOptions
	opts.Password = "ClueCon"
	opts.ReadKeepalive = 200 * time.Millisecond
	opts.OnDisconnect = func() {
		close(disconnected)
	}

	connection := newConnection(client, false, opts.Options)
	go connection.disconnectLoop(opts.OnDisconnect)

	// auth request 使连接进入正常状态
	_, err := server.Write([]byte("Content-Type: auth/request\r\n\r\n"))
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// 服务端静默：不关闭连接、不再写任何字节。
	// 200ms 读保活到期 → read 返回 deadline exceeded → 判死。

	select {
	case <-disconnected:
		// 预期：读保活判死 + onDisconnect 触发
	case <-time.After(3 * time.Second):
		t.Fatal("静默打开的连接未在读保活期限内被判死（onDisconnect 未触发）")
	}

	connection.Close()
	server.Close()
	client.Close()
}

// TestReceiveLoop_ReadKeepaliveRefreshOnTraffic 验证正常流量下 deadline 持续刷新：
// 服务端每隔 100ms 发数据（短于 300ms 保活窗），1s 后连接仍存活不误杀。
func TestReceiveLoop_ReadKeepaliveRefreshOnTraffic(t *testing.T) {
	server, client := net.Pipe()

	opts := DefaultInboundOptions
	opts.Password = "ClueCon"
	opts.ReadKeepalive = 300 * time.Millisecond
	fired := make(chan struct{})
	opts.OnDisconnect = func() {
		select {
		case <-fired:
		default:
			close(fired)
		}
	}

	connection := newConnection(client, false, opts.Options)
	go connection.disconnectLoop(opts.OnDisconnect)

	_, err := server.Write([]byte("Content-Type: auth/request\r\n\r\n"))
	assert.NoError(t, err)

	// 持续小流量 1s（每 100ms 一帧，均短于 300ms 保活窗）
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err := server.Write([]byte("Content-Type: auth/request\r\n\r\n"))
		assert.NoError(t, err)
	}

	select {
	case <-fired:
		t.Fatal("正常流量下读保活不应误杀（deadline 应被刷新）")
	default:
		// 预期：连接仍存活
	}

	connection.Close()
	server.Close()
	client.Close()
}

// TestDefaultInboundOptions_ReadKeepaliveEnabled 守护默认值：
// inbound 默认必须开启读保活（生产 eslgo.Dial 走此默认），outbound 默认关闭。
func TestDefaultInboundOptions_ReadKeepaliveEnabled(t *testing.T) {
	assert.Greater(t, DefaultInboundOptions.ReadKeepalive, time.Duration(0),
		"inbound 默认应开启读保活（半开死链防御）")
	assert.Equal(t, time.Duration(0), DefaultOptions.ReadKeepalive,
		"通用默认保持关闭（outbound 等场景不受影响）")
}
