package tuic

import (
	"testing"
	"time"
)

type idleTestClient struct {
	Client
	streams int64
	last    time.Time
	closed  int
}

func (c *idleTestClient) OpenStreams() int64            { return c.streams }
func (c *idleTestClient) LastVisited() time.Time        { return c.last }
func (c *idleTestClient) SetLastVisited(last time.Time) { c.last = last }
func (c *idleTestClient) CloseIdle()                    { c.closed++ }

func TestPoolClosesOnlyUnusedSessions(t *testing.T) {
	old := time.Now().Add(-2 * time.Minute)
	active := &idleTestClient{streams: 1, last: old}
	recent := &idleTestClient{last: time.Now()}
	idleTCP := &idleTestClient{last: old}
	idleUDP := &idleTestClient{last: old}
	pool := &PoolClient{}
	pool.tcpClients.PushBack(active)
	pool.tcpClients.PushBack(recent)
	pool.tcpClients.PushBack(idleTCP)
	pool.udpClients.PushBack(idleUDP)
	pool.CloseIdle(time.Minute)
	if active.closed != 0 || recent.closed != 0 || idleTCP.closed != 1 || idleUDP.closed != 1 {
		t.Fatal("only old sessions with no open streams may close")
	}
}

func TestPoolReservesSelectedSessionBeforeDial(t *testing.T) {
	client := &idleTestClient{last: time.Now().Add(-2 * time.Minute)}
	pool := &PoolClient{}
	pool.tcpClients.PushBack(client)
	if pool.getClient(false) != client {
		t.Fatal("unexpected selected client")
	}
	pool.CloseIdle(time.Minute)
	if client.closed != 0 {
		t.Fatal("a selected session must be protected before the dial starts")
	}
}
