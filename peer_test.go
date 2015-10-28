// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tchannel_test

import (
	"fmt"

	"runtime"

	"sort"

	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/uber/tchannel-go"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/uber/tchannel-go/raw"
	"github.com/uber/tchannel-go/testutils"
	"golang.org/x/net/context"
)

func TestGetPeerNoPeer(t *testing.T) {
	ch := testutils.NewClient(t, nil)
	peer, err := ch.Peers().Get(nil)
	assert.Equal(t, ErrNoPeers, err, "Empty peer list should return error")
	assert.Nil(t, peer, "should not return peer")
}

func TestGetPeerSinglePeer(t *testing.T) {
	ch := testutils.NewClient(t, nil)
	ch.Peers().Add("1.1.1.1:1234")

	peer, err := ch.Peers().Get(nil)
	assert.NoError(t, err, "peer list should return contained element")
	assert.Equal(t, "1.1.1.1:1234", peer.HostPort(), "returned peer mismatch")
}

func TestGetPeerAvoidPrevSelected(t *testing.T) {
	const (
		peer1 = "1.1.1.1:1"
		peer2 = "2.2.2.2:2"
		peer3 = "3.3.3.3:3"
	)

	ch := testutils.NewClient(t, nil)
	a, m := testutils.StrArray, testutils.StrMap
	tests := []struct {
		peers        []string
		prevSelected map[string]struct{}
		expected     map[string]struct{}
	}{
		{
			peers:    a(peer1),
			expected: m(peer1),
		},
		{
			peers:        a(peer1, peer2),
			prevSelected: m(peer1),
			expected:     m(peer2),
		},
		{
			peers:        a(peer1, peer2, peer3),
			prevSelected: m(peer1, peer2),
			expected:     m(peer3),
		},
		{
			peers:        a(peer1),
			prevSelected: m(peer1),
			expected:     m(peer1),
		},
		{
			peers:        a(peer1, peer2, peer3),
			prevSelected: m(peer1, peer2, peer3),
			expected:     m(peer1, peer2, peer3),
		},
	}

	for i, tt := range tests {
		peers := ch.GetSubChannel(fmt.Sprintf("test-%d", i), Isolated).Peers()
		for _, p := range tt.peers {
			peers.Add(p)
		}

		gotPeer, err := peers.Get(tt.prevSelected)
		if err != nil {
			t.Errorf("Got unexpected error selecting peer: %v", err)
			continue
		}

		got := gotPeer.HostPort()
		if _, ok := tt.expected[got]; !ok {
			t.Errorf("Got unexpected peer, expected one of %v got %v\n  Peers = %v PrevSelected = %v",
				tt.expected, got, tt.peers, tt.prevSelected)
		}
	}
}

func TestInboundEphemeralPeerRemoved(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		client := testutils.NewClient(t, nil)
		assert.NoError(t, client.Ping(ctx, hostPort), "Ping to server failed")

		// Server should have a host:port in the root peers for the client.
		var clientHP string
		peers := ch.RootPeers().Copy()
		for k := range peers {
			clientHP = k
		}

		// Close the connection, which should remove the peer from the server channel.
		client.Close()
		runtime.Gosched()
		assert.Equal(t, ChannelClosed, client.State(), "Client should be closed")

		// Wait for the channel to see the connection as closed and update the peer list.
		time.Sleep(time.Millisecond)

		_, ok := ch.RootPeers().Get(clientHP)
		assert.False(t, ok, "server's root peers should remove peer for client on close")
	})
}

func TestOutboundPeerNotAdded(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	WithVerifiedServer(t, nil, func(server *Channel, hostPort string) {
		server.Register(raw.Wrap(newTestHandler(t)), "echo")

		ch := testutils.NewClient(t, nil)
		defer ch.Close()

		ch.Ping(ctx, hostPort)
		raw.Call(ctx, ch, hostPort, server.PeerInfo().ServiceName, "echo", nil, nil)

		peer, err := ch.Peers().Get(nil)
		assert.Equal(t, ErrNoPeers, err, "Ping should not add peers")
		assert.Nil(t, peer, "Expected no peer to be returned")
	})
}

func TestPeerSelectionPreferIncoming(t *testing.T) {
	var allChannels []*Channel

	ctx, cancel := NewContext(time.Second)
	defer cancel()

	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		expected := make(map[string]bool)

		// 5 peers that make incoming connections to ch.
		for i := 0; i < 5; i++ {
			incoming, _, incomingHP := NewServer(t, &testutils.ChannelOpts{ServiceName: fmt.Sprintf("hyperbahn-%d", i)})
			allChannels = append(allChannels, incoming)
			assert.NoError(t, incoming.Ping(ctx, ch.PeerInfo().HostPort), "Ping failed")
			ch.Peers().Add(incomingHP)
			expected[incomingHP] = true
		}

		// 5 random peers that don't have any connections.
		for i := 0; i < 5; i++ {
			ch.Peers().Add(fmt.Sprintf("1.1.1.1:1%d", i))
		}

		// 5 random peers that we have outgoing connections to.
		for i := 0; i < 5; i++ {
			outgoing, _, outgoingHP := NewServer(t, &testutils.ChannelOpts{ServiceName: fmt.Sprintf("outgoing-%d", i)})
			allChannels = append(allChannels, outgoing)
			assert.NoError(t, ch.Ping(ctx, outgoingHP), "Ping failed")
			ch.Peers().Add(outgoingHP)
		}

		// Now select peers in parallel
		selected := make([]string, 1000)
		var selectedIndex int32
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					peer, err := ch.Peers().Get(nil)
					if assert.NoError(t, err, "Peers.Get failed") {
						selected[int(atomic.AddInt32(&selectedIndex, 1))-1] = peer.HostPort()
					}
				}
			}()
		}
		wg.Wait()

		for _, v := range selected {
			assert.True(t, expected[v], "Peers.Get got unexpected peer: %v", v)
		}
	})

	// Clean up allChannels
	for _, c := range allChannels {
		c.Close()
	}
}

type peerTest struct {
	t        *testing.T
	channels []*Channel
}

// NewService will return a new server channel and the host port.
func (pt *peerTest) NewService(svcName string) (*Channel, string) {
	ch, err := testutils.NewServer(&testutils.ChannelOpts{ServiceName: svcName})
	require.NoError(pt.t, err, "NewServer failed")
	pt.channels = append(pt.channels, ch)
	return ch, ch.PeerInfo().HostPort
}

// CleanUp will clean up all channels started as part of the peer test.
func (pt *peerTest) CleanUp() {
	for _, ch := range pt.channels {
		ch.Close()
	}
}

func TestPeerSelection(t *testing.T) {
	pt := &peerTest{t: t}
	defer pt.CleanUp()

	WithVerifiedServer(t, &testutils.ChannelOpts{ServiceName: "S1"}, func(ch *Channel, hostPort string) {
		doPing := func(ch *Channel) {
			ctx, cancel := NewContext(time.Second)
			defer cancel()
			assert.NoError(t, ch.Ping(ctx, hostPort), "Ping failed")
		}

		strategy, count := createScoreStrategy(0, 1)
		s2, _ := pt.NewService("S2")
		s2.GetSubChannel("S1").Peers().SetStrategy(strategy)
		doPing(s2)
		assert.EqualValues(t, 1+2, *count, "Expect exchange update from init resp, ping, pong")
	})
}

func TestIsolatedPeerHeap(t *testing.T) {
	const numPeers = 10
	ch, _ := testutils.NewClient(nil)

	ps1 := createSubChannelWNewStrategy(ch, "S1", numPeers, 1)
	ps2 := createSubChannelWNewStrategy(ch, "S1", numPeers, -1, Isolated)

	hostports := make([]string, numPeers)
	for i := 0; i < numPeers; i++ {
		hostports[i] = fmt.Sprintf("127.0.0.1:%d", i)
		ps1.UpdatePeerHeap(ps1.GetOrAdd(hostports[i]))
		ps2.UpdatePeerHeap(ps2.GetOrAdd(hostports[i]))
	}

	ph1 := ps1.GetHeap()
	ph2 := ps2.GetHeap()
	for i := 0; i < numPeers; i++ {
		assert.Equal(t, hostports[i], ph1.PopPeer().HostPort())
		assert.Equal(t, hostports[numPeers-i-1], ph2.PopPeer().HostPort())
	}
}

func createScoreStrategy(initial, delta int64) (calc ScoreCalculator, count *int64) {
	var score uint64
	count = new(int64)

	return ScoreCalculatorFunc(func(p *Peer) uint64 {
		atomic.AddInt64(count, 1)
		atomic.AddUint64(&score, uint64(delta))
		return atomic.LoadUint64(&score)
	}), count
}

func createSubChannelWNewStrategy(ch *Channel, name string, initial, delta int64, opts ...SubChannelOption) *PeerList {
	strategy, _ := createScoreStrategy(initial, delta)
	sc := ch.GetSubChannel(name, opts...)
	ps := sc.Peers()
	ps.SetStrategy(strategy)
	return ps
}

func testDistribution(t *testing.T, counts map[string]int, min, max float64) {
	for k, v := range counts {
		if float64(v) < min || float64(v) > max {
			t.Errorf("Key %v has value %v which is out of range %v-%v", k, v, min, max)
		}
	}
}

type peerSelectionTest struct {
	peerTest

	// numPeers is the number of peers added to the channel
	numPeers int
	// numCalls is the number of calls to make concurrently.
	numCalls int

	servers         []*Channel
	client          *Channel
	peerCounter     map[string]int
	peerCounterLock sync.Mutex
	onHandler       func()
}

// setupServers will create numPeer servers, and register handlers on them.
func (pt *peerSelectionTest) setupServers() {
	pt.servers = make([]*Channel, pt.numPeers)

	// Set up numPeers servers.
	for i := 0; i < pt.numPeers; i++ {
		var hostPort string
		pt.servers[i], hostPort = pt.NewService("server")
		pt.servers[i].Register(raw.Wrap(newTestHandler(pt.t)), "echo")
		testutils.RegisterFunc(pt.t, pt.servers[i], "hostport", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
			pt.onHandler()

			return &raw.Res{
				Arg3: []byte(hostPort),
			}, nil
		})
	}
}

func (pt *peerSelectionTest) setupClient() {
	pt.client, _ = testutils.NewClient(nil)
	clientSub := pt.client.GetSubChannel("server")

	strategy := ScoreCalculatorFunc(func(p *Peer) uint64 {
		return uint64(GetPendingRequests(p))
	})
	clientSub.Peers().SetStrategy(strategy)

	// This makes a direct call to each server from the client so connections are created.
	for _, server := range pt.servers {
		serverPeer := server.PeerInfo()
		ctx, cancel := NewContext(time.Second)
		defer cancel()

		_, _, _, err := raw.Call(ctx, pt.client, server.PeerInfo().HostPort, serverPeer.ServiceName, "echo", nil, []byte("arg3"))
		require.NoError(pt.t, err, "raw.Call failed")
		assert.Equal(pt.t, 0, GetPendingRequests(clientSub.Peers().GetOrAdd(serverPeer.HostPort)))
	}
}

func (pt *peerSelectionTest) createPeerCounter() {
	pt.peerCounter = make(map[string]int, pt.numPeers)
	for peerHP := range pt.client.Peers().Copy() {
		pt.peerCounter[peerHP] = 0
	}
}

func (pt *peerSelectionTest) makeCall() {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	clientSC := pt.client.GetSubChannel("server")
	_, arg3, _, err := raw.CallSC(ctx, clientSC, "hostport", nil, nil)
	require.NoError(pt.t, err, "raw.Call failed")
	pt.peerCounterLock.Lock()
	pt.peerCounter[string(arg3)]++
	pt.peerCounterLock.Unlock()
}

func (pt *peerSelectionTest) runBatch(numLoops int, preCall func()) {
	pt.createPeerCounter()

	for j := 0; j < numLoops; j++ {
		preCall()

		var callWG sync.WaitGroup
		callWG.Add(pt.numCalls)
		for i := 0; i < pt.numCalls; i++ {
			go func() {
				defer callWG.Done()
				pt.makeCall()
			}()
		}
		callWG.Wait()
	}
}

func (pt *peerSelectionTest) runSaturated(numConcurrent int) {
	pt.createPeerCounter()

	var wg sync.WaitGroup
	sem := make(chan struct{}, numConcurrent)
	for j := 0; j < pt.numCalls; j++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			pt.makeCall()
		}()
	}

	wg.Wait()
}

func (pt *peerSelectionTest) validate() {
	counts := make([]int, 0, len(pt.peerCounter))
	for _, v := range pt.peerCounter {
		counts = append(counts, v)
	}

	sort.Ints(counts)

	median := counts[len(counts)/2]
	testDistribution(pt.t, pt.peerCounter, float64(median)*0.9, float64(median)*1.1)
	pretty.Print(pt.peerCounter)
}

func TestConcurrentPeerSelectionBatched(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping slow test")
	}

	const (
		numPeers = 20
		numCalls = 40
		numLoops = 500
	)

	var handlerWG sync.WaitGroup
	pt := &peerSelectionTest{
		peerTest: peerTest{t: t},
		numPeers: numPeers,
		numCalls: numCalls,
		onHandler: func() {
			// This makes sure that no calls complete before all calls are in-flight.
			handlerWG.Done()
			handlerWG.Wait()
		},
	}
	defer pt.CleanUp()

	pt.setupServers()
	pt.setupClient()
	pt.runBatch(numLoops, func() {
		handlerWG.Add(numCalls)
	})
	pt.validate()
}

func TestConcurrentPeerSelectionSaturated(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping slow test")
	}

	const (
		numPeers    = 20
		numCalls    = 20000
		concurrency = 100
	)

	pt := &peerSelectionTest{
		peerTest:  peerTest{t: t},
		numPeers:  numPeers,
		numCalls:  numCalls,
		onHandler: func() {},
	}
	defer pt.CleanUp()

	pt.setupServers()
	pt.setupClient()
	pt.runSaturated(concurrency)
	pt.validate()
}
