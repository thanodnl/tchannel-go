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
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/uber/tchannel-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/raw"
	"github.com/uber/tchannel-go/testutils"
	"golang.org/x/net/context"
)

type channelState struct {
	ch      *Channel
	closeCh chan struct{}
	closed  bool
}

func makeCall(ch *Channel, hostPort, service string) error {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	_, _, _, err := raw.Call(ctx, ch, hostPort, service, "test", nil, nil)
	return err
}

func TestCloseOnlyListening(t *testing.T) {
	ch := testutils.NewServer(t, nil)

	// If there are no connections, then the channel should close immediately.
	ch.Close()
	assert.Equal(t, ChannelClosed, ch.State())
	assert.True(t, ch.Closed(), "Channel should be closed")
}

func TestCloseNewClient(t *testing.T) {
	ch := testutils.NewServer(t, nil)

	// If there are no connections, then the channel should close immediately.
	ch.Close()
	assert.Equal(t, ChannelClosed, ch.State())
	assert.True(t, ch.Closed(), "Channel should be closed")
}

func TestCloseAfterTimeout(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		testHandler := onErrorTestHandler{newTestHandler(t), func(_ context.Context, err error) {}}
		ch.Register(raw.Wrap(testHandler), "block")

		ctx, cancel := NewContext(10 * time.Millisecond)
		defer cancel()

		// Make a call, wait for it to timeout.
		clientCh := testutils.NewClient(t, nil)
		peerInfo := ch.PeerInfo()
		_, _, _, err := raw.Call(ctx, clientCh, peerInfo.HostPort, peerInfo.ServiceName, "block", nil, nil)
		require.Error(t, err, "Expected call to timeout")

		// The client channel should also close immediately.
		clientCh.Close()
		runtime.Gosched()
		assert.Equal(t, ChannelClosed, clientCh.State())
		assert.True(t, clientCh.Closed(), "Channel should be closed")

		// Unblock the testHandler so that a goroutine isn't leaked.
		<-testHandler.blockErr
	})
	VerifyNoBlockedGoroutines(t)
}

// TestCloseStress ensures that once a Channel is closed, it cannot be reached.
func TestCloseStress(t *testing.T) {
	CheckStress(t)

	const numHandlers = 5
	handler := &swapper{t}
	var lock sync.RWMutex
	var wg sync.WaitGroup
	var channels []*channelState

	// Start numHandlers servers, and don't close the connections till they are signalled.
	for i := 0; i < numHandlers; i++ {
		wg.Add(1)
		go func() {
			WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
				ch.Register(raw.Wrap(handler), "test")

				chState := &channelState{
					ch:      ch,
					closeCh: make(chan struct{}),
				}

				lock.Lock()
				channels = append(channels, chState)
				lock.Unlock()
				wg.Done()

				// Wait for a close signal.
				<-chState.closeCh

				// Lock until the connection is closed.
				lock.Lock()
				chState.closed = true
			})
			lock.Unlock()
		}()
	}

	// Wait till all the channels have been registered.
	wg.Wait()

	// Start goroutines to make calls until the test has ended.
	testEnded := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-testEnded:
					return
				default:
					// Keep making requests till the test ends.
				}

				// Get 2 random channels and make a call from one to the other.
				lock.RLock()
				chState1 := channels[rand.Intn(len(channels))]
				chState2 := channels[rand.Intn(len(channels))]
				if chState1 == chState2 {
					lock.RUnlock()
					continue
				}

				// Grab a read lock to make sure channels aren't closed while we call.
				ch1Closed := chState1.closed
				ch2Closed := chState2.closed
				err := makeCall(chState1.ch, chState2.ch.PeerInfo().HostPort, chState2.ch.PeerInfo().ServiceName)
				lock.RUnlock()
				if ch1Closed || ch2Closed {
					assert.Error(t, err, "Call from %v to %v should fail", chState1.ch.PeerInfo(), chState2.ch.PeerInfo())
				} else {
					assert.NoError(t, err, "Call from %v to %v should not fail", chState1.ch.PeerInfo(), chState2.ch.PeerInfo())
				}
			}
		}()
	}

	// Kill connections till all of the connections are dead.
	for i := 0; i < numHandlers; i++ {
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		channels[i].closeCh <- struct{}{}
	}
}

type closeSemanticsTest struct {
	*testing.T

	ctx      context.Context
	isolated bool
}

func (t *closeSemanticsTest) makeServer(name string) (*Channel, chan struct{}) {
	ch := testutils.NewServer(t.T, &testutils.ChannelOpts{ServiceName: name})

	c := make(chan struct{})
	testutils.RegisterFunc(t.T, ch, "stream", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		<-c
		return &raw.Res{}, nil
	})
	testutils.RegisterFunc(t.T, ch, "call", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		return &raw.Res{}, nil
	})
	return ch, c
}

func (t *closeSemanticsTest) withNewClient(f func(ch *Channel)) {
	ch := testutils.NewClient(t.T, &testutils.ChannelOpts{ServiceName: "client"})
	f(ch)
	ch.Close()
}

func (t *closeSemanticsTest) startCall(from *Channel, to *Channel, operation string) (*OutboundCall, error) {
	var call *OutboundCall
	var err error
	toPeer := to.PeerInfo()
	if t.isolated {
		sc := from.GetSubChannel(toPeer.ServiceName, Isolated)
		sc.Peers().Add(toPeer.HostPort)
		call, err = sc.BeginCall(t.ctx, operation, nil)
	} else {
		call, err = from.BeginCall(t.ctx, toPeer.HostPort, toPeer.ServiceName, operation, nil)
	}
	return call, err
}

func (t *closeSemanticsTest) call(from *Channel, to *Channel) error {
	call, err := t.startCall(from, to, "call")
	if err == nil {
		_, _, _, err = raw.WriteArgs(call, nil, nil)
	}
	return err
}

func (t *closeSemanticsTest) callStream(from *Channel, to *Channel) <-chan struct{} {
	c := make(chan struct{})

	call, err := t.startCall(from, to, "stream")
	require.NoError(t, err, "stream call failed to start")
	require.NoError(t, NewArgWriter(call.Arg2Writer()).Write(nil), "write arg2")
	require.NoError(t, NewArgWriter(call.Arg3Writer()).Write(nil), "write arg3")

	go func() {
		var d []byte
		require.NoError(t, NewArgReader(call.Response().Arg2Reader()).Read(&d), "read arg2 from %v to %v", from.PeerInfo(), to.PeerInfo())
		require.NoError(t, NewArgReader(call.Response().Arg3Reader()).Read(&d), "read arg3")
		c <- struct{}{}
	}()

	return c
}

func (t *closeSemanticsTest) runTest(ctx context.Context) {
	s1, s1C := t.makeServer("s1")
	s2, s2C := t.makeServer("s2")

	// Make a call from s1 -> s2, and s2 -> s1
	call1 := t.callStream(s1, s2)
	call2 := t.callStream(s2, s1)

	// s1 and s2 are both open, so calls to it should be successful.
	t.withNewClient(func(ch *Channel) {
		require.NoError(t, t.call(ch, s1), "failed to call s1")
		require.NoError(t, t.call(ch, s2), "failed to call s2")
	})
	require.NoError(t, t.call(s1, s2), "call s1 -> s2 failed")
	require.NoError(t, t.call(s2, s1), "call s2 -> s1 failed")

	// Close s1, should no longer be able to call it.
	s1.Close()
	assert.Equal(t, ChannelStartClose, s1.State())
	t.withNewClient(func(ch *Channel) {
		assert.Error(t, t.call(ch, s1), "closed channel should not accept incoming calls")
		require.NoError(t, t.call(ch, s2),
			"closed channel with pending incoming calls should allow outgoing calls")
	})

	// Even an existing connection (e.g. from s2) should fail.
	// TODO: this will fail until the peer is shared.
	if !assert.Equal(t, ErrChannelClosed, t.call(s2, s1),
		"closed channel should not accept incoming calls") {
		t.Errorf("err %v", t.call(s2, s1))
	}

	require.NoError(t, t.call(s1, s2),
		"closed channel with pending incoming calls should allow outgoing calls")

	// Once the incoming connection is drained, outgoing calls should fail.
	s1C <- struct{}{}
	<-call2
	runtime.Gosched()
	assert.Equal(t, ChannelInboundClosed, s1.State())
	require.Error(t, t.call(s1, s2),
		"closed channel with no pending incoming calls should not allow outgoing calls")

	// Now the channel should be completely closed as there are no pending connections.
	s2C <- struct{}{}
	<-call1
	runtime.Gosched()
	assert.Equal(t, ChannelClosed, s1.State())

	// Close s2 so we don't leave any goroutines running.
	s2.Close()
}

func TestCloseSemantics(t *testing.T) {
	// We defer the check as we want it to run after the SetTimeout clears the timeout.
	defer VerifyNoBlockedGoroutines(t)
	defer testutils.SetTimeout(t, 2*time.Second)()

	ctx, cancel := NewContext(time.Second)
	defer cancel()

	ct := &closeSemanticsTest{t, ctx, false /* isolated */}
	ct.runTest(ctx)
}

func TestCloseSemanticsIsolated(t *testing.T) {
	// We defer the check as we want it to run after the SetTimeout clears the timeout.
	defer VerifyNoBlockedGoroutines(t)
	defer testutils.SetTimeout(t, 2*time.Second)()

	ctx, cancel := NewContext(time.Second)
	defer cancel()

	ct := &closeSemanticsTest{t, ctx, true /* isolated */}
	ct.runTest(ctx)
}

func TestCloseSingleChannel(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()
	ch := testutils.NewServer(t, nil)

	var connected sync.WaitGroup
	var completed sync.WaitGroup
	blockCall := make(chan struct{})

	testutils.RegisterFunc(t, ch, "echo", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		connected.Done()
		<-blockCall
		return &raw.Res{
			Arg2: args.Arg2,
			Arg3: args.Arg3,
		}, nil
	})

	for i := 0; i < 10; i++ {
		connected.Add(1)
		completed.Add(1)
		go func() {
			peerInfo := ch.PeerInfo()
			_, _, _, err := raw.Call(ctx, ch, peerInfo.HostPort, peerInfo.ServiceName, "echo", nil, nil)
			assert.NoError(t, err, "Call failed")
			completed.Done()
		}()
	}

	// Wait for all calls to connect before triggerring the Close (so they do not fail).
	connected.Wait()
	ch.Close()

	// Unblock the calls, and wait for all the calls to complete.
	close(blockCall)
	completed.Wait()

	// Once all calls are complete, the channel should be closed.
	runtime.Gosched()
	assert.Equal(t, ChannelClosed, ch.State())
	VerifyNoBlockedGoroutines(t)
}

func TestCloseOneSide(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	ch1 := testutils.NewServer(t, &testutils.ChannelOpts{ServiceName: "client"})
	ch2 := testutils.NewServer(t, &testutils.ChannelOpts{ServiceName: "server"})

	connected := make(chan struct{})
	completed := make(chan struct{})
	blockCall := make(chan struct{})
	testutils.RegisterFunc(t, ch2, "echo", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		connected <- struct{}{}
		<-blockCall
		return &raw.Res{
			Arg2: args.Arg2,
			Arg3: args.Arg3,
		}, nil
	})

	go func() {
		ch2Peer := ch2.PeerInfo()
		_, _, _, err := raw.Call(ctx, ch1, ch2Peer.HostPort, ch2Peer.ServiceName, "echo", nil, nil)
		assert.NoError(t, err, "Call failed")
		completed <- struct{}{}
	}()

	// Wait for connected before calling Close.
	<-connected
	ch1.Close()

	// Now unblock the call and wait for the call to complete.
	close(blockCall)
	<-completed

	// Once the call completes, the channel should be closed.
	runtime.Gosched()
	assert.Equal(t, ChannelClosed, ch1.State())

	// We need to close all open TChannels before verifying blocked goroutines.
	ch2.Close()
	VerifyNoBlockedGoroutines(t)
}

// TestCloseSendError tests that system errors are not attempted to be sent when
// a connection is closed, and ensures there's no race conditions such as the error
// frame being added to the channel just as it is closed.
// TODO(prashant): This test is waiting for timeout, but socket close shouldn't wait for timeout.
func TestCloseSendError(t *testing.T) {
	ctx, cancel := NewContext(50 * time.Millisecond)
	defer cancel()

	serverCh := testutils.NewServer(t, nil)

	closed := uint32(0)
	counter := uint32(0)
	testutils.RegisterFunc(t, serverCh, "echo", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		atomic.AddUint32(&counter, 1)
		return &raw.Res{Arg2: args.Arg2, Arg3: args.Arg3}, nil
	})

	clientCh := testutils.NewClient(t, nil)

	// Make a call to create a connection that will be shared.
	peerInfo := serverCh.PeerInfo()
	_, _, _, err := raw.Call(ctx, clientCh, peerInfo.HostPort, peerInfo.ServiceName, "echo", nil, nil)
	require.NoError(t, err, "Call should succeed")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
			_, _, _, err := raw.Call(ctx, clientCh, peerInfo.HostPort, peerInfo.ServiceName, "echo", nil, nil)
			if err != nil && atomic.LoadUint32(&closed) == 0 {
				t.Errorf("Call failed: %v", err)
			}
			wg.Done()
		}()
	}

	// Wait for the server to have processed some number of these calls.
	for {
		if atomic.LoadUint32(&counter) >= 10 {
			break
		}
		runtime.Gosched()
	}

	atomic.AddUint32(&closed, 1)
	serverCh.Close()

	// Wait for all the goroutines to end
	wg.Wait()

	clientCh.Close()
	VerifyNoBlockedGoroutines(t)
}

func callWithNewClient(t *testing.T, hostPort string) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	client := testutils.NewClient(t, nil)
	assert.NoError(t, client.Ping(ctx, hostPort))
	client.Close()
}

func TestNoLeakedState(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		state1 := ch.IntrospectState(&IntrospectionOptions{})
		for i := 0; i < 100; i++ {
			callWithNewClient(t, hostPort)
		}

		time.Sleep(time.Millisecond)
		state2 := ch.IntrospectState(&IntrospectionOptions{})
		assert.Equal(t, state1, state2, "State mismatch")
	})
}
