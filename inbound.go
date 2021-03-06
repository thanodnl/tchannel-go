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

package tchannel

import (
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"
)

var errInboundRequestAlreadyActive = errors.New("inbound request is already active; possible duplicate client id")

// handleCallReq handles an incoming call request, registering a message
// exchange to receive further fragments for that call, and dispatching it in
// another goroutine
func (c *Connection) handleCallReq(frame *Frame) bool {
	switch state := c.readState(); state {
	case connectionActive:
		break
	case connectionStartClose, connectionInboundClosed, connectionClosed:
		c.SendSystemError(frame.Header.ID, nil, ErrChannelClosed)
		return true
	case connectionWaitingToRecvInitReq, connectionWaitingToSendInitReq, connectionWaitingToRecvInitRes:
		c.SendSystemError(frame.Header.ID, nil, NewSystemError(ErrCodeDeclined, "connection not ready"))
		return true
	default:
		panic(fmt.Errorf("unknown connection state for call req: %v", state))
	}

	callReq := new(callReq)
	initialFragment, err := parseInboundFragment(c.framePool, frame, callReq)
	if err != nil {
		// TODO(mmihic): Probably want to treat this as a protocol error
		c.log.Errorf("could not decode %s: %v", frame.Header, err)
		return true
	}

	call := new(InboundCall)
	call.conn = c
	ctx, cancel := newIncomingContext(call, callReq.TimeToLive, &callReq.Tracing)

	mex, err := c.inbound.newExchange(ctx, c.framePool, callReq.messageType(), frame.Header.ID, mexChannelBufferSize)
	if err != nil {
		if err == errDuplicateMex {
			err = errInboundRequestAlreadyActive
		}
		c.log.Errorf("could not register exchange for %s", frame.Header)
		c.SendSystemError(frame.Header.ID, nil, err)
		return true
	}

	// Close may have been called between the time we checked the state and us creating the exchange.
	if c.readState() != connectionActive {
		mex.shutdown()
		return true
	}

	response := new(InboundCallResponse)
	response.Annotations = Annotations{
		reporter: c.traceReporter,
		span:     callReq.Tracing,
		endpoint: TargetEndpoint{
			HostPort:    c.localPeerInfo.HostPort,
			ServiceName: callReq.Service,
		},
		timeNow: c.timeNow,
		binaryAnnotationsBacking: [2]BinaryAnnotation{
			{Key: "cn", Value: callReq.Headers[CallerName]},
			{Key: "as", Value: callReq.Headers[ArgScheme]},
		},
	}
	response.annotations = response.annotationsBacking[:0]
	response.binaryAnnotations = response.binaryAnnotationsBacking[:]
	response.AddAnnotation(AnnotationKeyServerReceive)
	response.mex = mex
	response.conn = c
	response.cancel = cancel
	response.span = callReq.Tracing
	response.log = c.log.WithFields(LogField{"In-Response", callReq.ID()})
	response.contents = newFragmentingWriter(response.log, response, initialFragment.checksumType.New())
	response.headers = transportHeaders{}
	response.messageForFragment = func(initial bool) message {
		if initial {
			callRes := new(callRes)
			callRes.Headers = response.headers
			callRes.ResponseCode = responseOK
			if response.applicationError {
				callRes.ResponseCode = responseApplicationError
			}
			return callRes
		}

		return new(callResContinue)
	}

	call.mex = mex
	call.initialFragment = initialFragment
	call.serviceName = string(callReq.Service)
	call.headers = callReq.Headers
	call.span = callReq.Tracing
	call.response = response
	call.log = c.log.WithFields(LogField{"In-Call", callReq.ID()})
	call.messageForFragment = func(initial bool) message { return new(callReqContinue) }
	call.contents = newFragmentingReader(call.log, call)
	call.statsReporter = c.statsReporter
	call.createStatsTags(c.commonStatsTags)

	response.statsReporter = c.statsReporter
	response.commonStatsTags = call.commonStatsTags

	setResponseHeaders(call.headers, response.headers)
	go c.dispatchInbound(c.connID, callReq.ID(), call)
	return false
}

// handleCallReqContinue handles the continuation of a call request, forwarding
// it to the request channel for that request, where it can be pulled during
// defragmentation
func (c *Connection) handleCallReqContinue(frame *Frame) bool {
	if err := c.inbound.forwardPeerFrame(frame); err != nil {
		// If forward fails, it's due to a timeout.
		c.inbound.timeoutExchange(frame.Header.ID)
		return true
	}
	return false
}

// createStatsTags creates the common stats tags, if they are not already created.
func (call *InboundCall) createStatsTags(connectionTags map[string]string) {
	call.commonStatsTags = map[string]string{
		"calling-service": call.CallerName(),
	}
	for k, v := range connectionTags {
		call.commonStatsTags[k] = v
	}
}

// dispatchInbound ispatches an inbound call to the appropriate handler
func (c *Connection) dispatchInbound(_ uint32, _ uint32, call *InboundCall) {
	if c.log.Enabled(LogLevelDebug) {
		c.log.Debugf("Received incoming call for %s from %s", call.ServiceName(), c.remotePeerInfo)
	}

	if err := call.readOperation(); err != nil {
		c.log.Errorf("Could not read operation from %s: %v", c.remotePeerInfo, err)
		return
	}

	call.commonStatsTags["endpoint"] = string(call.operation)
	call.statsReporter.IncCounter("inbound.calls.recvd", call.commonStatsTags, 1)
	call.response.calledAt = c.timeNow()
	call.response.SetOperation(string(call.operation))

	// NB(mmihic): Don't cast operation name to string here - this will
	// create a copy of the byte array, where as aliasing to string in the
	// map look up can be optimized by the compiler to avoid the copy.  See
	// https://github.com/golang/go/issues/3512
	h := c.handlers.find(call.ServiceName(), call.Operation())
	if h == nil {
		// Check the subchannel map to see if we find one there
		if c.log.Enabled(LogLevelDebug) {
			c.log.Debugf("Checking the subchannel's handlers for %s:%s", call.ServiceName(), call.Operation())
		}

		h = c.subChannels.find(call.ServiceName(), call.Operation())
	}
	if h == nil {
		c.log.Errorf("Could not find handler for %s:%s", call.ServiceName(), call.Operation())
		call.Response().SendSystemError(
			NewSystemError(ErrCodeBadRequest, "no handler for service %q and operation %q", call.ServiceName(), call.Operation()))
		return
	}

	// TODO(prashant): This is an expensive way to check for cancellation. Use a heap for timeouts.
	go func() {
		if <-call.mex.ctx.Done(); call.mex.ctx.Err() == context.DeadlineExceeded {
			call.mex.inboundTimeout()
		}
	}()

	if c.log.Enabled(LogLevelDebug) {
		c.log.Debugf("Dispatching %s:%s from %s", call.ServiceName(), call.Operation(), c.remotePeerInfo)
	}
	h.Handle(call.mex.ctx, call)
}

// An InboundCall is an incoming call from a peer
type InboundCall struct {
	reqResReader

	conn            *Connection
	response        *InboundCallResponse
	serviceName     string
	operation       []byte
	operationString string
	headers         transportHeaders
	span            Span
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// ServiceName returns the name of the service being called
func (call *InboundCall) ServiceName() string {
	return call.serviceName
}

// Operation returns the operation being called
func (call *InboundCall) Operation() []byte {
	return call.operation
}

// OperationString returns the operation being called as a string.
func (call *InboundCall) OperationString() string {
	return call.operationString
}

// Format the format of the request from the ArgScheme transport header.
func (call *InboundCall) Format() Format {
	return Format(call.headers[ArgScheme])
}

// CallerName returns the caller name from the CallerName transport header.
func (call *InboundCall) CallerName() string {
	return call.headers[CallerName]
}

// ShardKey returns the shard key from the ShardKey transport header.
func (call *InboundCall) ShardKey() string {
	return call.headers[ShardKey]
}

// Reads the entire operation name (arg1) from the request stream.
func (call *InboundCall) readOperation() error {
	var arg1 []byte
	if err := NewArgReader(call.arg1Reader()).Read(&arg1); err != nil {
		return call.failed(err)
	}

	call.operation = arg1
	call.operationString = string(arg1)
	return nil
}

// Arg2Reader returns an io.ReadCloser to read the second argument.
// The ReadCloser must be closed once the argument has been read.
func (call *InboundCall) Arg2Reader() (io.ReadCloser, error) {
	return call.arg2Reader()
}

// Arg3Reader returns an io.ReadCloser to read the last argument.
// The ReadCloser must be closed once the argument has been read.
func (call *InboundCall) Arg3Reader() (io.ReadCloser, error) {
	return call.arg3Reader()
}

// Response provides access to the InboundCallResponse object which can be used
// to write back to the calling peer
func (call *InboundCall) Response() *InboundCallResponse {
	if call.err != nil {
		// While reading Thrift, we cannot distinguish between malformed Thrift and other errors,
		// and so we may try to respond with a bad request. We should ensure that the response
		// is marked as failed if the request has failed so that we don't try to shutdown the exchange
		// a second time.
		call.response.err = call.err
	}
	return call.response
}

func (call *InboundCall) doneReading(unexpected error) {}

// An InboundCallResponse is used to send the response back to the calling peer
type InboundCallResponse struct {
	reqResWriter
	Annotations

	cancel context.CancelFunc
	// calledAt is the time the inbound call was routed to the application.
	calledAt         time.Time
	applicationError bool
	systemError      bool
	headers          transportHeaders
	span             Span
	statsReporter    StatsReporter
	commonStatsTags  map[string]string
}

// SendSystemError returns a system error response to the peer.  The call is considered
// complete after this method is called, and no further data can be written.
func (response *InboundCallResponse) SendSystemError(err error) error {
	if response.err != nil {
		return response.err
	}
	// Fail all future attempts to read fragments
	response.state = reqResWriterComplete
	response.systemError = true
	response.doneSending()
	return response.conn.SendSystemError(response.mex.msgID, CurrentSpan(response.mex.ctx), err)
}

// SetApplicationError marks the response as being an application error.  This method can
// only be called before any arguments have been sent to the calling peer.
func (response *InboundCallResponse) SetApplicationError() error {
	if response.state > reqResWriterPreArg2 {
		return response.failed(errReqResWriterStateMismatch{
			state:         response.state,
			expectedState: reqResWriterPreArg2,
		})
	}
	response.applicationError = true
	return nil
}

// Arg2Writer returns a WriteCloser that can be used to write the second argument.
// The returned writer must be closed once the write is complete.
func (response *InboundCallResponse) Arg2Writer() (ArgWriter, error) {
	if err := NewArgWriter(response.arg1Writer()).Write(nil); err != nil {
		return nil, err
	}
	return response.arg2Writer()
}

// Arg3Writer returns a WriteCloser that can be used to write the last argument.
// The returned writer must be closed once the write is complete.
func (response *InboundCallResponse) Arg3Writer() (ArgWriter, error) {
	return response.arg3Writer()
}

// doneSending shuts down the message exchange for this call.
// For incoming calls, the last message is sending the call response.
func (response *InboundCallResponse) doneSending() {
	// TODO(prashant): Move this to when the message is actually being sent.
	response.AddAnnotation(AnnotationKeyServerSend)
	response.Report()

	latency := response.GetTime().Sub(response.calledAt)
	response.statsReporter.RecordTimer("inbound.calls.latency", response.commonStatsTags, latency)

	if response.systemError {
		// TODO(prashant): Report the error code type as per metrics doc and enable.
		// response.statsReporter.IncCounter("inbound.calls.system-errors", response.commonStatsTags, 1)
	} else if response.applicationError {
		response.statsReporter.IncCounter("inbound.calls.app-errors", response.commonStatsTags, 1)
	} else {
		response.statsReporter.IncCounter("inbound.calls.success", response.commonStatsTags, 1)
	}

	// Cancel the context since the response is complete.
	response.cancel()

	// The message exchange is still open if there are no errors, call shutdown.
	if response.err == nil {
		response.mex.shutdown()
	}
}
