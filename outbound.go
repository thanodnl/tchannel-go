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
	"fmt"
	"io"
	"time"

	"github.com/uber/tchannel-go/typed"
	"golang.org/x/net/context"
)

// maxOperationSize is the maximum size of arg1.
const maxOperationSize = 16 * 1024

// beginCall begins an outbound call on the connection
func (c *Connection) beginCall(ctx context.Context, serviceName string, callOptions *CallOptions, operation string) (*OutboundCall, error) {
	switch c.readState() {
	case connectionActive, connectionStartClose:
		break
	case connectionInboundClosed, connectionClosed:
		return nil, ErrConnectionClosed
	case connectionWaitingToRecvInitReq, connectionWaitingToSendInitReq, connectionWaitingToRecvInitRes:
		return nil, ErrConnectionNotReady
	default:
		return nil, errConnectionUnknownState
	}

	deadline, ok := ctx.Deadline()
	// No deadline was set, we should not support no deadlines.
	if !ok {
		return nil, ErrTimeoutRequired
	}
	timeToLive := deadline.Sub(time.Now())
	if timeToLive <= 0 {
		return nil, ErrTimeout
	}

	requestID := c.NextMessageID()
	mex, err := c.outbound.newExchange(ctx, c.framePool, messageTypeCallReq, requestID, mexChannelBufferSize)
	if err != nil {
		return nil, err
	}

	// Close may have been called between the time we checked the state and us creating the exchange.
	if state := c.readState(); state != connectionStartClose && state != connectionActive {
		mex.shutdown()
		return nil, ErrConnectionClosed
	}

	headers := transportHeaders{
		CallerName: c.localPeerInfo.ServiceName,
	}
	callOptions.setHeaders(headers)
	if opts := currentCallOptions(ctx); opts != nil {
		opts.overrideHeaders(headers)
	}

	call := new(OutboundCall)
	call.mex = mex
	call.conn = c
	call.callReq = callReq{
		id:         requestID,
		Headers:    headers,
		Service:    serviceName,
		TimeToLive: timeToLive,
	}
	call.statsReporter = c.statsReporter
	call.createStatsTags(c.commonStatsTags, callOptions, operation)
	call.log = c.log.WithFields(LogField{"Out-Call", requestID})

	// TODO(mmihic): It'd be nice to do this without an fptr
	call.messageForFragment = func(initial bool) message {
		if initial {
			return &call.callReq
		}

		return new(callReqContinue)
	}

	call.contents = newFragmentingWriter(call.log, call, c.checksumType.New())
	span := CurrentSpan(ctx)
	if span != nil {
		call.callReq.Tracing = *span.NewChildSpan()
	} else {
		// TODO(mmihic): Potentially reject calls that are made outside a root context?
		call.callReq.Tracing.EnableTracing(false)
	}

	response := new(OutboundCallResponse)
	response.Annotations = Annotations{
		reporter: c.traceReporter,
		span:     call.callReq.Tracing,
		endpoint: TargetEndpoint{
			HostPort:    c.remotePeerInfo.HostPort,
			ServiceName: serviceName,
			Operation:   operation,
		},
		timeNow: c.timeNow,
		binaryAnnotationsBacking: [2]BinaryAnnotation{
			{Key: "cn", Value: call.callReq.Headers[CallerName]},
			{Key: "as", Value: call.callReq.Headers[ArgScheme]},
		},
	}
	response.annotations = response.annotationsBacking[:0]
	response.binaryAnnotations = response.binaryAnnotationsBacking[:]
	response.AddAnnotation(AnnotationKeyClientSend)

	response.requestState = callOptions.RequestState
	response.startedAt = c.timeNow()
	response.mex = mex
	response.log = c.log.WithFields(LogField{"Out-Response", requestID})
	response.messageForFragment = func(initial bool) message {
		if initial {
			return &response.callRes
		}

		return new(callResContinue)
	}
	response.contents = newFragmentingReader(response.log, response)
	response.statsReporter = call.statsReporter
	response.commonStatsTags = call.commonStatsTags

	call.response = response

	if err := call.writeOperation([]byte(operation)); err != nil {
		return nil, err
	}
	return call, nil
}

// handleCallRes handles an incoming call req message, forwarding the
// frame to the response channel waiting for it
func (c *Connection) handleCallRes(frame *Frame) bool {
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		return true
	}
	return false
}

// handleCallResContinue handles an incoming call res continue message,
// forwarding the frame to the response channel waiting for it
func (c *Connection) handleCallResContinue(frame *Frame) bool {
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		return true
	}
	return false
}

// An OutboundCall is an active call to a remote peer.  A client makes a call
// by calling BeginCall on the Channel, writing argument content via
// ArgWriter2() ArgWriter3(), and then reading reading response data via the
// ArgReader2() and ArgReader3() methods on the Response() object.
type OutboundCall struct {
	reqResWriter

	callReq         callReq
	response        *OutboundCallResponse
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// Response provides access to the call's response object, which can be used to
// read response arguments
func (call *OutboundCall) Response() *OutboundCallResponse {
	return call.response
}

// createStatsTags creates the common stats tags, if they are not already created.
func (call *OutboundCall) createStatsTags(connectionTags map[string]string, callOptions *CallOptions, operation string) {
	call.commonStatsTags = map[string]string{
		"target-service": call.callReq.Service,
	}
	for k, v := range connectionTags {
		call.commonStatsTags[k] = v
	}
	if callOptions.Format != HTTP {
		call.commonStatsTags["target-endpoint"] = string(operation)
	}
}

// writeOperation writes the operation (arg1) to the call
func (call *OutboundCall) writeOperation(operation []byte) error {
	if len(operation) > maxOperationSize {
		return call.failed(ErrOperationTooLarge)
	}

	call.statsReporter.IncCounter("outbound.calls.send", call.commonStatsTags, 1)
	return NewArgWriter(call.arg1Writer()).Write(operation)
}

// Arg2Writer returns a WriteCloser that can be used to write the second argument.
// The returned writer must be closed once the write is complete.
func (call *OutboundCall) Arg2Writer() (ArgWriter, error) {
	return call.arg2Writer()
}

// Arg3Writer returns a WriteCloser that can be used to write the last argument.
// The returned writer must be closed once the write is complete.
func (call *OutboundCall) Arg3Writer() (ArgWriter, error) {
	return call.arg3Writer()
}

func (call *OutboundCall) doneSending() {}

// An OutboundCallResponse is the response to an outbound call
type OutboundCallResponse struct {
	reqResReader
	Annotations

	callRes callRes

	requestState *RequestState
	// startedAt is the time at which the outbound call was started.
	startedAt       time.Time
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// ApplicationError returns true if the call resulted in an application level error
// TODO(mmihic): In current implementation, you must have called Arg2Reader before this
// method returns the proper value.  We should instead have this block until the first
// fragment is available, if the first fragment hasn't been received.
func (response *OutboundCallResponse) ApplicationError() bool {
	// TODO(mmihic): Wait for first fragment
	return response.callRes.ResponseCode == responseApplicationError
}

// Format the format of the request from the ArgScheme transport header.
func (response *OutboundCallResponse) Format() Format {
	return Format(response.callRes.Headers[ArgScheme])
}

// Arg2Reader returns an io.ReadCloser to read the second argument.
// The ReadCloser must be closed once the argument has been read.
func (response *OutboundCallResponse) Arg2Reader() (io.ReadCloser, error) {
	var operation []byte
	if err := NewArgReader(response.arg1Reader()).Read(&operation); err != nil {
		return nil, err
	}

	return response.arg2Reader()
}

// Arg3Reader returns an io.ReadCloser to read the last argument.
// The ReadCloser must be closed once the argument has been read.
func (response *OutboundCallResponse) Arg3Reader() (io.ReadCloser, error) {
	return response.arg3Reader()
}

// handleError handles an error coming back from the peer. If the error is a
// protocol level error, the entire connection will be closed.  If the error is
// a request specific error, it will be written to the request's response
// channel and converted into a SystemError returned from the next reader or
// access call.
func (c *Connection) handleError(frame *Frame) {
	errMsg := errorMessage{
		id: frame.Header.ID,
	}
	rbuf := typed.NewReadBuffer(frame.SizedPayload())
	if err := errMsg.read(rbuf); err != nil {
		c.log.Warnf("Unable to read Error frame from %s: %v", c.remotePeerInfo, err)
		c.connectionError(err)
		return
	}

	if errMsg.errCode == ErrCodeProtocol {
		c.log.Warnf("Peer %s reported protocol error: %s", c.remotePeerInfo, errMsg.message)
		c.connectionError(errMsg.AsSystemError())
		return
	}

	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		c.log.Infof("Failed to forward error frame %v to mex, error: %v", frame.Header, errMsg)
	}
}

func cloneTags(tags map[string]string) map[string]string {
	newTags := make(map[string]string, len(tags))
	for k, v := range tags {
		newTags[k] = v
	}
	return newTags
}

// doneReading shuts down the message exchange for this call.
// For outgoing calls, the last message is reading the call response.
func (response *OutboundCallResponse) doneReading(unexpected error) {
	response.AddAnnotation(AnnotationKeyClientReceive)
	response.Report()

	isSuccess := unexpected == nil && !response.ApplicationError()
	lastAttempt := isSuccess || !response.requestState.HasRetries(unexpected)

	now := response.GetTime()
	latency := now.Sub(response.startedAt)
	response.statsReporter.RecordTimer("outbound.calls.per-attempt.latency", response.commonStatsTags, latency)
	if lastAttempt {
		requestLatency := response.requestState.SinceStart(now, latency)
		response.statsReporter.RecordTimer("outbound.calls.latency", response.commonStatsTags, requestLatency)
	}
	if retryCount := response.requestState.RetryCount(); retryCount > 0 {
		retryTags := cloneTags(response.commonStatsTags)
		retryTags["retry-count"] = fmt.Sprint(retryCount)
		response.statsReporter.IncCounter("outbound.calls.retries", retryTags, 1)
	}

	if unexpected != nil {
		// TODO(prashant): Report the error code type as per metrics doc and enable.
		// response.statsReporter.IncCounter("outbound.calls.system-errors", response.commonStatsTags, 1)
	} else if response.ApplicationError() {
		// TODO(prashant): Figure out how to add "type" to tags, which TChannel does not know about.
		response.statsReporter.IncCounter("outbound.calls.per-attempt.app-errors", response.commonStatsTags, 1)
		if lastAttempt {
			response.statsReporter.IncCounter("outbound.calls.app-errors", response.commonStatsTags, 1)
		}
	} else {
		response.statsReporter.IncCounter("outbound.calls.success", response.commonStatsTags, 1)
	}

	response.mex.shutdown()
}
