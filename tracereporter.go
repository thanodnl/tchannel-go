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
	"log"
	"time"
)

// When reporting spans, we report:
// bunch of "binary" annotations (eg

// AnnotationKey is the key for annotations.
type AnnotationKey string

// Known annotation keys
const (
	AnnotationKeyClientSend    = "cs"
	AnnotationKeyClientReceive = "cr"
	AnnotationKeyServerSend    = "ss"
	AnnotationKeyServerReceive = "sr"
)

// BinaryAnnotation is additional context information about the span.
type BinaryAnnotation struct {
	Key string
	// Value contains one of: string, float64, bool, []byte, int64
	Value interface{}
}

// Annotation represents a specific event and the timestamp at which it occurred.
type Annotation struct {
	Key       AnnotationKey
	Timestamp time.Time
}

// TraceReporter is the interface used to report Trace spans.
type TraceReporter interface {
	// Report method is intended to report Span information.
	// It returns any error encountered otherwise nil.
	Report(span Span, annotations []Annotation, binaryAnnotations []BinaryAnnotation, targetEndpoint TargetEndpoint)
}

// TraceReporterFunc allows using a function as a TraceReporter.
type TraceReporterFunc func(span Span, annotations []Annotation, binaryAnnotations []BinaryAnnotation, targetEndpoint TargetEndpoint)

// Report calls the underlying function.
func (f TraceReporterFunc) Report(span Span, annotations []Annotation, binaryAnnotations []BinaryAnnotation, targetEndpoint TargetEndpoint) {
	f(span, annotations, binaryAnnotations, targetEndpoint)
}

// NullReporter is the default TraceReporter which does not do anything.
var NullReporter TraceReporter = nullReporter{}

type nullReporter struct{}

func (nullReporter) Report(_ Span, _ []Annotation, _ []BinaryAnnotation, _ TargetEndpoint) {
}

// SimpleTraceReporter is a trace reporter which prints using the default logger.
var SimpleTraceReporter TraceReporter = simpleTraceReporter{}

type simpleTraceReporter struct{}

func (simpleTraceReporter) Report(
	span Span, annotations []Annotation, binaryAnnotations []BinaryAnnotation, targetEndpoint TargetEndpoint) {
	log.Printf("SimpleTraceReporter.Report span: %+v annotations: %+v binaryAnnotations: %+v targetEndpoint: %+v",
		span, annotations, binaryAnnotations, targetEndpoint)
}

// Annotations is used to track annotations and report them to a TraceReporter.
type Annotations struct {
	timeNow           func() time.Time
	reporter          TraceReporter
	endpoint          TargetEndpoint
	span              Span
	annotations       []Annotation
	binaryAnnotations []BinaryAnnotation

	annotationsBacking       [2]Annotation
	binaryAnnotationsBacking [2]BinaryAnnotation
}

// SetOperation sets the operation being called.
func (as *Annotations) SetOperation(operation string) {
	as.endpoint.Operation = operation
}

// GetTime returns the time using the timeNow function stored in the annotations.
func (as *Annotations) GetTime() time.Time {
	return as.timeNow()
}

// AddBinaryAnnotation adds a binary annotation.
func (as *Annotations) AddBinaryAnnotation(key string, value interface{}) {
	binaryAnnotation := BinaryAnnotation{Key: key, Value: value}
	as.binaryAnnotations = append(as.binaryAnnotations, binaryAnnotation)
}

// AddAnnotation adds a standard annotation.
func (as *Annotations) AddAnnotation(key AnnotationKey) {
	annotation := Annotation{Key: key, Timestamp: as.timeNow()}
	as.annotations = append(as.annotations, annotation)
}

// Report reports the annotations to the given trace reporter, if tracing is enabled in the span.
func (as *Annotations) Report() {
	if as.span.TracingEnabled() {
		as.reporter.Report(as.span, as.annotations, as.binaryAnnotations, as.endpoint)
	}
}
