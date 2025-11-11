package testutil

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/gorizond/koldun/pkg/natsutil"
	"github.com/nats-io/nats.go"
)

// PublishedMessage captures subject/payload pairs observed by RecordingNATSConn.
type PublishedMessage struct {
	Subject string
	Data    []byte
}

// RecordingNATSConn is a lightweight natsutil.NATSConn that records Publish calls.
type RecordingNATSConn struct {
	mu         sync.Mutex
	Published  []PublishedMessage
	PublishErr error
}

func (r *RecordingNATSConn) Publish(subject string, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	copyPayload := append([]byte(nil), data...)
	r.Published = append(r.Published, PublishedMessage{Subject: subject, Data: copyPayload})

	if r.PublishErr != nil {
		return r.PublishErr
	}

	return nil
}

func (r *RecordingNATSConn) Subscribe(string, nats.MsgHandler) (*nats.Subscription, error) {
	return nil, errors.New("recording connection Subscribe not implemented")
}

func (r *RecordingNATSConn) SubscribeSync(string) (*nats.Subscription, error) {
	return nil, errors.New("recording connection SubscribeSync not implemented")
}

func (r *RecordingNATSConn) QueueSubscribe(string, string, nats.MsgHandler) (*nats.Subscription, error) {
	return nil, errors.New("recording connection QueueSubscribe not implemented")
}

func (r *RecordingNATSConn) Flush() error { return nil }

func (r *RecordingNATSConn) Close() {}

func (r *RecordingNATSConn) Drain() error { return nil }

func (r *RecordingNATSConn) JetStream(...nats.JSOpt) (nats.JetStreamContext, error) {
	return nil, errors.New("recording connection JetStream not implemented")
}

func (r *RecordingNATSConn) Status() nats.Status { return nats.CONNECTED }

var _ natsutil.NATSConn = (*RecordingNATSConn)(nil)

// SequenceTransport replays a sequence of HTTP status codes for RoundTrip calls.
type SequenceTransport struct {
	Statuses []int
	mu       sync.Mutex
	calls    int
}

func (s *SequenceTransport) RoundTrip(*http.Request) (*http.Response, error) {
	s.mu.Lock()
	idx := s.calls
	s.calls++
	s.mu.Unlock()

	status := http.StatusOK
	switch {
	case len(s.Statuses) == 0:
		status = http.StatusOK
	case idx < len(s.Statuses):
		status = s.Statuses[idx]
	default:
		status = s.Statuses[len(s.Statuses)-1]
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader("{}")),
		Header:     make(http.Header),
	}, nil
}

func (s *SequenceTransport) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

var _ http.RoundTripper = (*SequenceTransport)(nil)
