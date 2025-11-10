package natsutil

//go:generate mockgen -destination=mocks/mock_nats.go -package=mocks github.com/gorizond/koldun/pkg/natsutil NATSConn,NATSKeyValue

import (
	"time"

	"github.com/nats-io/nats.go"
)

// NATSConn is an interface that wraps the essential NATS connection methods
// needed for testing. This allows us to mock NATS operations in unit tests.
type NATSConn interface {
	// Publish publishes data to the given subject.
	Publish(subject string, data []byte) error

	// Subscribe creates a subscription to the given subject.
	Subscribe(subject string, handler nats.MsgHandler) (*nats.Subscription, error)

	// SubscribeSync creates a synchronous subscription.
	SubscribeSync(subject string) (*nats.Subscription, error)

	// QueueSubscribe creates a queue subscription.
	QueueSubscribe(subject, queue string, handler nats.MsgHandler) (*nats.Subscription, error)

	// Flush flushes the connection.
	Flush() error

	// Close closes the connection.
	Close()

	// Drain drains the connection.
	Drain() error

	// JetStream returns a JetStream context.
	JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error)

	// Status returns the current connection status.
	Status() nats.Status
}

// NATSKeyValue is an interface that wraps NATS KV store operations
// for testability.
type NATSKeyValue interface {
	// Get retrieves a value by key.
	Get(key string) (nats.KeyValueEntry, error)

	// Put stores a value for a key.
	Put(key string, value []byte) (uint64, error)

	// Delete deletes a key.
	Delete(key string, opts ...nats.DeleteOpt) error

	// Keys returns all keys in the bucket.
	Keys(opts ...nats.WatchOpt) ([]string, error)

	// Status returns the status of the bucket.
	Status() (nats.KeyValueStatus, error)

	// Watch watches for changes in the bucket.
	Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error)

	// Create creates a new key-value pair only if the key does not exist.
	Create(key string, value []byte) (uint64, error)

	// Update updates an existing key-value pair.
	Update(key string, value []byte, revision uint64) (uint64, error)

	// Purge purges all keys in the bucket.
	Purge(key string, opts ...nats.DeleteOpt) error
}

// NATSJetStream is an interface for JetStream operations
type NATSJetStream interface {
	// CreateKeyValue creates a key-value bucket.
	CreateKeyValue(cfg *nats.KeyValueConfig) (nats.KeyValue, error)

	// KeyValue returns an existing key-value bucket.
	KeyValue(bucket string) (nats.KeyValue, error)

	// DeleteKeyValue deletes a key-value bucket.
	DeleteKeyValue(bucket string) error
}

// RetryConfig defines retry behavior for NATS operations.
// Moved here from dispatcher package for reusability.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
}

// DefaultRetryConfig returns sensible defaults for NATS retry operations.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
	}
}
