package natsutil

import (
	"github.com/nats-io/nats.go"
)

// NATSConnWrapper wraps a real *nats.Conn to implement the NATSConn interface.
type NATSConnWrapper struct {
	conn *nats.Conn
}

// NewNATSConnWrapper creates a new wrapper around a real NATS connection.
func NewNATSConnWrapper(conn *nats.Conn) NATSConn {
	return &NATSConnWrapper{conn: conn}
}

func (w *NATSConnWrapper) Publish(subject string, data []byte) error {
	return w.conn.Publish(subject, data)
}

func (w *NATSConnWrapper) Subscribe(subject string, handler nats.MsgHandler) (*nats.Subscription, error) {
	return w.conn.Subscribe(subject, handler)
}

func (w *NATSConnWrapper) SubscribeSync(subject string) (*nats.Subscription, error) {
	return w.conn.SubscribeSync(subject)
}

func (w *NATSConnWrapper) QueueSubscribe(subject, queue string, handler nats.MsgHandler) (*nats.Subscription, error) {
	return w.conn.QueueSubscribe(subject, queue, handler)
}

func (w *NATSConnWrapper) Flush() error {
	return w.conn.Flush()
}

func (w *NATSConnWrapper) Close() {
	w.conn.Close()
}

func (w *NATSConnWrapper) Drain() error {
	return w.conn.Drain()
}

func (w *NATSConnWrapper) JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	return w.conn.JetStream(opts...)
}

func (w *NATSConnWrapper) Status() nats.Status {
	return w.conn.Status()
}

// NATSKeyValueWrapper wraps a real nats.KeyValue to implement the NATSKeyValue interface.
type NATSKeyValueWrapper struct {
	kv nats.KeyValue
}

// NewNATSKeyValueWrapper creates a new wrapper around a real NATS KV store.
func NewNATSKeyValueWrapper(kv nats.KeyValue) NATSKeyValue {
	return &NATSKeyValueWrapper{kv: kv}
}

func (w *NATSKeyValueWrapper) Get(key string) (nats.KeyValueEntry, error) {
	return w.kv.Get(key)
}

func (w *NATSKeyValueWrapper) Put(key string, value []byte) (uint64, error) {
	return w.kv.Put(key, value)
}

func (w *NATSKeyValueWrapper) Delete(key string, opts ...nats.DeleteOpt) error {
	return w.kv.Delete(key, opts...)
}

func (w *NATSKeyValueWrapper) Keys(opts ...nats.WatchOpt) ([]string, error) {
	return w.kv.Keys(opts...)
}

func (w *NATSKeyValueWrapper) Status() (nats.KeyValueStatus, error) {
	return w.kv.Status()
}

func (w *NATSKeyValueWrapper) Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return w.kv.Watch(keys, opts...)
}

func (w *NATSKeyValueWrapper) Create(key string, value []byte) (uint64, error) {
	return w.kv.Create(key, value)
}

func (w *NATSKeyValueWrapper) Update(key string, value []byte, revision uint64) (uint64, error) {
	return w.kv.Update(key, value, revision)
}

func (w *NATSKeyValueWrapper) Purge(key string, opts ...nats.DeleteOpt) error {
	return w.kv.Purge(key, opts...)
}

// NATSJetStreamWrapper wraps a real nats.JetStreamContext
type NATSJetStreamWrapper struct {
	js nats.JetStreamContext
}

// NewNATSJetStreamWrapper creates a new wrapper around a real JetStream context.
func NewNATSJetStreamWrapper(js nats.JetStreamContext) NATSJetStream {
	return &NATSJetStreamWrapper{js: js}
}

func (w *NATSJetStreamWrapper) CreateKeyValue(cfg *nats.KeyValueConfig) (nats.KeyValue, error) {
	return w.js.CreateKeyValue(cfg)
}

func (w *NATSJetStreamWrapper) KeyValue(bucket string) (nats.KeyValue, error) {
	return w.js.KeyValue(bucket)
}

func (w *NATSJetStreamWrapper) DeleteKeyValue(bucket string) error {
	return w.js.DeleteKeyValue(bucket)
}
