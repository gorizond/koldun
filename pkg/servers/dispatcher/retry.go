package dispatcher

import (
	"github.com/gorizond/koldun/pkg/natsutil"
	"github.com/sirupsen/logrus"
)

// RetryConfig is an alias for natsutil.RetryConfig for backward compatibility.
// Deprecated: Use natsutil.RetryConfig directly.
type RetryConfig = natsutil.RetryConfig

// DefaultRetryConfig returns sensible defaults for NATS retry operations.
// Deprecated: Use natsutil.DefaultRetryConfig() directly.
func DefaultRetryConfig() RetryConfig {
	return natsutil.DefaultRetryConfig()
}

// PublishWithRetry attempts to publish a message to NATS with exponential backoff retry logic.
// Deprecated: Use natsutil.PublishWithRetry directly.
func PublishWithRetry(nc natsutil.NATSConn, subject string, data []byte, cfg RetryConfig, log *logrus.Entry) error {
	return natsutil.PublishWithRetry(nc, subject, data, cfg, log)
}

// KVPutWithRetry attempts to put a value in a NATS KV store with retry logic.
// Deprecated: Use natsutil.KVPutWithRetry directly.
func KVPutWithRetry(kv natsutil.NATSKeyValue, key string, value []byte, cfg RetryConfig, log *logrus.Entry) (uint64, error) {
	return natsutil.KVPutWithRetry(kv, key, value, cfg, log)
}

// KVDeleteWithRetry attempts to delete a key from NATS KV store with retry logic.
// Deprecated: Use natsutil.KVDeleteWithRetry directly.
func KVDeleteWithRetry(kv natsutil.NATSKeyValue, key string, cfg RetryConfig, log *logrus.Entry) error {
	return natsutil.KVDeleteWithRetry(kv, key, cfg, log)
}
