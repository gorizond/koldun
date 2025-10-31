package dispatcher

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// RetryConfig defines the retry behavior for NATS operations.
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

// PublishWithRetry attempts to publish a message to NATS with exponential backoff retry logic.
// Returns an error if all retry attempts fail.
func PublishWithRetry(nc *nats.Conn, subject string, data []byte, cfg RetryConfig, log *logrus.Entry) error {
	if nc == nil {
		return errors.New("NATS connection is nil")
	}
	if subject == "" {
		return errors.New("subject is empty")
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		err := nc.Publish(subject, data)
		if err == nil {
			if attempt > 0 && log != nil {
				log.WithFields(logrus.Fields{
					"subject": subject,
					"attempt": attempt + 1,
				}).Debug("publish succeeded after retry")
			}
			return nil
		}

		lastErr = err

		// Don't retry on certain errors
		if errors.Is(err, nats.ErrConnectionClosed) ||
			errors.Is(err, nats.ErrConnectionDraining) ||
			errors.Is(err, nats.ErrBadSubject) {
			if log != nil {
				log.WithError(err).WithField("subject", subject).Warn("publish failed with non-retryable error")
			}
			return err
		}

		// Last attempt failed, don't sleep
		if attempt == cfg.MaxRetries {
			break
		}

		if log != nil {
			log.WithFields(logrus.Fields{
				"subject": subject,
				"attempt": attempt + 1,
				"backoff": backoff,
				"error":   err,
			}).Warn("publish failed, retrying")
		}

		time.Sleep(backoff)

		// Exponential backoff with max limit
		backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}

	if log != nil {
		log.WithError(lastErr).WithFields(logrus.Fields{
			"subject":    subject,
			"maxRetries": cfg.MaxRetries,
		}).Error("publish failed after all retries")
	}

	return lastErr
}

// KVPutWithRetry attempts to put a value in a NATS KV store with retry logic.
func KVPutWithRetry(kv nats.KeyValue, key string, value []byte, cfg RetryConfig, log *logrus.Entry) (uint64, error) {
	if kv == nil {
		return 0, errors.New("KV store is nil")
	}
	if key == "" {
		return 0, errors.New("key is empty")
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		rev, err := kv.Put(key, value)
		if err == nil {
			if attempt > 0 && log != nil {
				log.WithFields(logrus.Fields{
					"key":     key,
					"attempt": attempt + 1,
				}).Debug("KV put succeeded after retry")
			}
			return rev, nil
		}

		lastErr = err

		// Don't retry on certain errors
		if errors.Is(err, nats.ErrBadBucket) ||
			errors.Is(err, nats.ErrInvalidKey) {
			if log != nil {
				log.WithError(err).WithField("key", key).Warn("KV put failed with non-retryable error")
			}
			return 0, err
		}

		// Last attempt failed, don't sleep
		if attempt == cfg.MaxRetries {
			break
		}

		if log != nil {
			log.WithFields(logrus.Fields{
				"key":     key,
				"attempt": attempt + 1,
				"backoff": backoff,
				"error":   err,
			}).Warn("KV put failed, retrying")
		}

		time.Sleep(backoff)

		backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}

	if log != nil {
		log.WithError(lastErr).WithFields(logrus.Fields{
			"key":        key,
			"maxRetries": cfg.MaxRetries,
		}).Error("KV put failed after all retries")
	}

	return 0, lastErr
}

// KVDeleteWithRetry attempts to delete a key from NATS KV store with retry logic.
func KVDeleteWithRetry(kv nats.KeyValue, key string, cfg RetryConfig, log *logrus.Entry) error {
	if kv == nil {
		return errors.New("KV store is nil")
	}
	if key == "" {
		return errors.New("key is empty")
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		err := kv.Delete(key)
		if err == nil || errors.Is(err, nats.ErrKeyNotFound) {
			// Success or key doesn't exist (idempotent)
			if attempt > 0 && log != nil {
				log.WithFields(logrus.Fields{
					"key":     key,
					"attempt": attempt + 1,
				}).Debug("KV delete succeeded after retry")
			}
			return nil
		}

		lastErr = err

		// Don't retry on certain errors
		if errors.Is(err, nats.ErrBadBucket) ||
			errors.Is(err, nats.ErrInvalidKey) {
			if log != nil {
				log.WithError(err).WithField("key", key).Warn("KV delete failed with non-retryable error")
			}
			return err
		}

		// Last attempt failed, don't sleep
		if attempt == cfg.MaxRetries {
			break
		}

		if log != nil {
			log.WithFields(logrus.Fields{
				"key":     key,
				"attempt": attempt + 1,
				"backoff": backoff,
				"error":   err,
			}).Warn("KV delete failed, retrying")
		}

		time.Sleep(backoff)

		backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}

	if log != nil {
		log.WithError(lastErr).WithFields(logrus.Fields{
			"key":        key,
			"maxRetries": cfg.MaxRetries,
		}).Error("KV delete failed after all retries")
	}

	return lastErr
}
