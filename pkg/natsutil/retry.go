package natsutil

import (
	"errors"
	"time"

	"github.com/gorizond/koldun/pkg/metrics"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// PublishWithRetry attempts to publish a message to NATS with exponential backoff retry logic.
// Returns an error if all retry attempts fail.
func PublishWithRetry(nc NATSConn, subject string, data []byte, cfg RetryConfig, log *logrus.Entry) error {
	if nc == nil {
		return errors.New("NATS connection is nil")
	}
	if subject == "" {
		return errors.New("subject is empty")
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		start := time.Now()
		err := nc.Publish(subject, data)
		metrics.NATSOperationDuration.WithLabelValues("publish").Observe(time.Since(start).Seconds())

		if err == nil {
			metrics.NATSOperationsTotal.WithLabelValues("publish", "success").Inc()
			if attempt > 0 {
				metrics.NATSRetries.WithLabelValues("publish").Add(float64(attempt))
				if log != nil {
					log.WithFields(logrus.Fields{
						"subject": subject,
						"attempt": attempt + 1,
					}).Debug("publish succeeded after retry")
				}
			}
			return nil
		}

		metrics.NATSOperationsTotal.WithLabelValues("publish", "error").Inc()

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
func KVPutWithRetry(kv NATSKeyValue, key string, value []byte, cfg RetryConfig, log *logrus.Entry) (uint64, error) {
	if kv == nil {
		return 0, errors.New("KV store is nil")
	}
	if key == "" {
		return 0, errors.New("key is empty")
	}

	bucketName := "unknown"
	if kv != nil {
		if status, err := kv.Status(); err == nil {
			bucketName = status.Bucket()
		}
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		start := time.Now()
		rev, err := kv.Put(key, value)
		metrics.NATSKVOperationDuration.WithLabelValues("put", bucketName).Observe(time.Since(start).Seconds())

		if err == nil {
			metrics.NATSKVOperations.WithLabelValues("put", bucketName, "success").Inc()
			if attempt > 0 {
				metrics.NATSRetries.WithLabelValues("kv_put").Add(float64(attempt))
				if log != nil {
					log.WithFields(logrus.Fields{
						"key":     key,
						"attempt": attempt + 1,
					}).Debug("KV put succeeded after retry")
				}
			}
			return rev, nil
		}

		metrics.NATSKVOperations.WithLabelValues("put", bucketName, "error").Inc()

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
func KVDeleteWithRetry(kv NATSKeyValue, key string, cfg RetryConfig, log *logrus.Entry) error {
	if kv == nil {
		return errors.New("KV store is nil")
	}
	if key == "" {
		return errors.New("key is empty")
	}

	bucketName := "unknown"
	if kv != nil {
		if status, err := kv.Status(); err == nil {
			bucketName = status.Bucket()
		}
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		start := time.Now()
		err := kv.Delete(key) // No opts needed for our use case
		metrics.NATSKVOperationDuration.WithLabelValues("delete", bucketName).Observe(time.Since(start).Seconds())

		if err == nil || errors.Is(err, nats.ErrKeyNotFound) {
			// Success or key doesn't exist (idempotent)
			metrics.NATSKVOperations.WithLabelValues("delete", bucketName, "success").Inc()
			if attempt > 0 {
				metrics.NATSRetries.WithLabelValues("kv_delete").Add(float64(attempt))
				if log != nil {
					log.WithFields(logrus.Fields{
						"key":     key,
						"attempt": attempt + 1,
					}).Debug("KV delete succeeded after retry")
				}
			}
			return nil
		}

		metrics.NATSKVOperations.WithLabelValues("delete", bucketName, "error").Inc()

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
