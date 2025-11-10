package natsutil_test

import (
	"errors"
	"testing"
	"time"

	"github.com/gorizond/koldun/pkg/natsutil"
	"github.com/gorizond/koldun/pkg/natsutil/mocks"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"go.uber.org/mock/gomock"
)

func TestPublishWithRetry_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mocks.NewMockNATSConn(ctrl)
	mockConn.EXPECT().Publish("test.subject", []byte("data")).Return(nil).Times(1)

	cfg := natsutil.DefaultRetryConfig()
	err := natsutil.PublishWithRetry(mockConn, "test.subject", []byte("data"), cfg, nil)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestPublishWithRetry_NilConnection(t *testing.T) {
	cfg := natsutil.DefaultRetryConfig()
	err := natsutil.PublishWithRetry(nil, "test.subject", []byte("data"), cfg, nil)

	if err == nil {
		t.Error("Expected error for nil connection")
	}
	if err.Error() != "NATS connection is nil" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestPublishWithRetry_EmptySubject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mocks.NewMockNATSConn(ctrl)

	cfg := natsutil.DefaultRetryConfig()
	err := natsutil.PublishWithRetry(mockConn, "", []byte("data"), cfg, nil)

	if err == nil {
		t.Error("Expected error for empty subject")
	}
	if err.Error() != "subject is empty" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestPublishWithRetry_SuccessAfterRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mocks.NewMockNATSConn(ctrl)
	tempErr := errors.New("temporary error")

	gomock.InOrder(
		mockConn.EXPECT().Publish("test.subject", []byte("data")).Return(tempErr),
		mockConn.EXPECT().Publish("test.subject", []byte("data")).Return(tempErr),
		mockConn.EXPECT().Publish("test.subject", []byte("data")).Return(nil),
	)

	cfg := natsutil.RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New().WithField("test", "retry")
	err := natsutil.PublishWithRetry(mockConn, "test.subject", []byte("data"), cfg, logger)

	if err != nil {
		t.Errorf("Expected success after retries, got %v", err)
	}
}

func TestPublishWithRetry_NonRetryableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mocks.NewMockNATSConn(ctrl)
	mockConn.EXPECT().Publish("test.subject", []byte("data")).Return(nats.ErrConnectionClosed).Times(1)

	cfg := natsutil.DefaultRetryConfig()
	logger := logrus.New().WithField("test", "retry")
	err := natsutil.PublishWithRetry(mockConn, "test.subject", []byte("data"), cfg, logger)

	if err == nil {
		t.Error("Expected error for closed connection")
	}
	if !errors.Is(err, nats.ErrConnectionClosed) {
		t.Errorf("Expected ErrConnectionClosed, got %v", err)
	}
}

func TestPublishWithRetry_AllRetriesFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mocks.NewMockNATSConn(ctrl)
	tempErr := errors.New("persistent error")

	// Expect MaxRetries + 1 calls (initial + retries)
	mockConn.EXPECT().Publish("test.subject", []byte("data")).Return(tempErr).Times(4)

	cfg := natsutil.RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New().WithField("test", "retry")
	err := natsutil.PublishWithRetry(mockConn, "test.subject", []byte("data"), cfg, logger)

	if err == nil {
		t.Error("Expected error after all retries failed")
	}
	if err.Error() != "persistent error" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestKVPutWithRetry_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockNATSKeyValue(ctrl)
	mockStatus := &mockKeyValueStatus{bucket: "test-bucket"}

	mockKV.EXPECT().Status().Return(mockStatus, nil).AnyTimes()
	mockKV.EXPECT().Put("test-key", []byte("value")).Return(uint64(1), nil).Times(1)

	cfg := natsutil.DefaultRetryConfig()
	rev, err := natsutil.KVPutWithRetry(mockKV, "test-key", []byte("value"), cfg, nil)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if rev != 1 {
		t.Errorf("Expected revision 1, got %d", rev)
	}
}

func TestKVPutWithRetry_NilKV(t *testing.T) {
	cfg := natsutil.DefaultRetryConfig()
	_, err := natsutil.KVPutWithRetry(nil, "test-key", []byte("value"), cfg, nil)

	if err == nil {
		t.Error("Expected error for nil KV store")
	}
	if err.Error() != "KV store is nil" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestKVPutWithRetry_EmptyKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockNATSKeyValue(ctrl)

	cfg := natsutil.DefaultRetryConfig()
	_, err := natsutil.KVPutWithRetry(mockKV, "", []byte("value"), cfg, nil)

	if err == nil {
		t.Error("Expected error for empty key")
	}
	if err.Error() != "key is empty" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestKVPutWithRetry_NonRetryableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockNATSKeyValue(ctrl)
	mockStatus := &mockKeyValueStatus{bucket: "test-bucket"}

	mockKV.EXPECT().Status().Return(mockStatus, nil).AnyTimes()
	mockKV.EXPECT().Put("invalid key", []byte("value")).Return(uint64(0), nats.ErrInvalidKey).Times(1)

	cfg := natsutil.DefaultRetryConfig()
	logger := logrus.New().WithField("test", "retry")
	_, err := natsutil.KVPutWithRetry(mockKV, "invalid key", []byte("value"), cfg, logger)

	if err == nil {
		t.Error("Expected error for invalid key")
	}
	if !errors.Is(err, nats.ErrInvalidKey) {
		t.Errorf("Expected ErrInvalidKey, got %v", err)
	}
}

func TestKVDeleteWithRetry_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockNATSKeyValue(ctrl)
	mockStatus := &mockKeyValueStatus{bucket: "test-bucket"}

	mockKV.EXPECT().Status().Return(mockStatus, nil).AnyTimes()
	mockKV.EXPECT().Delete("test-key").Return(nil).Times(1)

	cfg := natsutil.DefaultRetryConfig()
	err := natsutil.KVDeleteWithRetry(mockKV, "test-key", cfg, nil)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestKVDeleteWithRetry_KeyNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockNATSKeyValue(ctrl)
	mockStatus := &mockKeyValueStatus{bucket: "test-bucket"}

	mockKV.EXPECT().Status().Return(mockStatus, nil).AnyTimes()
	mockKV.EXPECT().Delete("missing-key").Return(nats.ErrKeyNotFound).Times(1)

	cfg := natsutil.DefaultRetryConfig()
	err := natsutil.KVDeleteWithRetry(mockKV, "missing-key", cfg, nil)

	// ErrKeyNotFound is treated as success (idempotent)
	if err != nil {
		t.Errorf("Expected no error for missing key, got %v", err)
	}
}

func TestKVDeleteWithRetry_AllRetriesFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockNATSKeyValue(ctrl)
	mockStatus := &mockKeyValueStatus{bucket: "test-bucket"}
	tempErr := errors.New("persistent error")

	mockKV.EXPECT().Status().Return(mockStatus, nil).AnyTimes()
	mockKV.EXPECT().Delete("test-key").Return(tempErr).Times(4) // initial + 3 retries

	cfg := natsutil.RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	logger := logrus.New().WithField("test", "retry")
	err := natsutil.KVDeleteWithRetry(mockKV, "test-key", cfg, logger)

	if err == nil {
		t.Error("Expected error after all retries failed")
	}
	if err.Error() != "persistent error" {
		t.Errorf("Unexpected error: %v", err)
	}
}

// mockKeyValueStatus implements nats.KeyValueStatus for testing
type mockKeyValueStatus struct {
	bucket string
}

func (m *mockKeyValueStatus) Bucket() string                                 { return m.bucket }
func (m *mockKeyValueStatus) Values() uint64                                 { return 0 }
func (m *mockKeyValueStatus) History() int64                                 { return 0 }
func (m *mockKeyValueStatus) TTL() time.Duration                             { return 0 }
func (m *mockKeyValueStatus) BucketByteLimit() int                           { return 0 }
func (m *mockKeyValueStatus) Replicas() int                                  { return 0 }
func (m *mockKeyValueStatus) BackingStore() string                           { return "" }
func (m *mockKeyValueStatus) StreamInfo() *nats.StreamInfo                   { return nil }
func (m *mockKeyValueStatus) IsCompressed() bool                             { return false }
func (m *mockKeyValueStatus) Metadata() map[string]string                    { return nil }
func (m *mockKeyValueStatus) Description() string                            { return "" }
func (m *mockKeyValueStatus) MaxBucketSize() int64                           { return 0 }
func (m *mockKeyValueStatus) MaxValueSize() int32                            { return 0 }
func (m *mockKeyValueStatus) Bytes() uint64                                  { return 0 }
func (m *mockKeyValueStatus) IsRePublish() bool                              { return false }
func (m *mockKeyValueStatus) RePublish() *nats.RePublish                     { return nil }
func (m *mockKeyValueStatus) IsMirror() bool                                 { return false }
func (m *mockKeyValueStatus) Mirror() *nats.StreamSource                     { return nil }
func (m *mockKeyValueStatus) Sources() []*nats.StreamSource                  { return nil }
func (m *mockKeyValueStatus) Storage() nats.StorageType                      { return nats.FileStorage }
func (m *mockKeyValueStatus) Placement() *nats.Placement                     { return nil }
func (m *mockKeyValueStatus) SubjectTransform() *nats.SubjectTransformConfig { return nil }
