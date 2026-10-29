package conversation

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestRecord_Validate(t *testing.T) {
	tests := []struct {
		name    string
		record  Record
		wantErr bool
	}{
		{
			name: "valid minimal record",
			record: Record{
				Hash:        "test-hash",
				Namespace:   "default",
				Model:       "test-model",
				RootImage:   "root:latest",
				WorkerImage: "worker:latest",
			},
			wantErr: false,
		},
		{
			name: "missing hash",
			record: Record{
				Namespace:   "default",
				Model:       "test-model",
				RootImage:   "root:latest",
				WorkerImage: "worker:latest",
			},
			wantErr: true,
		},
		{
			name: "missing namespace",
			record: Record{
				Hash:        "test-hash",
				Model:       "test-model",
				RootImage:   "root:latest",
				WorkerImage: "worker:latest",
			},
			wantErr: true,
		},
		{
			name: "missing model",
			record: Record{
				Hash:        "test-hash",
				Namespace:   "default",
				RootImage:   "root:latest",
				WorkerImage: "worker:latest",
			},
			wantErr: true,
		},
		{
			name: "missing root image",
			record: Record{
				Hash:        "test-hash",
				Namespace:   "default",
				Model:       "test-model",
				WorkerImage: "worker:latest",
			},
			wantErr: true,
		},
		{
			name: "missing worker image",
			record: Record{
				Hash:      "test-hash",
				Namespace: "default",
				Model:     "test-model",
				RootImage: "root:latest",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.record.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Record.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRecord_ValidateDefaults(t *testing.T) {
	record := Record{
		Hash:                    "test-hash",
		Namespace:               "default",
		Model:                   "test-model",
		RootImage:               "root:latest",
		WorkerImage:             "worker:latest",
		DispatcherMetricsListen: "  :9090  ",
	}

	err := record.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if record.Session == "" {
		t.Error("Session should be populated with default value")
	}

	if record.DispatcherImage == "" {
		t.Error("DispatcherImage should default to RootImage")
	}

	if record.ReplicaPower <= 0 {
		t.Error("ReplicaPower should default to >= 1")
	}

	if record.Queue == nil {
		t.Error("Queue should be initialized")
	}

	if record.Scaling == nil {
		t.Error("Scaling should be initialized")
	}

	if record.DispatcherMetricsListen != ":9090" {
		t.Errorf("DispatcherMetricsListen should be trimmed, got %q", record.DispatcherMetricsListen)
	}
}

func TestRecord_Marshal(t *testing.T) {
	record := Record{
		Hash:         "test-hash",
		Session:      "test-session",
		Namespace:    "default",
		Model:        "test-model",
		CreatedAt:    1609459200,
		ReplicaPower: 2,
		RootImage:    "root:latest",
		WorkerImage:  "worker:latest",
	}

	data, err := record.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshal() returned empty data")
	}

	var unmarshaled Record
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Hash != record.Hash {
		t.Errorf("Hash mismatch: got %v, want %v", unmarshaled.Hash, record.Hash)
	}
}

func TestParseRecord(t *testing.T) {
	validRecord := Record{
		Hash:        "test-hash",
		Namespace:   "default",
		Model:       "test-model",
		RootImage:   "root:latest",
		WorkerImage: "worker:latest",
	}

	data, err := json.Marshal(validRecord)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	parsed, err := ParseRecord(data)
	if err != nil {
		t.Fatalf("ParseRecord() error = %v", err)
	}

	if parsed.Hash != validRecord.Hash {
		t.Errorf("Hash mismatch: got %v, want %v", parsed.Hash, validRecord.Hash)
	}
}

func TestParseRecord_InvalidJSON(t *testing.T) {
	invalidData := []byte("not valid json")

	_, err := ParseRecord(invalidData)
	if err == nil {
		t.Error("ParseRecord() should fail on invalid JSON")
	}
}

func TestRecord_ModelParts(t *testing.T) {
	tests := []struct {
		name          string
		model         string
		wantNamespace string
		wantName      string
	}{
		{
			name:          "namespace and name",
			model:         "models/llama2",
			wantNamespace: "models",
			wantName:      "llama2",
		},
		{
			name:          "name only",
			model:         "llama2",
			wantNamespace: "",
			wantName:      "llama2",
		},
		{
			name:          "with whitespace",
			model:         " models/llama2 ",
			wantNamespace: "models",
			wantName:      "llama2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := Record{Model: tt.model}
			gotNamespace, gotName := record.ModelParts()

			if gotNamespace != tt.wantNamespace {
				t.Errorf("ModelParts() namespace = %v, want %v", gotNamespace, tt.wantNamespace)
			}
			if gotName != tt.wantName {
				t.Errorf("ModelParts() name = %v, want %v", gotName, tt.wantName)
			}
		})
	}
}

func TestRecord_SessionName(t *testing.T) {
	tests := []struct {
		name   string
		record Record
		want   string
	}{
		{
			name: "with session set",
			record: Record{
				Session: "my-session",
				Hash:    "test-hash",
			},
			want: "my-session",
		},
		{
			name: "with dllama set",
			record: Record{
				Dllama: "my-dllama",
				Hash:   "test-hash",
			},
			want: "my-dllama",
		},
		{
			name: "from hash",
			record: Record{
				Hash: "TestHash123",
			},
			want: "session-testhash123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.record.SessionName()
			if got != tt.want {
				t.Errorf("SessionName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecord_SessionNamespacedName(t *testing.T) {
	record := Record{
		Namespace: "prod",
		Session:   "my-session",
	}

	got := record.SessionNamespacedName()
	want := "prod/my-session"

	if got != want {
		t.Errorf("SessionNamespacedName() = %v, want %v", got, want)
	}
}

func TestNATSConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *NATSConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &NATSConfig{
				URL: "nats://localhost:4222",
			},
			wantErr: false,
		},
		{
			name: "with credentials secret",
			config: &NATSConfig{
				URL:               "nats://localhost:4222",
				CredentialsSecret: "my-secret",
			},
			wantErr: false,
		},
		{
			name: "missing URL",
			config: &NATSConfig{
				URL: "",
			},
			wantErr: true,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("NATSConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "lowercase alphanumeric",
			input: "test123",
			want:  "test123",
		},
		{
			name:  "uppercase to lowercase",
			input: "TEST123",
			want:  "test123",
		},
		{
			name:  "with hyphens",
			input: "test-name-123",
			want:  "test-name-123",
		},
		{
			name:  "multiple hyphens collapsed",
			input: "test---name",
			want:  "test-name",
		},
		{
			name:  "special characters converted",
			input: "test_name@123",
			want:  "testname123",
		},
		{
			name:  "leading/trailing hyphens removed",
			input: "-test-name-",
			want:  "test-name",
		},
		{
			name:  "long name truncated",
			input: "this-is-a-very-long-name-that-exceeds-the-kubernetes-limit-of-sixty-three-characters-for-resource-names",
			want:  "this-is-a-very-long-name-that-exceeds-the-kubernetes-limit-of-s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeName(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeName() = %v, want %v", got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("sanitizeName() returned name longer than 63: %d", len(got))
			}
		})
	}
}

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "alphanumeric",
			input: "Test123",
			want:  "Test123",
		},
		{
			name:  "with hyphens and underscores",
			input: "test-name_123",
			want:  "test-name_123",
		},
		{
			name:  "special chars to hyphens",
			input: "test@name#123",
			want:  "test-name-123",
		},
		{
			name:  "spaces to hyphens",
			input: "test name 123",
			want:  "test-name-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTruncateIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		value string
		max   int
		want  string
	}{
		{
			name:  "within limit",
			value: "short",
			max:   10,
			want:  "short",
		},
		{
			name:  "exact limit",
			value: "exact",
			max:   5,
			want:  "exact",
		},
		{
			name:  "exceeds limit",
			value: "this-is-a-long-identifier",
			max:   10,
			want:  "this-is-a-",
		},
		{
			name:  "zero max",
			value: "test",
			max:   0,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateIdentifier(tt.value, tt.max)
			if got != tt.want {
				t.Errorf("truncateIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecord_QueueDefaults(t *testing.T) {
	record := Record{
		Hash:        "test-hash",
		Namespace:   "default",
		Model:       "test-model",
		RootImage:   "root:latest",
		WorkerImage: "worker:latest",
	}

	err := record.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if record.Queue.BacklogSubject == "" {
		t.Error("BacklogSubject should have default value")
	}

	if record.Queue.DllamaSubjectPrefix == "" {
		t.Error("DllamaSubjectPrefix should have default value")
	}

	if record.Queue.AssignmentsBucket == "" {
		t.Error("AssignmentsBucket should have default value")
	}

	if record.Queue.StateStream == "" {
		t.Error("StateStream should have default value")
	}
}

func TestRecord_ScalingDefaults(t *testing.T) {
	record := Record{
		Hash:        "test-hash",
		Namespace:   "default",
		Model:       "test-model",
		RootImage:   "root:latest",
		WorkerImage: "worker:latest",
	}

	err := record.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if record.Scaling.MinDllamas <= 0 {
		t.Error("MinDllamas should default to > 0")
	}

	if record.Scaling.DesiredDllamas < record.Scaling.MinDllamas {
		t.Error("DesiredDllamas should be >= MinDllamas")
	}
}

func TestRecordNamespacedName(t *testing.T) {
	record := &Record{
		Namespace: "test-ns",
		Session:   "my-session",
	}

	result := record.NamespacedName()
	expected := "test-ns/my-session"
	if result != expected {
		t.Errorf("NamespacedName() = %q, want %q", result, expected)
	}
}

func TestRecordAffinityFieldsRoundTrip(t *testing.T) {
	record := Record{
		Hash:           "conv",
		Namespace:      "ns",
		Model:          "ns/model",
		RootImage:      "root:latest",
		WorkerImage:    "worker:latest",
		PinnedDllama:   "worker-7",
		Cursor:         42,
		MessageHash:    "abc123",
		LastActivityAt: 1700000000,
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed, err := ParseRecord(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.PinnedDllama != "worker-7" {
		t.Errorf("PinnedDllama = %q, want worker-7", parsed.PinnedDllama)
	}
	if parsed.Cursor != 42 {
		t.Errorf("Cursor = %d, want 42", parsed.Cursor)
	}
	if parsed.MessageHash != "abc123" {
		t.Errorf("MessageHash = %q, want abc123", parsed.MessageHash)
	}
	if parsed.LastActivityAt != 1700000000 {
		t.Errorf("LastActivityAt = %d, want 1700000000", parsed.LastActivityAt)
	}
}

func TestMessageCount(t *testing.T) {
	count, err := MessageCount([]byte(`{"messages":[{"role":"user"},{"role":"assistant"}]}`))
	if err != nil {
		t.Fatalf("MessageCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	count, err = MessageCount([]byte(`{"model":"m"}`))
	if err != nil {
		t.Fatalf("MessageCount empty: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	if _, err := MessageCount([]byte(`not-json`)); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestPrefixHashStableAndExact(t *testing.T) {
	raw := []byte(`{"model":"m","messages":[{"role":"user","content":"hi","extra":{"keep":true}},{"role":"assistant","content":"hello"}]}`)

	first, err := PrefixHash(raw, 1)
	if err != nil {
		t.Fatalf("PrefixHash: %v", err)
	}
	second, err := PrefixHash(raw, 1)
	if err != nil {
		t.Fatalf("PrefixHash: %v", err)
	}
	if first != second {
		t.Fatalf("hash is not stable: %q != %q", first, second)
	}
	if first == "" {
		t.Fatal("hash must not be empty")
	}

	full, err := PrefixHash(raw, 2)
	if err != nil {
		t.Fatalf("PrefixHash full: %v", err)
	}
	if full == first {
		t.Fatal("prefix of one message must differ from the full history")
	}

	// Unknown fields inside messages participate in the hash, so a mutated
	// message changes the prefix hash.
	mutated := []byte(`{"model":"m","messages":[{"role":"user","content":"hi","extra":{"keep":false}},{"role":"assistant","content":"hello"}]}`)
	mutatedHash, err := PrefixHash(mutated, 1)
	if err != nil {
		t.Fatalf("PrefixHash mutated: %v", err)
	}
	if mutatedHash == first {
		t.Fatal("mutating an unknown field must change the prefix hash")
	}

	empty, err := PrefixHash(raw, 0)
	if err != nil {
		t.Fatalf("PrefixHash zero: %v", err)
	}
	if empty != "" {
		t.Fatalf("hash of zero messages = %q, want empty", empty)
	}
}

type fakeKV struct {
	value       []byte
	revision    uint64
	updates     int
	failUpdates int
}

func (f *fakeKV) Get(string) (nats.KeyValueEntry, error) {
	return &fakeEntry{value: f.value, revision: f.revision}, nil
}

func (f *fakeKV) Update(_ string, value []byte, revision uint64) (uint64, error) {
	f.updates++
	if f.failUpdates > 0 {
		f.failUpdates--
		return 0, fmt.Errorf("%w: stale revision", nats.ErrKeyExists)
	}
	if revision != f.revision {
		return 0, nats.ErrKeyExists
	}
	f.value = append([]byte(nil), value...)
	f.revision++
	return f.revision, nil
}

type fakeEntry struct {
	nats.KeyValueEntry
	value    []byte
	revision uint64
}

func (e *fakeEntry) Value() []byte    { return e.value }
func (e *fakeEntry) Revision() uint64 { return e.revision }

func TestUpdateRecordRetriesOnConflict(t *testing.T) {
	record := Record{Hash: "conv", Namespace: "ns", Model: "ns/model", RootImage: "root", WorkerImage: "worker"}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	kv := &fakeKV{value: data, revision: 3, failUpdates: 1}

	updated, err := UpdateRecord(kv, "nats_ttl_conv", func(r *Record) bool {
		if r.PinnedDllama == "worker-1" {
			return false
		}
		r.PinnedDllama = "worker-1"
		return true
	})
	if err != nil {
		t.Fatalf("UpdateRecord: %v", err)
	}
	if updated.PinnedDllama != "worker-1" {
		t.Fatalf("pin = %q, want worker-1", updated.PinnedDllama)
	}
	if kv.updates != 2 {
		t.Fatalf("updates = %d, want 2 (one conflict + one success)", kv.updates)
	}

	parsed, err := ParseRecord(kv.value)
	if err != nil {
		t.Fatalf("parse stored record: %v", err)
	}
	if parsed.PinnedDllama != "worker-1" {
		t.Fatalf("stored pin = %q, want worker-1", parsed.PinnedDllama)
	}
}

func TestUpdateRecordSkipsUnchanged(t *testing.T) {
	record := Record{Hash: "conv", Namespace: "ns", Model: "ns/model", RootImage: "root", WorkerImage: "worker", PinnedDllama: "worker-1"}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	kv := &fakeKV{value: data, revision: 1}

	_, err = UpdateRecord(kv, "key", func(r *Record) bool { return r.PinnedDllama != "worker-1" })
	if err != nil {
		t.Fatalf("UpdateRecord: %v", err)
	}
	if kv.updates != 0 {
		t.Fatalf("updates = %d, want 0", kv.updates)
	}
}

func TestUpdateRecordGivesUpAfterRetries(t *testing.T) {
	record := Record{Hash: "conv", Namespace: "ns", Model: "ns/model", RootImage: "root", WorkerImage: "worker"}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	kv := &fakeKV{value: data, revision: 1, failUpdates: 10}

	_, err = UpdateRecord(kv, "key", func(r *Record) bool { r.Cursor = 1; return true })
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if kv.updates != updateRecordRetries {
		t.Fatalf("updates = %d, want %d", kv.updates, updateRecordRetries)
	}
}
