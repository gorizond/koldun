package conversation

import (
	"encoding/json"
	"testing"
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
