package v1

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestDllamaNATSConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *DllamaNATSConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
			},
			wantErr: false,
		},
		{
			name: "valid config with credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name: "nats-creds",
				},
			},
			wantErr: false,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "empty URL",
			config: &DllamaNATSConfig{
				URL: "",
			},
			wantErr: true,
		},
		{
			name: "whitespace URL",
			config: &DllamaNATSConfig{
				URL: "   ",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("DllamaNATSConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestModelStatus_DeepCopy tests the DeepCopy method for ModelStatus
func TestModelStatus_DeepCopy(t *testing.T) {
	tests := []struct {
		name   string
		status *ModelStatus
	}{
		{
			name:   "nil status",
			status: nil,
		},
		{
			name:   "empty status",
			status: &ModelStatus{},
		},
		{
			name: "status with download state",
			status: &ModelStatus{
				DownloadState: "Running",
			},
		},
		{
			name: "status with all fields",
			status: &ModelStatus{
				ObservedGeneration: 2,
				DownloadState:      "Succeeded",
				ArtifactSizeBytes:  1024000,
				DownloadJobName:    "model-download-123",
				ConversionJobName:  "model-convert-456",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *ModelStatus
			if tt.status != nil {
				got = tt.status.DeepCopy()
			}

			if tt.status == nil {
				if got != nil {
					t.Errorf("DeepCopy() of nil should return nil, got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("DeepCopy() = nil, want non-nil")
				return
			}

			if got == tt.status {
				t.Errorf("DeepCopy() returned same instance, want different instance")
			}

			if !reflect.DeepEqual(got, tt.status) {
				t.Errorf("DeepCopy() content mismatch:\ngot = %+v\nwant = %+v", got, tt.status)
			}
		})
	}
}

// TestSessionStatus_DeepCopy tests the DeepCopy method for SessionStatus
func TestSessionStatus_DeepCopy(t *testing.T) {
	tests := []struct {
		name   string
		status *SessionStatus
	}{
		{
			name:   "nil status",
			status: nil,
		},
		{
			name:   "empty status",
			status: &SessionStatus{},
		},
		{
			name: "status with workers",
			status: &SessionStatus{
				ReadyWorkers:     3,
				AvailableWorkers: 2,
			},
		},
		{
			name: "status with all fields",
			status: &SessionStatus{
				ObservedGeneration: 5,
				ReadyWorkers:       3,
				BusyWorkers:        1,
				AvailableWorkers:   2,
				Backlog:            10,
				InFlight:           2,
				ActiveRequests:     1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *SessionStatus
			if tt.status != nil {
				got = tt.status.DeepCopy()
			}

			if tt.status == nil {
				if got != nil {
					t.Errorf("DeepCopy() of nil should return nil, got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("DeepCopy() = nil, want non-nil")
				return
			}

			if got == tt.status {
				t.Errorf("DeepCopy() returned same instance, want different instance")
			}

			if !reflect.DeepEqual(got, tt.status) {
				t.Errorf("DeepCopy() content mismatch:\ngot = %+v\nwant = %+v", got, tt.status)
			}
		})
	}
}

// TestModelConversionSpec_DeepCopy tests the DeepCopy method for ModelConversionSpec
func TestModelConversionSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *ModelConversionSpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "empty spec",
			spec: &ModelConversionSpec{},
		},
		{
			name: "spec with weightsFloatType",
			spec: &ModelConversionSpec{
				WeightsFloatType: "q40",
			},
		},
		{
			name: "spec with all fields",
			spec: &ModelConversionSpec{
				WeightsFloatType: "q40",
				ConvertWeights:   "yes",
				Memory:           "8Gi",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *ModelConversionSpec
			if tt.spec != nil {
				got = tt.spec.DeepCopy()
			}

			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() of nil should return nil, got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("DeepCopy() = nil, want non-nil")
				return
			}

			if got == tt.spec {
				t.Errorf("DeepCopy() returned same instance, want different instance")
			}

			if !reflect.DeepEqual(got, tt.spec) {
				t.Errorf("DeepCopy() content mismatch:\ngot = %+v\nwant = %+v", got, tt.spec)
			}
		})
	}
}

func TestDllamaStatus_DeepCopy(t *testing.T) {
	tests := []struct {
		name   string
		status *DllamaStatus
	}{
		{
			name:   "nil status",
			status: nil,
		},
		{
			name:   "empty status",
			status: &DllamaStatus{},
		},
		{
			name: "status with conditions",
			status: &DllamaStatus{
				ObservedGeneration: 5,
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionTrue},
					{Type: "Available", Status: metav1.ConditionFalse},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.DeepCopy()

			// Check nil case
			if tt.status == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			// Verify it's a different instance
			if got == tt.status {
				t.Error("DeepCopy() returned same pointer")
			}

			// Verify fields are copied
			if got.ObservedGeneration != tt.status.ObservedGeneration {
				t.Errorf("ObservedGeneration = %v, want %v", got.ObservedGeneration, tt.status.ObservedGeneration)
			}

			// Verify Conditions are deep copied
			if tt.status.Conditions != nil {
				if len(got.Conditions) != len(tt.status.Conditions) {
					t.Errorf("Conditions length = %v, want %v", len(got.Conditions), len(tt.status.Conditions))
				}
				if len(got.Conditions) > 0 && &got.Conditions[0] == &tt.status.Conditions[0] {
					t.Error("DeepCopy() didn't deep copy Conditions slice")
				}
			}
		})
	}
}

func TestModelObjectStorageSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *ModelObjectStorageSpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "empty spec",
			spec: &ModelObjectStorageSpec{},
		},
		{
			name: "spec without secret",
			spec: &ModelObjectStorageSpec{
				Endpoint:         "s3://storage.example.com",
				BucketForSource:  "source-bucket",
				BucketForConvert: "convert-bucket",
			},
		},
		{
			name: "spec with secret",
			spec: &ModelObjectStorageSpec{
				Endpoint: "s3://storage.example.com",
				SecretRef: &SecretReference{
					Name:      "storage-creds",
					Namespace: "storage-ns",
				},
				BucketForSource:  "source-bucket",
				BucketForConvert: "convert-bucket",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			// Check nil case
			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			// Verify it's a different instance
			if got == tt.spec {
				t.Error("DeepCopy() returned same pointer")
			}

			// Verify fields are copied
			if got.Endpoint != tt.spec.Endpoint {
				t.Errorf("Endpoint = %v, want %v", got.Endpoint, tt.spec.Endpoint)
			}
			if got.BucketForSource != tt.spec.BucketForSource {
				t.Errorf("BucketForSource = %v, want %v", got.BucketForSource, tt.spec.BucketForSource)
			}
			if got.BucketForConvert != tt.spec.BucketForConvert {
				t.Errorf("BucketForConvert = %v, want %v", got.BucketForConvert, tt.spec.BucketForConvert)
			}

			// Verify SecretRef is deep copied
			if tt.spec.SecretRef != nil {
				if got.SecretRef == tt.spec.SecretRef {
					t.Error("DeepCopy() didn't deep copy SecretRef")
				}
				if got.SecretRef.Name != tt.spec.SecretRef.Name {
					t.Errorf("SecretRef.Name = %v, want %v", got.SecretRef.Name, tt.spec.SecretRef.Name)
				}
			}
		})
	}
}

func TestDllamaNATSConfig_ToRootConfig(t *testing.T) {
	tests := []struct {
		name   string
		config *DllamaNATSConfig
		want   *RootNATSConfig
	}{
		{
			name: "with credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name:      "nats-creds",
					Namespace: "default",
				},
			},
			want: &RootNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name:      "nats-creds",
					Namespace: "default",
				},
			},
		},
		{
			name: "without credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
			},
			want: &RootNATSConfig{
				URL: "nats://localhost:4222",
			},
		},
		{
			name:   "nil config",
			config: nil,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.ToRootConfig()

			if got == nil && tt.want == nil {
				return
			}
			if got == nil || tt.want == nil {
				t.Errorf("DllamaNATSConfig.ToRootConfig() = %v, want %v", got, tt.want)
				return
			}

			if got.URL != tt.want.URL {
				t.Errorf("DllamaNATSConfig.ToRootConfig().URL = %v, want %v", got.URL, tt.want.URL)
			}

			if (got.CredentialsSecret == nil) != (tt.want.CredentialsSecret == nil) {
				t.Errorf("DllamaNATSConfig.ToRootConfig().CredentialsSecret mismatch")
				return
			}

			if got.CredentialsSecret != nil && tt.want.CredentialsSecret != nil {
				if got.CredentialsSecret.Name != tt.want.CredentialsSecret.Name {
					t.Errorf("CredentialsSecret.Name = %v, want %v", got.CredentialsSecret.Name, tt.want.CredentialsSecret.Name)
				}
				if got.CredentialsSecret.Namespace != tt.want.CredentialsSecret.Namespace {
					t.Errorf("CredentialsSecret.Namespace = %v, want %v", got.CredentialsSecret.Namespace, tt.want.CredentialsSecret.Namespace)
				}
			}
		})
	}
}

func TestDllamaNATSConfig_ToWorkerConfig(t *testing.T) {
	tests := []struct {
		name   string
		config *DllamaNATSConfig
		want   *WorkerNATSConfig
	}{
		{
			name: "with credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name:      "nats-creds",
					Namespace: "default",
				},
			},
			want: &WorkerNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name:      "nats-creds",
					Namespace: "default",
				},
			},
		},
		{
			name: "without credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
			},
			want: &WorkerNATSConfig{
				URL: "nats://localhost:4222",
			},
		},
		{
			name:   "nil config",
			config: nil,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.ToWorkerConfig()

			if got == nil && tt.want == nil {
				return
			}
			if got == nil || tt.want == nil {
				t.Errorf("DllamaNATSConfig.ToWorkerConfig() = %v, want %v", got, tt.want)
				return
			}

			if got.URL != tt.want.URL {
				t.Errorf("DllamaNATSConfig.ToWorkerConfig().URL = %v, want %v", got.URL, tt.want.URL)
			}

			if (got.CredentialsSecret == nil) != (tt.want.CredentialsSecret == nil) {
				t.Errorf("DllamaNATSConfig.ToWorkerConfig().CredentialsSecret mismatch")
				return
			}

			if got.CredentialsSecret != nil && tt.want.CredentialsSecret != nil {
				if got.CredentialsSecret.Name != tt.want.CredentialsSecret.Name {
					t.Errorf("CredentialsSecret.Name = %v, want %v", got.CredentialsSecret.Name, tt.want.CredentialsSecret.Name)
				}
				if got.CredentialsSecret.Namespace != tt.want.CredentialsSecret.Namespace {
					t.Errorf("CredentialsSecret.Namespace = %v, want %v", got.CredentialsSecret.Namespace, tt.want.CredentialsSecret.Namespace)
				}
			}
		})
	}
}

func TestDllamaNATSConfig_DeepCopy(t *testing.T) {
	tests := []struct {
		name   string
		config *DllamaNATSConfig
	}{
		{
			name: "with credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name:      "nats-creds",
					Namespace: "default",
				},
			},
		},
		{
			name: "without credentials",
			config: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
			},
		},
		{
			name:   "nil config",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.DeepCopy()

			if got == nil && tt.config == nil {
				return
			}
			if got == nil || tt.config == nil {
				t.Errorf("DllamaNATSConfig.DeepCopy() = %v, want %v", got, tt.config)
				return
			}

			// Verify values are equal
			if got.URL != tt.config.URL {
				t.Errorf("DeepCopy().URL = %v, want %v", got.URL, tt.config.URL)
			}

			// Verify it's a true deep copy (different pointers)
			if tt.config.CredentialsSecret != nil {
				if got.CredentialsSecret == tt.config.CredentialsSecret {
					t.Error("DeepCopy() returned same pointer for CredentialsSecret")
				}
				if got.CredentialsSecret.Name != tt.config.CredentialsSecret.Name {
					t.Errorf("CredentialsSecret.Name = %v, want %v", got.CredentialsSecret.Name, tt.config.CredentialsSecret.Name)
				}
			}
		})
	}
}

func TestDllamaSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec DllamaSpec
	}{
		{
			name: "with NATS config",
			spec: DllamaSpec{
				ModelRef: ModelReference{
					Kind: "Model",
					Name: "test-model",
				},
				ReplicaPower: 3,
				RootImage:    "root:latest",
				WorkerImage:  "worker:latest",
				NATS: &DllamaNATSConfig{
					URL: "nats://localhost:4222",
					CredentialsSecret: &SecretReference{
						Name: "nats-creds",
					},
				},
			},
		},
		{
			name: "without NATS config",
			spec: DllamaSpec{
				ModelRef: ModelReference{
					Kind: "Model",
					Name: "test-model",
				},
				ReplicaPower: 2,
				RootImage:    "root:latest",
				WorkerImage:  "worker:latest",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			// Verify values are equal
			if got.ModelRef.Name != tt.spec.ModelRef.Name {
				t.Errorf("DeepCopy().ModelRef.Name = %v, want %v", got.ModelRef.Name, tt.spec.ModelRef.Name)
			}
			if got.ReplicaPower != tt.spec.ReplicaPower {
				t.Errorf("DeepCopy().ReplicaPower = %v, want %v", got.ReplicaPower, tt.spec.ReplicaPower)
			}

			// Verify NATS deep copy
			if tt.spec.NATS != nil {
				if got.NATS == tt.spec.NATS {
					t.Error("DeepCopy() returned same pointer for NATS config")
				}
				if got.NATS.URL != tt.spec.NATS.URL {
					t.Errorf("NATS.URL = %v, want %v", got.NATS.URL, tt.spec.NATS.URL)
				}
			} else if got.NATS != nil {
				t.Errorf("DeepCopy().NATS = %v, want nil", got.NATS)
			}
		})
	}
}

func TestRootNATSConfig_GetURL(t *testing.T) {
	tests := []struct {
		name   string
		config *RootNATSConfig
		want   string
	}{
		{
			name: "with URL",
			config: &RootNATSConfig{
				URL: "nats://localhost:4222",
			},
			want: "nats://localhost:4222",
		},
		{
			name:   "nil config",
			config: nil,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetURL()
			if got != tt.want {
				t.Errorf("RootNATSConfig.GetURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorkerSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *WorkerSpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "empty spec",
			spec: &WorkerSpec{},
		},
		{
			name: "spec with args",
			spec: &WorkerSpec{
				Args: []string{"arg1", "arg2", "arg3"},
			},
		},
		{
			name: "spec with cache",
			spec: &WorkerSpec{
				CacheSpec: &CacheSpec{
					Endpoint: "s3://cache.example.com",
					Bucket:   "models-cache",
				},
			},
		},
		{
			name: "spec with NATS",
			spec: &WorkerSpec{
				NATS: &WorkerNATSConfig{
					URL: "nats://localhost:4222",
					CredentialsSecret: &SecretReference{
						Name: "nats-creds",
					},
				},
			},
		},
		{
			name: "full spec",
			spec: &WorkerSpec{
				Args: []string{"--verbose", "--debug"},
				CacheSpec: &CacheSpec{
					Endpoint: "s3://cache.local",
					Bucket:   "koldun-models",
				},
				NATS: &WorkerNATSConfig{
					URL: "nats://nats.local:4222",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			// Check nil case
			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			// Verify it's a different instance
			if got == tt.spec {
				t.Error("DeepCopy() returned same pointer")
			}

			// Verify Args are copied
			if tt.spec.Args != nil {
				if &got.Args[0] == &tt.spec.Args[0] {
					t.Error("DeepCopy() didn't deep copy Args slice")
				}
				if !reflect.DeepEqual(got.Args, tt.spec.Args) {
					t.Errorf("Args = %v, want %v", got.Args, tt.spec.Args)
				}
			}

			// Verify CacheSpec is deep copied
			if tt.spec.CacheSpec != nil {
				if got.CacheSpec == tt.spec.CacheSpec {
					t.Error("DeepCopy() didn't deep copy CacheSpec")
				}
				if got.CacheSpec.Endpoint != tt.spec.CacheSpec.Endpoint {
					t.Errorf("CacheSpec.Endpoint = %v, want %v", got.CacheSpec.Endpoint, tt.spec.CacheSpec.Endpoint)
				}
				if got.CacheSpec.Bucket != tt.spec.CacheSpec.Bucket {
					t.Errorf("CacheSpec.Bucket = %v, want %v", got.CacheSpec.Bucket, tt.spec.CacheSpec.Bucket)
				}
			}

			// Verify NATS is deep copied
			if tt.spec.NATS != nil {
				if got.NATS == tt.spec.NATS {
					t.Error("DeepCopy() didn't deep copy NATS")
				}
				if got.NATS.URL != tt.spec.NATS.URL {
					t.Errorf("NATS.URL = %v, want %v", got.NATS.URL, tt.spec.NATS.URL)
				}
			}
		})
	}
}

func TestWorkerNATSConfig_DeepCopy(t *testing.T) {
	tests := []struct {
		name   string
		config *WorkerNATSConfig
	}{
		{
			name:   "nil config",
			config: nil,
		},
		{
			name:   "empty config",
			config: &WorkerNATSConfig{},
		},
		{
			name: "config with URL only",
			config: &WorkerNATSConfig{
				URL: "nats://localhost:4222",
			},
		},
		{
			name: "config with credentials",
			config: &WorkerNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name: "nats-creds",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.DeepCopy()

			// Check nil case
			if tt.config == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			// Verify it's a different instance
			if got == tt.config {
				t.Error("DeepCopy() returned same pointer")
			}

			// Verify URL is copied
			if got.URL != tt.config.URL {
				t.Errorf("URL = %v, want %v", got.URL, tt.config.URL)
			}

			// Verify CredentialsSecret is deep copied
			if tt.config.CredentialsSecret != nil {
				if got.CredentialsSecret == tt.config.CredentialsSecret {
					t.Error("DeepCopy() didn't deep copy CredentialsSecret")
				}
				if got.CredentialsSecret.Name != tt.config.CredentialsSecret.Name {
					t.Errorf("CredentialsSecret.Name = %v, want %v", got.CredentialsSecret.Name, tt.config.CredentialsSecret.Name)
				}
			}
		})
	}
}

func TestCacheSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *CacheSpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "empty spec",
			spec: &CacheSpec{},
		},
		{
			name: "full spec",
			spec: &CacheSpec{
				Endpoint: "s3://cache.example.com",
				Bucket:   "koldun-models-cache",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			// Check nil case
			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			// Verify it's a different instance
			if got == tt.spec {
				t.Error("DeepCopy() returned same pointer")
			}

			// Verify fields are copied
			if got.Endpoint != tt.spec.Endpoint {
				t.Errorf("Endpoint = %v, want %v", got.Endpoint, tt.spec.Endpoint)
			}
			if got.Bucket != tt.spec.Bucket {
				t.Errorf("Bucket = %v, want %v", got.Bucket, tt.spec.Bucket)
			}
		})
	}
}

func TestAddKnownTypes(t *testing.T) {
	scheme := runtime.NewScheme()

	if err := addKnownTypes(scheme); err != nil {
		t.Fatalf("addKnownTypes() error = %v", err)
	}

	testCases := []struct {
		kind     string
		expected runtime.Object
	}{
		{"Dllama", &Dllama{}},
		{"DllamaList", &DllamaList{}},
		{"Model", &Model{}},
		{"ModelList", &ModelList{}},
		{"Root", &Root{}},
		{"RootList", &RootList{}},
		{"Worker", &Worker{}},
		{"WorkerList", &WorkerList{}},
		{"Ingress", &Ingress{}},
		{"IngressList", &IngressList{}},
		{"Session", &Session{}},
		{"SessionList", &SessionList{}},
	}

	for _, tt := range testCases {
		t.Run(tt.kind, func(t *testing.T) {
			obj, err := scheme.New(SchemeGroupVersion.WithKind(tt.kind))
			if err != nil {
				t.Fatalf("scheme.New(%q) error = %v", tt.kind, err)
			}
			if reflect.TypeOf(obj) != reflect.TypeOf(tt.expected) {
				t.Fatalf("scheme.New(%q) returned %T, want %T", tt.kind, obj, tt.expected)
			}
		})
	}
}

func TestDllamaDeepCopyVariants(t *testing.T) {
	original := &Dllama{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Dllama",
			APIVersion: SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			Labels: map[string]string{
				"app": "dllama",
			},
		},
		Spec: DllamaSpec{
			ModelRef: ModelReference{
				Name:      "model",
				Namespace: "ml",
			},
			ReplicaPower: 2,
			RootImage:    "dllama-root:latest",
			WorkerImage:  "dllama-worker:latest",
			NATS: &DllamaNATSConfig{
				URL: "nats://localhost:4222",
				CredentialsSecret: &SecretReference{
					Name:      "nats-creds",
					Namespace: "system",
				},
			},
		},
		Status: DllamaStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	copyObj := original.DeepCopyObject()
	if copyObj == original {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if reflect.TypeOf(copyObj) != reflect.TypeOf(original) {
		t.Fatalf("DeepCopyObject() returned %T, want %T", copyObj, original)
	}
	if !reflect.DeepEqual(copyObj, original) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", original, copyObj)
	}

	mutated := original.DeepCopy()
	mutated.ObjectMeta.Labels["new"] = "label"
	mutated.Spec.NATS.CredentialsSecret.Name = "changed"
	mutated.Status.Conditions[0].Status = metav1.ConditionFalse

	if reflect.DeepEqual(mutated, original) {
		t.Fatal("DeepCopy() did not produce an independent copy")
	}
}

func TestDllamaListDeepCopyVariants(t *testing.T) {
	list := &DllamaList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "DllamaList",
			APIVersion: SchemeGroupVersion.String(),
		},
		ListMeta: metav1.ListMeta{
			ResourceVersion: "123",
			Continue:        "token",
		},
		Items: []Dllama{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "item-1",
				},
				Spec: DllamaSpec{
					ModelRef: ModelReference{Name: "model"},
				},
			},
		},
	}

	copyObj := list.DeepCopyObject()
	if copyObj == list {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if reflect.TypeOf(copyObj) != reflect.TypeOf(list) {
		t.Fatalf("DeepCopyObject() returned %T, want %T", copyObj, list)
	}
	if !reflect.DeepEqual(copyObj, list) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", list, copyObj)
	}

	list.Items[0].ObjectMeta.Name = "modified"
	if reflect.DeepEqual(copyObj, list) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestModelDeepCopyVariants(t *testing.T) {
	original := &Model{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Model",
			APIVersion: SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "sample-model",
			Annotations: map[string]string{
				"team": "ml",
			},
		},
		Spec: ModelSpec{
			LaunchOptions: []string{"--max-tokens=128"},
			ObjectStorage: &ModelObjectStorageSpec{
				Endpoint: "https://object.storage",
				SecretRef: &SecretReference{
					Name: "storage-secret",
				},
				BucketForSource:  "source-bucket",
				BucketForConvert: "convert-bucket",
			},
			Download: &ModelDownloadSpec{
				Image:   "downloader:latest",
				Command: []string{"download"},
				Args:    []string{"--full"},
				HuggingFaceTokenSecretRef: &SecretReference{
					Name: "hf-secret",
				},
				Memory:      "1Gi",
				ChunkMaxMiB: 64,
				Concurrency: 2,
			},
			Conversion: &ModelConversionSpec{
				Image:            "converter:latest",
				Command:          []string{"convert"},
				Args:             []string{"--opt"},
				WeightsFloatType: "q40",
				ConvertWeights:   "hf",
				OutputPath:       "s3://converted",
				Memory:           "2Gi",
				RcloneImage:      "rclone/rclone:1.67",
				ToolsImage:       "alpine:3.18",
				ConverterVersion: "v1.0.0",
				Dependencies: map[string]string{
					"torch": "2.1.0",
				},
			},
			PV: &ModelPVSpec{
				AccessModes: []string{"ReadWriteMany"},
				VolumeAttributes: map[string]string{
					"fstype": "xfs",
				},
				PVCAccessModes: []string{"ReadWriteMany"},
			},
		},
		Status: ModelStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
			ObservedGeneration: 3,
		},
	}

	copyObj := original.DeepCopyObject()
	if copyObj == original {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, original) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", original, copyObj)
	}

	original.Spec.ObjectStorage.SecretRef.Name = "changed"
	if reflect.DeepEqual(copyObj, original) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestModelListDeepCopyVariants(t *testing.T) {
	list := &ModelList{
		ListMeta: metav1.ListMeta{
			ResourceVersion: "rv",
		},
		Items: []Model{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "model-a",
				},
				Spec: ModelSpec{
					LaunchOptions: []string{"--fast"},
				},
			},
		},
	}

	copyObj := list.DeepCopyObject()
	if copyObj == list {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, list) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", list, copyObj)
	}

	list.Items[0].Spec.LaunchOptions[0] = "--slow"
	if reflect.DeepEqual(copyObj, list) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestRootDeepCopyVariants(t *testing.T) {
	original := &Root{
		ObjectMeta: metav1.ObjectMeta{
			Name: "root-a",
			Labels: map[string]string{
				"component": "root",
			},
		},
		Spec: RootSpec{
			Args: []string{"--debug"},
			CacheSpec: &CacheSpec{
				Endpoint: "s3://cache",
				Bucket:   "cache-bucket",
			},
			WorkerSelector: map[string]string{
				"role": "worker",
			},
			NATS: &RootNATSConfig{
				URL: "nats://root:4222",
				CredentialsSecret: &SecretReference{
					Name: "root-creds",
				},
			},
			Memory: &RootMemorySpec{
				OverheadMaxRatio: func() *float64 { v := 1.5; return &v }(),
			},
		},
		Status: RootStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Healthy",
					Status: metav1.ConditionUnknown,
				},
			},
		},
	}

	copyObj := original.DeepCopyObject()
	if copyObj == original {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, original) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", original, copyObj)
	}

	original.Spec.NATS.CredentialsSecret.Name = "mutated"
	if reflect.DeepEqual(copyObj, original) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestRootListDeepCopyVariants(t *testing.T) {
	list := &RootList{
		Items: []Root{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "root-list-item",
				},
			},
		},
	}

	copyObj := list.DeepCopyObject()
	if copyObj == list {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, list) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", list, copyObj)
	}

	list.Items[0].ObjectMeta.Name = "changed"
	if reflect.DeepEqual(copyObj, list) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestRootNATSConfigDeepCopy(t *testing.T) {
	original := &RootNATSConfig{
		URL: "nats://root:4222",
		CredentialsSecret: &SecretReference{
			Name:      "root-creds",
			Namespace: "system",
		},
	}

	copyConfig := original.DeepCopy()
	if copyConfig == original {
		t.Fatal("DeepCopy() returned the same pointer")
	}
	if !reflect.DeepEqual(copyConfig, original) {
		t.Fatalf("DeepCopy() mismatch:\noriginal = %#v\ncopy = %#v", original, copyConfig)
	}

	original.CredentialsSecret.Name = "mutated"
	if reflect.DeepEqual(copyConfig, original) {
		t.Fatal("DeepCopy() copy mutated after modifying original")
	}
}

func TestWorkerDeepCopyVariants(t *testing.T) {
	original := &Worker{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
		},
		Spec: WorkerSpec{
			Args: []string{"--serve"},
			CacheSpec: &CacheSpec{
				Endpoint: "s3://cache",
				Bucket:   "worker-cache",
			},
			NATS: &WorkerNATSConfig{
				URL: "nats://worker:4222",
				CredentialsSecret: &SecretReference{
					Name: "worker-creds",
				},
			},
		},
		Status: WorkerStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionFalse,
				},
			},
		},
	}

	copyObj := original.DeepCopyObject()
	if copyObj == original {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, original) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", original, copyObj)
	}

	original.Spec.CacheSpec.Bucket = "mutated"
	if reflect.DeepEqual(copyObj, original) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestWorkerListDeepCopyVariants(t *testing.T) {
	list := &WorkerList{
		Items: []Worker{
			{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}},
		},
	}

	copyObj := list.DeepCopyObject()
	if copyObj == list {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, list) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", list, copyObj)
	}

	list.Items[0].ObjectMeta.Name = "changed"
	if reflect.DeepEqual(copyObj, list) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestSessionDeepCopyVariants(t *testing.T) {
	now := metav1.NewTime(time.Now().UTC())
	duration := metav1.Duration{Duration: 5 * time.Minute}

	original := &Session{
		ObjectMeta: metav1.ObjectMeta{
			Name: "session-1",
			Labels: map[string]string{
				"user": "alice",
			},
		},
		Spec: SessionSpec{
			Queue: &SessionQueueSpec{
				AckTimeout: &duration,
			},
			Scaling: &SessionScalingSpec{
				MinDllamas:           1,
				MaxDllamas:           5,
				ScaleUpBacklog:       2,
				ScaleDownIdleSeconds: 30,
				DesiredDllamas:       3,
			},
			NATS: &SessionNATSConfig{
				URL: "nats://session:4222",
				CredentialsSecret: &SecretReference{
					Name: "session-creds",
				},
			},
			TTL: &duration,
		},
		Status: SessionStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
				},
			},
			Workers: []SessionWorker{
				{
					Name:          "worker-1",
					LastHeartbeat: &now,
				},
			},
		},
	}

	copyObj := original.DeepCopyObject()
	if copyObj == original {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, original) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", original, copyObj)
	}

	original.Spec.Queue.AckTimeout.Duration = 10 * time.Minute
	if reflect.DeepEqual(copyObj, original) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestSessionListDeepCopyVariants(t *testing.T) {
	list := &SessionList{
		Items: []Session{
			{ObjectMeta: metav1.ObjectMeta{Name: "session-a"}},
		},
	}

	copyObj := list.DeepCopyObject()
	if copyObj == list {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, list) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", list, copyObj)
	}

	list.Items[0].ObjectMeta.Name = "changed"
	if reflect.DeepEqual(copyObj, list) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestIngressDeepCopyVariants(t *testing.T) {
	convTTL := metav1.Duration{Duration: time.Minute}
	respTimeout := metav1.Duration{Duration: 2 * time.Minute}
	now := metav1.Now()

	original := &Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ingress-1",
		},
		Spec: IngressSpec{
			Backend: IngressBackendSpec{
				Image:           "ingress:latest",
				ImagePullPolicy: "Always",
				RootImage:       "root:latest",
				WorkerImage:     "worker:latest",
				DispatcherImage: "dispatcher:latest",
				ReplicaPower:    4,
				HashSecret:      "secret",
				AllowAnonymous:  true,
				NATS: IngressNATSConfig{
					URL:                "nats://ingress:4222",
					ConversationBucket: "conv",
					ModelsBucket:       "models",
					TokensBucket:       "tokens",
					ModelPrefix:        "model-",
					TokenPrefix:        "token-",
					TTLPrefix:          "ttl-",
				},
				ConversationTTL: &convTTL,
				ResponseTimeout: &respTimeout,
				SessionScaling: &IngressSessionScalingSpec{
					MinDllamas:           1,
					MaxDllamas:           3,
					ScaleUpBacklog:       5,
					ScaleDownIdleSeconds: 30,
				},
				ExtraArgs: []string{"--log-level=debug"},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
				RootMemory: &IngressRootMemorySpec{
					OverheadMaxRatio: func() *float64 { v := 2.0; return &v }(),
				},
			},
			Route: IngressRouteSpec{
				Host:             "example.com",
				Path:             "/chat",
				PathType:         "Prefix",
				IngressClassName: "nginx",
				Annotations: map[string]string{
					"nginx.ingress.kubernetes.io/rewrite-target": "/",
				},
				TLS: []IngressTLSSpec{
					{
						SecretName: "tls-secret",
						Hosts:      []string{"example.com", "api.example.com"},
					},
				},
			},
			Service: &IngressServiceSpec{
				Type: "ClusterIP",
				Port: 8443,
			},
		},
		Status: IngressStatus{
			ObservedGeneration: 2,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
				},
			},
			BackendServiceName: "backend-service",
			IngressName:        "ingress-resource",
		},
	}

	copyObj := original.DeepCopyObject()
	if copyObj == original {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, original) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", original, copyObj)
	}

	original.Spec.Route.Annotations["new"] = "annotation"
	if reflect.DeepEqual(copyObj, original) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

func TestIngressListDeepCopyVariants(t *testing.T) {
	list := &IngressList{
		Items: []Ingress{
			{ObjectMeta: metav1.ObjectMeta{Name: "ingress-a"}},
		},
	}

	copyObj := list.DeepCopyObject()
	if copyObj == list {
		t.Fatal("DeepCopyObject() returned the same pointer")
	}
	if !reflect.DeepEqual(copyObj, list) {
		t.Fatalf("DeepCopyObject() mismatch:\noriginal = %#v\ncopy = %#v", list, copyObj)
	}

	list.Items[0].ObjectMeta.Name = "changed"
	if reflect.DeepEqual(copyObj, list) {
		t.Fatal("DeepCopyObject() copy mutated after modifying original")
	}
}

// TestModelList_DeepCopy tests the DeepCopy methods for ModelList
func TestModelList_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		list *ModelList
	}{
		{
			name: "nil list",
			list: nil,
		},
		{
			name: "empty list",
			list: &ModelList{},
		},
		{
			name: "list with metadata",
			list: &ModelList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ModelList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "12345",
				},
			},
		},
		{
			name: "list with single item",
			list: &ModelList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ModelList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				Items: []Model{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "model1",
							Namespace: "default",
						},
						Spec: ModelSpec{
							SourceURL: "http://example.com/model",
						},
					},
				},
			},
		},
		{
			name: "list with multiple items",
			list: &ModelList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "ModelList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "67890",
					Continue:        "next-page",
				},
				Items: []Model{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "model1",
							Namespace: "ns1",
						},
						Spec: ModelSpec{
							SourceURL: "http://example.com/model1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "model2",
							Namespace: "ns2",
						},
						Spec: ModelSpec{
							SourceURL: "http://example.com/model2",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test DeepCopy
			var got *ModelList
			if tt.list != nil {
				got = tt.list.DeepCopy()
			}

			if tt.list == nil {
				if got != nil {
					t.Errorf("DeepCopy() of nil should return nil, got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("DeepCopy() = nil, want non-nil")
				return
			}

			// Verify it's a different instance
			if got == tt.list {
				t.Errorf("DeepCopy() returned same instance, want different instance")
			}

			// Verify content is equal
			if !reflect.DeepEqual(got, tt.list) {
				t.Errorf("DeepCopy() content mismatch:\ngot = %+v\nwant = %+v", got, tt.list)
			}

			// Test DeepCopyInto
			into := &ModelList{}
			tt.list.DeepCopyInto(into)

			if !reflect.DeepEqual(into, tt.list) {
				t.Errorf("DeepCopyInto() content mismatch:\ngot = %+v\nwant = %+v", into, tt.list)
			}

			// Test DeepCopyObject
			obj := tt.list.DeepCopyObject()
			if obj == nil {
				t.Errorf("DeepCopyObject() = nil, want non-nil")
				return
			}

			gotObj, ok := obj.(*ModelList)
			if !ok {
				t.Errorf("DeepCopyObject() returned %T, want *ModelList", obj)
				return
			}

			if !reflect.DeepEqual(gotObj, tt.list) {
				t.Errorf("DeepCopyObject() content mismatch:\ngot = %+v\nwant = %+v", gotObj, tt.list)
			}
		})
	}
}

// TestSessionList_DeepCopy tests the DeepCopy methods for SessionList
func TestSessionList_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		list *SessionList
	}{
		{
			name: "nil list",
			list: nil,
		},
		{
			name: "empty list",
			list: &SessionList{},
		},
		{
			name: "list with metadata",
			list: &SessionList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "SessionList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "12345",
				},
			},
		},
		{
			name: "list with single item",
			list: &SessionList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "SessionList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				Items: []Session{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "session1",
							Namespace: "default",
						},
						Spec: SessionSpec{
							Hash: "session1-hash",
						},
					},
				},
			},
		},
		{
			name: "list with multiple items",
			list: &SessionList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "SessionList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "67890",
					Continue:        "next-page",
				},
				Items: []Session{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "session1",
							Namespace: "ns1",
						},
						Spec: SessionSpec{
							Hash: "session1-hash",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "session2",
							Namespace: "ns2",
						},
						Spec: SessionSpec{
							Hash: "session2-hash",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test DeepCopy
			var got *SessionList
			if tt.list != nil {
				got = tt.list.DeepCopy()
			}

			if tt.list == nil {
				if got != nil {
					t.Errorf("DeepCopy() of nil should return nil, got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("DeepCopy() = nil, want non-nil")
				return
			}

			// Verify it's a different instance
			if got == tt.list {
				t.Errorf("DeepCopy() returned same instance, want different instance")
			}

			// Verify content is equal
			if !reflect.DeepEqual(got, tt.list) {
				t.Errorf("DeepCopy() content mismatch:\ngot = %+v\nwant = %+v", got, tt.list)
			}

			// Test DeepCopyInto
			into := &SessionList{}
			tt.list.DeepCopyInto(into)

			if !reflect.DeepEqual(into, tt.list) {
				t.Errorf("DeepCopyInto() content mismatch:\ngot = %+v\nwant = %+v", into, tt.list)
			}

			// Test DeepCopyObject
			obj := tt.list.DeepCopyObject()
			if obj == nil {
				t.Errorf("DeepCopyObject() = nil, want non-nil")
				return
			}

			gotObj, ok := obj.(*SessionList)
			if !ok {
				t.Errorf("DeepCopyObject() returned %T, want *SessionList", obj)
				return
			}

			if !reflect.DeepEqual(gotObj, tt.list) {
				t.Errorf("DeepCopyObject() content mismatch:\ngot = %+v\nwant = %+v", gotObj, tt.list)
			}
		})
	}
}

// TestDllamaList_DeepCopy tests the DeepCopy methods for DllamaList
func TestDllamaList_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		list *DllamaList
	}{
		{
			name: "nil list",
			list: nil,
		},
		{
			name: "empty list",
			list: &DllamaList{},
		},
		{
			name: "list with metadata",
			list: &DllamaList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "DllamaList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "12345",
				},
			},
		},
		{
			name: "list with single item",
			list: &DllamaList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "DllamaList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				Items: []Dllama{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "dllama1",
							Namespace: "default",
						},
						Spec: DllamaSpec{
							ModelRef: ModelReference{
								Name: "model1",
							},
						},
					},
				},
			},
		},
		{
			name: "list with multiple items",
			list: &DllamaList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "DllamaList",
					APIVersion: "koldun.gorizond.io/v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "67890",
				},
				Items: []Dllama{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "dllama1",
							Namespace: "ns1",
						},
						Spec: DllamaSpec{
							ModelRef: ModelReference{
								Name: "model1",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "dllama2",
							Namespace: "ns2",
						},
						Spec: DllamaSpec{
							ModelRef: ModelReference{
								Name: "model2",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test DeepCopy
			var got *DllamaList
			if tt.list != nil {
				got = tt.list.DeepCopy()
			}

			if tt.list == nil {
				if got != nil {
					t.Errorf("DeepCopy() of nil should return nil, got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("DeepCopy() = nil, want non-nil")
				return
			}

			// Verify it's a different instance
			if got == tt.list {
				t.Errorf("DeepCopy() returned same instance, want different instance")
			}

			// Verify content is equal
			if !reflect.DeepEqual(got, tt.list) {
				t.Errorf("DeepCopy() content mismatch:\ngot = %+v\nwant = %+v", got, tt.list)
			}

			// Test DeepCopyInto
			into := &DllamaList{}
			tt.list.DeepCopyInto(into)

			if !reflect.DeepEqual(into, tt.list) {
				t.Errorf("DeepCopyInto() content mismatch:\ngot = %+v\nwant = %+v", into, tt.list)
			}

			// Test DeepCopyObject
			obj := tt.list.DeepCopyObject()
			if obj == nil {
				t.Errorf("DeepCopyObject() = nil, want non-nil")
				return
			}

			gotObj, ok := obj.(*DllamaList)
			if !ok {
				t.Errorf("DeepCopyObject() returned %T, want *DllamaList", obj)
				return
			}

			if !reflect.DeepEqual(gotObj, tt.list) {
				t.Errorf("DeepCopyObject() content mismatch:\ngot = %+v\nwant = %+v", gotObj, tt.list)
			}
		})
	}
}

// TestModelPVSpec_DeepCopy tests the DeepCopy method for ModelPVSpec
func TestModelPVSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *ModelPVSpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "empty spec",
			spec: &ModelPVSpec{},
		},
		{
			name: "full spec",
			spec: &ModelPVSpec{
				StorageClassName: "fast-storage",
				Capacity:         "10Gi",
				AccessModes:      []string{"ReadWriteOnce", "ReadOnlyMany"},
				ReclaimPolicy:    "Retain",
				CSIDriver:        "ru.yandex.s3.csi",
				VolumeAttributes: map[string]string{"type": "ssd", "region": "us-west"},
				PVCAccessModes:   []string{"ReadWriteOnce"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			if got == tt.spec {
				t.Error("DeepCopy() returned same pointer, not a copy")
			}

			if !reflect.DeepEqual(got, tt.spec) {
				t.Errorf("DeepCopy() = %+v, want %+v", got, tt.spec)
			}

			// Verify deep copy of slices
			if len(tt.spec.AccessModes) > 0 {
				got.AccessModes[0] = "modified"
				if tt.spec.AccessModes[0] == "modified" {
					t.Error("DeepCopy() did not deep copy AccessModes slice")
				}
			}

			// Verify deep copy of maps
			if len(tt.spec.VolumeAttributes) > 0 {
				got.VolumeAttributes["new"] = "value"
				if _, exists := tt.spec.VolumeAttributes["new"]; exists {
					t.Error("DeepCopy() did not deep copy VolumeAttributes map")
				}
			}
		})
	}
}

// TestRootList_DeepCopy tests the DeepCopy methods for RootList
func TestRootList_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		list *RootList
	}{
		{
			name: "nil list",
			list: nil,
		},
		{
			name: "empty list",
			list: &RootList{},
		},
		{
			name: "list with items",
			list: &RootList{
				Items: []Root{
					{
						Spec: RootSpec{
							ModelRef: "model-1",
							Image:    "image-1",
						},
					},
					{
						Spec: RootSpec{
							ModelRef: "model-2",
							Image:    "image-2",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test DeepCopy
			got := tt.list.DeepCopy()

			if tt.list == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			if got == tt.list {
				t.Error("DeepCopy() returned same pointer")
			}

			if !reflect.DeepEqual(got, tt.list) {
				t.Errorf("DeepCopy() mismatch:\ngot = %+v\nwant = %+v", got, tt.list)
			}

			// Test DeepCopyInto
			var into RootList
			tt.list.DeepCopyInto(&into)
			if !reflect.DeepEqual(&into, tt.list) {
				t.Errorf("DeepCopyInto() mismatch:\ngot = %+v\nwant = %+v", &into, tt.list)
			}

			// Test DeepCopyObject
			gotObj := tt.list.DeepCopyObject()
			if gotList, ok := gotObj.(*RootList); !ok {
				t.Errorf("DeepCopyObject() type = %T, want *RootList", gotObj)
			} else if !reflect.DeepEqual(gotList, tt.list) {
				t.Errorf("DeepCopyObject() content mismatch:\ngot = %+v\nwant = %+v", gotObj, tt.list)
			}
		})
	}
}

// TestRootSpec_DeepCopy tests the DeepCopy method for RootSpec
func TestRootSpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *RootSpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "simple spec",
			spec: &RootSpec{
				ModelRef: "test-model",
				Image:    "test-image",
			},
		},
		{
			name: "spec with memory config",
			spec: &RootSpec{
				ModelRef: "test-model",
				Image:    "test-image",
				Memory: &RootMemorySpec{
					OverheadMaxRatio: func() *float64 { v := 2.0; return &v }(),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			if got == tt.spec {
				t.Error("DeepCopy() returned same pointer")
			}

			if !reflect.DeepEqual(got, tt.spec) {
				t.Errorf("DeepCopy() = %+v, want %+v", got, tt.spec)
			}
		})
	}
}

// TestRootMemorySpec_DeepCopy tests the DeepCopy method for RootMemorySpec
func TestRootMemorySpec_DeepCopy(t *testing.T) {
	tests := []struct {
		name string
		spec *RootMemorySpec
	}{
		{
			name: "nil spec",
			spec: nil,
		},
		{
			name: "empty spec",
			spec: &RootMemorySpec{},
		},
		{
			name: "spec with value",
			spec: &RootMemorySpec{
				OverheadMaxRatio: func() *float64 { v := 1.5; return &v }(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.spec.DeepCopy()

			if tt.spec == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			if got == tt.spec {
				t.Error("DeepCopy() returned same pointer")
			}

			if !reflect.DeepEqual(got, tt.spec) {
				t.Errorf("DeepCopy() = %+v, want %+v", got, tt.spec)
			}
		})
	}
}

// TestRootStatus_DeepCopy tests the DeepCopy method for RootStatus
func TestRootStatus_DeepCopy(t *testing.T) {
	tests := []struct {
		name   string
		status *RootStatus
	}{
		{
			name:   "nil status",
			status: nil,
		},
		{
			name:   "empty status",
			status: &RootStatus{},
		},
		{
			name: "status with conditions",
			status: &RootStatus{
				Conditions: []metav1.Condition{
					{
						Type:   "Ready",
						Status: metav1.ConditionTrue,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.DeepCopy()

			if tt.status == nil {
				if got != nil {
					t.Errorf("DeepCopy() = %v, want nil", got)
				}
				return
			}

			if got == tt.status {
				t.Error("DeepCopy() returned same pointer")
			}

			if !reflect.DeepEqual(got, tt.status) {
				t.Errorf("DeepCopy() = %+v, want %+v", got, tt.status)
			}
		})
	}
}
