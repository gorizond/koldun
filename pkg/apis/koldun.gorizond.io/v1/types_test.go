package v1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
