package v1

import (
	"testing"
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
