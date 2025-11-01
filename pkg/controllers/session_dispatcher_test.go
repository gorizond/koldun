package controllers

import (
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
)

func TestDispatcherArgs(t *testing.T) {
	tests := []struct {
		name              string
		session           *v1.Session
		backlogSubject    string
		assignmentsBucket string
		dllamaPrefix      string
		statePrefix       string
		queueGroup        string
		ackWait           time.Duration
		expectedContains  []string
	}{
		{
			name: "basic dispatcher args",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					Hash: "test-hash-123",
					NATS: &v1.SessionNATSConfig{
						URL: "nats://nats.default.svc:4222",
					},
				},
			},
			backlogSubject:    "session.backlog.test",
			assignmentsBucket: "dispatcher-assignments",
			dllamaPrefix:      "dllama.",
			statePrefix:       "state.",
			queueGroup:        "session-dispatchers",
			ackWait:           30 * time.Second,
			expectedContains: []string{
				"dispatcher",
				"--dispatcher-hash=test-hash-123",
				"--dispatcher-nats-url=nats://nats.default.svc:4222",
				"--dispatcher-backlog-subject=session.backlog.test",
				"--dispatcher-assignments-bucket=dispatcher-assignments",
				"--dispatcher-dllama-prefix=dllama.",
				"--dispatcher-state-prefix=state.",
				"--dispatcher-queue-group=session-dispatchers",
				"--dispatcher-ack-wait=30s",
			},
		},
		{
			name: "trimmed hash and URL",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					Hash: "  hash-with-spaces  ",
					NATS: &v1.SessionNATSConfig{
						URL: "  nats://localhost:4222  ",
					},
				},
			},
			backlogSubject:    "backlog",
			assignmentsBucket: "assignments",
			dllamaPrefix:      "prefix.",
			statePrefix:       "state.",
			queueGroup:        "group",
			ackWait:           1 * time.Minute,
			expectedContains: []string{
				"--dispatcher-hash=hash-with-spaces",
				"--dispatcher-nats-url=nats://localhost:4222",
				"--dispatcher-ack-wait=1m0s",
			},
		},
		{
			name: "different ack wait durations",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					Hash: "hash",
					NATS: &v1.SessionNATSConfig{
						URL: "nats://nats:4222",
					},
				},
			},
			backlogSubject:    "backlog",
			assignmentsBucket: "assignments",
			dllamaPrefix:      "dllama.",
			statePrefix:       "state.",
			queueGroup:        "group",
			ackWait:           5 * time.Minute,
			expectedContains: []string{
				"--dispatcher-ack-wait=5m0s",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dispatcherArgs(
				tt.session,
				tt.backlogSubject,
				tt.assignmentsBucket,
				tt.dllamaPrefix,
				tt.statePrefix,
				tt.queueGroup,
				tt.ackWait,
			)

			// Check that all expected strings are present
			for _, expected := range tt.expectedContains {
				found := false
				for _, arg := range result {
					if arg == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected arg %q not found in result: %v", expected, result)
				}
			}

			// Verify first arg is always "dispatcher"
			if len(result) == 0 || result[0] != "dispatcher" {
				t.Errorf("first arg should be 'dispatcher', got: %v", result)
			}

			// Verify we have the expected number of args (1 command + 8 flags)
			if len(result) != 9 {
				t.Errorf("expected 9 args, got %d: %v", len(result), result)
			}
		})
	}
}

func TestDesiredDllamaSpecForSession(t *testing.T) {
	tests := []struct {
		name     string
		session  *v1.Session
		validate func(t *testing.T, spec v1.DllamaSpec)
	}{
		{
			name: "basic dllama spec",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					ModelRef: v1.ModelReference{
						Name: "llama-7b",
					},
					ReplicaPower: 4,
					RootImage:    "ghcr.io/gorizond/dllama-root:v1.0.0",
					WorkerImage:  "ghcr.io/gorizond/dllama-worker:v1.0.0",
					NATS: &v1.SessionNATSConfig{
						URL: "nats://nats:4222",
					},
				},
			},
			validate: func(t *testing.T, spec v1.DllamaSpec) {
				if spec.ModelRef.Name != "llama-7b" {
					t.Errorf("ModelRef.Name = %q, want 'llama-7b'", spec.ModelRef.Name)
				}
				if spec.ReplicaPower != 4 {
					t.Errorf("ReplicaPower = %d, want 4", spec.ReplicaPower)
				}
				if spec.RootImage != "ghcr.io/gorizond/dllama-root:v1.0.0" {
					t.Errorf("RootImage = %q, want 'ghcr.io/gorizond/dllama-root:v1.0.0'", spec.RootImage)
				}
				if spec.WorkerImage != "ghcr.io/gorizond/dllama-worker:v1.0.0" {
					t.Errorf("WorkerImage = %q, want 'ghcr.io/gorizond/dllama-worker:v1.0.0'", spec.WorkerImage)
				}
				if spec.NATS == nil {
					t.Fatal("NATS should not be nil")
				}
				if spec.NATS.URL != "nats://nats:4222" {
					t.Errorf("NATS.URL = %q, want 'nats://nats:4222'", spec.NATS.URL)
				}
			},
		},
		{
			name: "with NATS credentials",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					ModelRef: v1.ModelReference{
						Name: "model",
					},
					ReplicaPower: 2,
					RootImage:    "root:latest",
					WorkerImage:  "worker:latest",
					NATS: &v1.SessionNATSConfig{
						URL: "nats://nats:4222",
						CredentialsSecret: &v1.SecretReference{
							Name: "nats-creds",
						},
					},
				},
			},
			validate: func(t *testing.T, spec v1.DllamaSpec) {
				if spec.NATS == nil {
					t.Fatal("NATS should not be nil")
				}
				if spec.NATS.CredentialsSecret == nil {
					t.Fatal("NATS.CredentialsSecret should not be nil")
				}
				if spec.NATS.CredentialsSecret.Name != "nats-creds" {
					t.Errorf("CredentialsSecret.Name = %q, want 'nats-creds'",
						spec.NATS.CredentialsSecret.Name)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := desiredDllamaSpecForSession(tt.session)
			tt.validate(t, result)
		})
	}
}

func TestEnsureTrailingDot(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "string without dot",
			input:    "prefix",
			expected: "prefix.",
		},
		{
			name:     "string with dot",
			input:    "prefix.",
			expected: "prefix.",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "whitespace trimmed",
			input:    "  prefix  ",
			expected: "prefix.",
		},
		{
			name:     "whitespace with dot trimmed",
			input:    "  prefix.  ",
			expected: "prefix.",
		},
		{
			name:     "only whitespace",
			input:    "   ",
			expected: "",
		},
		{
			name:     "dot at end after whitespace",
			input:    "prefix.  ",
			expected: "prefix.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ensureTrailingDot(tt.input)
			if result != tt.expected {
				t.Errorf("ensureTrailingDot(%q) = %q, want %q",
					tt.input, result, tt.expected)
			}
		})
	}
}
