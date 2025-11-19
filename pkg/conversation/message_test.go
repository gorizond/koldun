package conversation

import (
	"encoding/json"
	"testing"
)

func TestBacklogMessage_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		msg  BacklogMessage
	}{
		{
			name: "complete message",
			msg: BacklogMessage{
				ID:        "test-id-123",
				Payload:   json.RawMessage(`{"key":"value"}`),
				CreatedAt: 1637000000,
			},
		},
		{
			name: "message without createdAt",
			msg: BacklogMessage{
				ID:      "test-id-456",
				Payload: json.RawMessage(`{"data":"test"}`),
			},
		},
		{
			name: "message with empty payload",
			msg: BacklogMessage{
				ID:        "empty-payload",
				Payload:   json.RawMessage(`{}`),
				CreatedAt: 1637000001,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(tt.msg)
			if err != nil {
				t.Fatalf("failed to marshal BacklogMessage: %v", err)
			}

			// Unmarshal back
			var got BacklogMessage
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("failed to unmarshal BacklogMessage: %v", err)
			}

			// Verify fields
			if got.ID != tt.msg.ID {
				t.Errorf("ID mismatch: got %q, want %q", got.ID, tt.msg.ID)
			}
			if string(got.Payload) != string(tt.msg.Payload) {
				t.Errorf("Payload mismatch: got %q, want %q", string(got.Payload), string(tt.msg.Payload))
			}
			if got.CreatedAt != tt.msg.CreatedAt {
				t.Errorf("CreatedAt mismatch: got %d, want %d", got.CreatedAt, tt.msg.CreatedAt)
			}
		})
	}
}

func TestBacklogMessage_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		want    BacklogMessage
		wantErr bool
	}{
		{
			name: "valid JSON",
			json: `{"id":"msg-1","payload":{"test":true},"createdAt":1637000000}`,
			want: BacklogMessage{
				ID:        "msg-1",
				Payload:   json.RawMessage(`{"test":true}`),
				CreatedAt: 1637000000,
			},
		},
		{
			name: "missing createdAt field",
			json: `{"id":"msg-2","payload":{"test":false}}`,
			want: BacklogMessage{
				ID:      "msg-2",
				Payload: json.RawMessage(`{"test":false}`),
			},
		},
		{
			name:    "invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got BacklogMessage
			err := json.Unmarshal([]byte(tt.json), &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.ID != tt.want.ID {
				t.Errorf("ID = %q, want %q", got.ID, tt.want.ID)
			}
			if string(got.Payload) != string(tt.want.Payload) {
				t.Errorf("Payload = %q, want %q", string(got.Payload), string(tt.want.Payload))
			}
			if got.CreatedAt != tt.want.CreatedAt {
				t.Errorf("CreatedAt = %d, want %d", got.CreatedAt, tt.want.CreatedAt)
			}
		})
	}
}

func TestAssignmentEnvelope_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		env  AssignmentEnvelope
	}{
		{
			name: "complete envelope",
			env: AssignmentEnvelope{
				AssignmentID: "assign-123",
				RequestID:    "req-456",
				Payload:      json.RawMessage(`{"model":"llama","prompt":"test"}`),
			},
		},
		{
			name: "envelope with empty payload",
			env: AssignmentEnvelope{
				AssignmentID: "assign-789",
				RequestID:    "req-101",
				Payload:      json.RawMessage(`{}`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(tt.env)
			if err != nil {
				t.Fatalf("failed to marshal AssignmentEnvelope: %v", err)
			}

			// Unmarshal back
			var got AssignmentEnvelope
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("failed to unmarshal AssignmentEnvelope: %v", err)
			}

			// Verify fields
			if got.AssignmentID != tt.env.AssignmentID {
				t.Errorf("AssignmentID mismatch: got %q, want %q", got.AssignmentID, tt.env.AssignmentID)
			}
			if got.RequestID != tt.env.RequestID {
				t.Errorf("RequestID mismatch: got %q, want %q", got.RequestID, tt.env.RequestID)
			}
			if string(got.Payload) != string(tt.env.Payload) {
				t.Errorf("Payload mismatch: got %q, want %q", string(got.Payload), string(tt.env.Payload))
			}
		})
	}
}

func TestAssignmentEnvelope_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		want    AssignmentEnvelope
		wantErr bool
	}{
		{
			name: "valid JSON",
			json: `{"assignmentId":"a1","requestId":"r1","payload":{"data":"value"}}`,
			want: AssignmentEnvelope{
				AssignmentID: "a1",
				RequestID:    "r1",
				Payload:      json.RawMessage(`{"data":"value"}`),
			},
		},
		{
			name:    "invalid JSON",
			json:    `{bad json}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got AssignmentEnvelope
			err := json.Unmarshal([]byte(tt.json), &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.AssignmentID != tt.want.AssignmentID {
				t.Errorf("AssignmentID = %q, want %q", got.AssignmentID, tt.want.AssignmentID)
			}
			if got.RequestID != tt.want.RequestID {
				t.Errorf("RequestID = %q, want %q", got.RequestID, tt.want.RequestID)
			}
			if string(got.Payload) != string(tt.want.Payload) {
				t.Errorf("Payload = %q, want %q", string(got.Payload), string(tt.want.Payload))
			}
		})
	}
}

func TestWorkerStateEvent_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name  string
		event WorkerStateEvent
	}{
		{
			name: "complete event",
			event: WorkerStateEvent{
				Dllama:       "dllama-test",
				State:        "ready",
				AssignmentID: "assign-123",
				Active:       5,
				Timestamp:    1637000000,
				Error:        "",
			},
		},
		{
			name: "event with error",
			event: WorkerStateEvent{
				Dllama:    "dllama-error",
				State:     "failed",
				Timestamp: 1637000001,
				Error:     "connection timeout",
			},
		},
		{
			name: "minimal event",
			event: WorkerStateEvent{
				Dllama: "dllama-minimal",
				State:  "idle",
			},
		},
		{
			name: "busy event",
			event: WorkerStateEvent{
				Dllama:       "dllama-busy",
				State:        "busy",
				AssignmentID: "assign-456",
				Active:       1,
				Timestamp:    1637000002,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			data, err := json.Marshal(tt.event)
			if err != nil {
				t.Fatalf("failed to marshal WorkerStateEvent: %v", err)
			}

			// Unmarshal back
			var got WorkerStateEvent
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("failed to unmarshal WorkerStateEvent: %v", err)
			}

			// Verify fields
			if got.Dllama != tt.event.Dllama {
				t.Errorf("Dllama mismatch: got %q, want %q", got.Dllama, tt.event.Dllama)
			}
			if got.State != tt.event.State {
				t.Errorf("State mismatch: got %q, want %q", got.State, tt.event.State)
			}
			if got.AssignmentID != tt.event.AssignmentID {
				t.Errorf("AssignmentID mismatch: got %q, want %q", got.AssignmentID, tt.event.AssignmentID)
			}
			if got.Active != tt.event.Active {
				t.Errorf("Active mismatch: got %d, want %d", got.Active, tt.event.Active)
			}
			if got.Timestamp != tt.event.Timestamp {
				t.Errorf("Timestamp mismatch: got %d, want %d", got.Timestamp, tt.event.Timestamp)
			}
			if got.Error != tt.event.Error {
				t.Errorf("Error mismatch: got %q, want %q", got.Error, tt.event.Error)
			}
		})
	}
}

func TestWorkerStateEvent_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		want    WorkerStateEvent
		wantErr bool
	}{
		{
			name: "valid complete JSON",
			json: `{"dllama":"d1","state":"running","assignmentId":"a1","active":3,"timestamp":1637000000,"error":""}`,
			want: WorkerStateEvent{
				Dllama:       "d1",
				State:        "running",
				AssignmentID: "a1",
				Active:       3,
				Timestamp:    1637000000,
				Error:        "",
			},
		},
		{
			name: "minimal valid JSON",
			json: `{"dllama":"d2","state":"idle"}`,
			want: WorkerStateEvent{
				Dllama: "d2",
				State:  "idle",
			},
		},
		{
			name: "JSON with error field",
			json: `{"dllama":"d3","state":"error","error":"test error message"}`,
			want: WorkerStateEvent{
				Dllama: "d3",
				State:  "error",
				Error:  "test error message",
			},
		},
		{
			name:    "invalid JSON",
			json:    `{invalid`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got WorkerStateEvent
			err := json.Unmarshal([]byte(tt.json), &got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.Dllama != tt.want.Dllama {
				t.Errorf("Dllama = %q, want %q", got.Dllama, tt.want.Dllama)
			}
			if got.State != tt.want.State {
				t.Errorf("State = %q, want %q", got.State, tt.want.State)
			}
			if got.AssignmentID != tt.want.AssignmentID {
				t.Errorf("AssignmentID = %q, want %q", got.AssignmentID, tt.want.AssignmentID)
			}
			if got.Active != tt.want.Active {
				t.Errorf("Active = %d, want %d", got.Active, tt.want.Active)
			}
			if got.Timestamp != tt.want.Timestamp {
				t.Errorf("Timestamp = %d, want %d", got.Timestamp, tt.want.Timestamp)
			}
			if got.Error != tt.want.Error {
				t.Errorf("Error = %q, want %q", got.Error, tt.want.Error)
			}
		})
	}
}
