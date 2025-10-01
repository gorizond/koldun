package conversation

import "encoding/json"

// BacklogMessage captures a queued request awaiting dispatcher assignment.
type BacklogMessage struct {
	ID        string          `json:"id"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt int64           `json:"createdAt,omitempty"`
}

// AssignmentEnvelope wraps a backlog payload for delivery to a specific Dllama worker.
type AssignmentEnvelope struct {
	AssignmentID string          `json:"assignmentId"`
	RequestID    string          `json:"requestId"`
	Payload      json.RawMessage `json:"payload"`
}

// WorkerStateEvent describes dispatcher-facing status published by Dllama workers.
type WorkerStateEvent struct {
	Dllama       string `json:"dllama"`
	State        string `json:"state"`
	AssignmentID string `json:"assignmentId,omitempty"`
	Active       int32  `json:"active,omitempty"`
	Timestamp    int64  `json:"timestamp,omitempty"`
	Error        string `json:"error,omitempty"`
}
