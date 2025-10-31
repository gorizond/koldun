package openai

import (
	"encoding/json"
	"testing"
)

func TestChatCompletionRequest_JSON(t *testing.T) {
	temp := 0.7
	req := ChatCompletionRequest{
		Model: "gpt-3.5-turbo",
		Messages: []ChatMessage{
			{
				Role:    "user",
				Content: "Hello, world!",
			},
		},
		Temperature: &temp,
		Stream:      true,
	}

	// Test marshaling
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal ChatCompletionRequest: %v", err)
	}

	// Test unmarshaling
	var decoded ChatCompletionRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal ChatCompletionRequest: %v", err)
	}

	// Verify fields
	if decoded.Model != req.Model {
		t.Errorf("Model = %v, want %v", decoded.Model, req.Model)
	}
	if len(decoded.Messages) != len(req.Messages) {
		t.Errorf("Messages length = %v, want %v", len(decoded.Messages), len(req.Messages))
	}
	if decoded.Temperature == nil || *decoded.Temperature != *req.Temperature {
		t.Errorf("Temperature mismatch")
	}
	if decoded.Stream != req.Stream {
		t.Errorf("Stream = %v, want %v", decoded.Stream, req.Stream)
	}
}

func TestChatMessage_JSON(t *testing.T) {
	tests := []struct {
		name    string
		message ChatMessage
	}{
		{
			name: "user message",
			message: ChatMessage{
				Role:    "user",
				Content: "Hello",
			},
		},
		{
			name: "assistant message",
			message: ChatMessage{
				Role:    "assistant",
				Content: "Hi there!",
			},
		},
		{
			name: "system message",
			message: ChatMessage{
				Role:    "system",
				Content: "You are a helpful assistant.",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.message)
			if err != nil {
				t.Fatalf("Failed to marshal ChatMessage: %v", err)
			}

			var decoded ChatMessage
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Failed to unmarshal ChatMessage: %v", err)
			}

			if decoded.Role != tt.message.Role {
				t.Errorf("Role = %v, want %v", decoded.Role, tt.message.Role)
			}
			if decoded.Content != tt.message.Content {
				t.Errorf("Content = %v, want %v", decoded.Content, tt.message.Content)
			}
		})
	}
}

func TestErrorResponse_JSON(t *testing.T) {
	errResp := ErrorResponse{
		Error: ErrorBody{
			Message: "Invalid API key",
			Type:    "invalid_request_error",
			Code:    "invalid_api_key",
		},
	}

	data, err := json.Marshal(errResp)
	if err != nil {
		t.Fatalf("Failed to marshal ErrorResponse: %v", err)
	}

	var decoded ErrorResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal ErrorResponse: %v", err)
	}

	if decoded.Error.Message != errResp.Error.Message {
		t.Errorf("Error.Message = %v, want %v", decoded.Error.Message, errResp.Error.Message)
	}
	if decoded.Error.Type != errResp.Error.Type {
		t.Errorf("Error.Type = %v, want %v", decoded.Error.Type, errResp.Error.Type)
	}
	if decoded.Error.Code != errResp.Error.Code {
		t.Errorf("Error.Code = %v, want %v", decoded.Error.Code, errResp.Error.Code)
	}
}

func TestChatCompletionRequest_MultipleMessages(t *testing.T) {
	req := ChatCompletionRequest{
		Model: "gpt-4",
		Messages: []ChatMessage{
			{Role: "system", Content: "You are helpful"},
			{Role: "user", Content: "What is AI?"},
			{Role: "assistant", Content: "AI is..."},
			{Role: "user", Content: "Tell me more"},
		},
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	var decoded ChatCompletionRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal request: %v", err)
	}

	if len(decoded.Messages) != 4 {
		t.Errorf("Messages length = %v, want 4", len(decoded.Messages))
	}

	expectedRoles := []string{"system", "user", "assistant", "user"}
	for i, msg := range decoded.Messages {
		if msg.Role != expectedRoles[i] {
			t.Errorf("Message[%d].Role = %v, want %v", i, msg.Role, expectedRoles[i])
		}
	}
}
