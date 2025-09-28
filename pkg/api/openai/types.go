package openai

// ChatCompletionRequest captures the subset of the OpenAI chat completion
// payload used across the koldun backends.
type ChatCompletionRequest struct {
	Model       string            `json:"model"`
	Messages    []ChatMessage     `json:"messages"`
	Temperature *float64          `json:"temperature,omitempty"`
	MaxTokens   *int              `json:"max_tokens,omitempty"`
	Stream      bool              `json:"stream,omitempty"`
	Stop        []string          `json:"stop,omitempty"`
	User        string            `json:"user,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ChatMessage represents a single message in the conversation history.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

// ErrorResponse follows the shape returned by the OpenAI API on failures.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody provides a simplified error payload for clients.
type ErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type,omitempty"`
	Code    string `json:"code,omitempty"`
}
