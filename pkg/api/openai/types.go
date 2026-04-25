package openai

// ChatCompletionRequest captures the subset of the OpenAI chat completion
// payload used across the koldun backends.
type ChatCompletionRequest struct {
	Model       string            `json:"model"`
	Messages    []ChatMessage     `json:"messages"`
	Tools       []Tool            `json:"tools,omitempty"`
	ToolChoice  *ToolChoice       `json:"tool_choice,omitempty"`
	Temperature *float64          `json:"temperature,omitempty"`
	MaxTokens   *int              `json:"max_tokens,omitempty"`
	Stream      bool              `json:"stream,omitempty"`
	Stop        []string          `json:"stop,omitempty"`
	User        string            `json:"user,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ChatMessage represents a single message in the conversation history.
type ChatMessage struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// Tool describes a callable function available to the model.
type Tool struct {
	Type     string   `json:"type"`
	Function Function `json:"function"`
}

// Function holds the schema for a single callable function.
type Function struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  map[string]interface{} `json:"parameters"`
}

// ToolCall represents a single function invocation requested by the model.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

// FunctionCall contains the name and serialized arguments for a tool call.
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolChoice controls whether the model may call tools.
type ToolChoice struct {
	Type     string `json:"type"`
	Function struct {
		Name string `json:"name"`
	} `json:"function,omitempty"`
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
