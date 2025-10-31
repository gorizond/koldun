package conversation

import (
	"testing"
	"time"
)

func TestMakeID_ValidInputs(t *testing.T) {
	tests := []struct {
		name      string
		userKey   string
		chatID    string
		chatStart string
		opts      *IDOptions
		wantErr   bool
	}{
		{
			name:      "basic valid inputs",
			userKey:   "user123",
			chatID:    "chat456",
			chatStart: "1609459200",
			opts:      nil,
			wantErr:   false,
		},
		{
			name:      "with RFC3339 timestamp",
			userKey:   "alice@example.com",
			chatID:    "session-xyz",
			chatStart: "2021-01-01T00:00:00Z",
			opts:      nil,
			wantErr:   false,
		},
		{
			name:      "without human prefix",
			userKey:   "bot-worker",
			chatID:    "task-123",
			chatStart: "1609459200",
			opts:      &IDOptions{HumanPrefix: false},
			wantErr:   false,
		},
		{
			name:      "with custom hash length",
			userKey:   "user789",
			chatID:    "chat999",
			chatStart: "1609459200",
			opts:      &IDOptions{HashLength: 16},
			wantErr:   false,
		},
		{
			name:      "with HMAC secret",
			userKey:   "secure-user",
			chatID:    "secure-chat",
			chatStart: "1609459200",
			opts:      &IDOptions{Secret: []byte("test-secret-key")},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := MakeID(tt.userKey, tt.chatID, tt.chatStart, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("MakeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if id == "" {
					t.Error("MakeID() returned empty string")
				}
				if len(id) > maxIdentifierLength {
					t.Errorf("MakeID() returned ID longer than %d: %d", maxIdentifierLength, len(id))
				}
			}
		})
	}
}

func TestMakeID_EmptyInputs(t *testing.T) {
	tests := []struct {
		name      string
		userKey   string
		chatID    string
		chatStart string
		wantErr   bool
	}{
		{
			name:      "empty user key",
			userKey:   "",
			chatID:    "chat123",
			chatStart: "1609459200",
			wantErr:   true,
		},
		{
			name:      "empty chat ID",
			userKey:   "user123",
			chatID:    "",
			chatStart: "1609459200",
			wantErr:   true,
		},
		{
			name:      "empty chat start",
			userKey:   "user123",
			chatID:    "chat123",
			chatStart: "",
			wantErr:   true,
		},
		{
			name:      "whitespace only user key",
			userKey:   "   ",
			chatID:    "chat123",
			chatStart: "1609459200",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := MakeID(tt.userKey, tt.chatID, tt.chatStart, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("MakeID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMakeID_Deterministic(t *testing.T) {
	userKey := "test-user"
	chatID := "test-chat"
	chatStart := "1609459200"

	id1, err1 := MakeID(userKey, chatID, chatStart, nil)
	if err1 != nil {
		t.Fatalf("MakeID() error = %v", err1)
	}

	id2, err2 := MakeID(userKey, chatID, chatStart, nil)
	if err2 != nil {
		t.Fatalf("MakeID() error = %v", err2)
	}

	if id1 != id2 {
		t.Errorf("MakeID() not deterministic: %s != %s", id1, id2)
	}
}

func TestMakeID_DifferentInputsProduceDifferentIDs(t *testing.T) {
	id1, _ := MakeID("user1", "chat1", "1609459200", nil)
	id2, _ := MakeID("user2", "chat1", "1609459200", nil)
	id3, _ := MakeID("user1", "chat2", "1609459200", nil)
	id4, _ := MakeID("user1", "chat1", "1609459300", nil)

	if id1 == id2 {
		t.Error("Different user keys should produce different IDs")
	}
	if id1 == id3 {
		t.Error("Different chat IDs should produce different IDs")
	}
	if id1 == id4 {
		t.Error("Different timestamps should produce different IDs")
	}
}

func TestMakeID_MaxLength(t *testing.T) {
	longUserKey := "very-long-user-key-that-exceeds-normal-length-expectations-for-testing"
	chatID := "chat123"
	chatStart := "1609459200"

	id, err := MakeID(longUserKey, chatID, chatStart, nil)
	if err != nil {
		t.Fatalf("MakeID() error = %v", err)
	}

	if len(id) > maxIdentifierLength {
		t.Errorf("MakeID() exceeded max length: got %d, want <= %d", len(id), maxIdentifierLength)
	}
}

func TestMakeID_CustomNowFunction(t *testing.T) {
	fixedTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	opts := &IDOptions{
		Now: func() time.Time { return fixedTime },
	}

	id1, err1 := MakeID("user", "chat", "invalid-timestamp", opts)
	if err1 != nil {
		t.Fatalf("MakeID() error = %v", err1)
	}

	id2, err2 := MakeID("user", "chat", "invalid-timestamp", opts)
	if err2 != nil {
		t.Fatalf("MakeID() error = %v", err2)
	}

	if id1 != id2 {
		t.Errorf("Custom Now function should produce consistent results: %s != %s", id1, id2)
	}
}

func TestBase36FromBytes(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{
			name:  "empty bytes",
			input: []byte{},
			want:  "0",
		},
		{
			name:  "single zero byte",
			input: []byte{0},
			want:  "0",
		},
		{
			name:  "small value",
			input: []byte{1},
			want:  "1",
		},
		{
			name:  "value 36",
			input: []byte{36},
			want:  "10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := base36FromBytes(tt.input)
			if got != tt.want {
				t.Errorf("base36FromBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBase36FromInt(t *testing.T) {
	tests := []struct {
		name  string
		input int64
		want  string
	}{
		{
			name:  "zero",
			input: 0,
			want:  "0",
		},
		{
			name:  "negative",
			input: -1,
			want:  "0",
		},
		{
			name:  "one",
			input: 1,
			want:  "1",
		},
		{
			name:  "36",
			input: 36,
			want:  "10",
		},
		{
			name:  "large value",
			input: 1609459200,
			want:  "qm8ao0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := base36FromInt(tt.input)
			if got != tt.want {
				t.Errorf("base36FromInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{
			name:  "empty string",
			input: "",
			want:  0,
		},
		{
			name:  "unix timestamp",
			input: "1609459200",
			want:  1609459200,
		},
		{
			name:  "RFC3339 format",
			input: "2021-01-01T00:00:00Z",
			want:  1609459200,
		},
		{
			name:  "invalid format",
			input: "not-a-timestamp",
			want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTimestamp(tt.input)
			if got != tt.want {
				t.Errorf("parseTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalise(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "lowercase letters",
			input: "abcdef",
			want:  "abcdef",
		},
		{
			name:  "uppercase letters",
			input: "ABCDEF",
			want:  "abcdef",
		},
		{
			name:  "mixed case with numbers",
			input: "User123",
			want:  "user123",
		},
		{
			name:  "special characters removed",
			input: "user@example.com",
			want:  "userexamplecom",
		},
		{
			name:  "spaces removed",
			input: "hello world",
			want:  "helloworld",
		},
		{
			name:  "empty string",
			input: "",
			want:  "x",
		},
		{
			name:  "only special characters",
			input: "@#$%",
			want:  "x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalise(tt.input)
			if got != tt.want {
				t.Errorf("normalise() = %v, want %v", got, tt.want)
			}
		})
	}
}
