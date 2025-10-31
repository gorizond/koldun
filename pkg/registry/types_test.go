package registry

import (
	"encoding/json"
	"testing"
)

func TestModel_JSONMarshaling(t *testing.T) {
	model := Model{
		Namespace:           "models",
		Name:                "llama2-7b",
		DisplayName:         "Llama 2 7B",
		ConversionSizeBytes: 13476839680,
		ConversionSizeHuman: "12.5 GiB",
		OutputPVCName:       "llama2-7b-pvc",
		ReplicaPower:        2,
	}

	// Marshal
	data, err := json.Marshal(model)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Unmarshal
	var unmarshaled Model
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	// Verify fields
	if unmarshaled.Namespace != model.Namespace {
		t.Errorf("Namespace = %v, want %v", unmarshaled.Namespace, model.Namespace)
	}
	if unmarshaled.Name != model.Name {
		t.Errorf("Name = %v, want %v", unmarshaled.Name, model.Name)
	}
	if unmarshaled.DisplayName != model.DisplayName {
		t.Errorf("DisplayName = %v, want %v", unmarshaled.DisplayName, model.DisplayName)
	}
	if unmarshaled.ConversionSizeBytes != model.ConversionSizeBytes {
		t.Errorf("ConversionSizeBytes = %v, want %v", unmarshaled.ConversionSizeBytes, model.ConversionSizeBytes)
	}
	if unmarshaled.ConversionSizeHuman != model.ConversionSizeHuman {
		t.Errorf("ConversionSizeHuman = %v, want %v", unmarshaled.ConversionSizeHuman, model.ConversionSizeHuman)
	}
	if unmarshaled.OutputPVCName != model.OutputPVCName {
		t.Errorf("OutputPVCName = %v, want %v", unmarshaled.OutputPVCName, model.OutputPVCName)
	}
	if unmarshaled.ReplicaPower != model.ReplicaPower {
		t.Errorf("ReplicaPower = %v, want %v", unmarshaled.ReplicaPower, model.ReplicaPower)
	}
}

func TestModel_MinimalJSON(t *testing.T) {
	model := Model{
		Namespace: "default",
		Name:      "test-model",
	}

	data, err := json.Marshal(model)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var unmarshaled Model
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if unmarshaled.Namespace != model.Namespace {
		t.Errorf("Namespace = %v, want %v", unmarshaled.Namespace, model.Namespace)
	}
	if unmarshaled.Name != model.Name {
		t.Errorf("Name = %v, want %v", unmarshaled.Name, model.Name)
	}
}

func TestToken_JSONMarshaling(t *testing.T) {
	token := Token{
		Hash:      "abc123def456",
		Disabled:  false,
		Namespace: "default",
		Metadata: map[string]string{
			"user": "alice",
			"role": "admin",
		},
	}

	// Marshal
	data, err := json.Marshal(token)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Unmarshal
	var unmarshaled Token
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	// Verify fields
	if unmarshaled.Hash != token.Hash {
		t.Errorf("Hash = %v, want %v", unmarshaled.Hash, token.Hash)
	}
	if unmarshaled.Disabled != token.Disabled {
		t.Errorf("Disabled = %v, want %v", unmarshaled.Disabled, token.Disabled)
	}
	if unmarshaled.Namespace != token.Namespace {
		t.Errorf("Namespace = %v, want %v", unmarshaled.Namespace, token.Namespace)
	}
	if len(unmarshaled.Metadata) != len(token.Metadata) {
		t.Errorf("Metadata length = %v, want %v", len(unmarshaled.Metadata), len(token.Metadata))
	}
	for k, v := range token.Metadata {
		if unmarshaled.Metadata[k] != v {
			t.Errorf("Metadata[%s] = %v, want %v", k, unmarshaled.Metadata[k], v)
		}
	}
}

func TestToken_MinimalJSON(t *testing.T) {
	token := Token{
		Hash:      "abc123",
		Namespace: "default",
	}

	data, err := json.Marshal(token)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var unmarshaled Token
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if unmarshaled.Hash != token.Hash {
		t.Errorf("Hash = %v, want %v", unmarshaled.Hash, token.Hash)
	}
	if unmarshaled.Namespace != token.Namespace {
		t.Errorf("Namespace = %v, want %v", unmarshaled.Namespace, token.Namespace)
	}
	if unmarshaled.Disabled != false {
		t.Errorf("Disabled should default to false, got %v", unmarshaled.Disabled)
	}
}

func TestToken_DisabledFlag(t *testing.T) {
	tests := []struct {
		name     string
		disabled bool
	}{
		{
			name:     "enabled token",
			disabled: false,
		},
		{
			name:     "disabled token",
			disabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := Token{
				Hash:      "test-hash",
				Disabled:  tt.disabled,
				Namespace: "default",
			}

			data, err := json.Marshal(token)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}

			var unmarshaled Token
			err = json.Unmarshal(data, &unmarshaled)
			if err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}

			if unmarshaled.Disabled != tt.disabled {
				t.Errorf("Disabled = %v, want %v", unmarshaled.Disabled, tt.disabled)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	// Test that constants are defined with expected values
	if DefaultModelBucket == "" {
		t.Error("DefaultModelBucket should not be empty")
	}
	if DefaultTokenBucket == "" {
		t.Error("DefaultTokenBucket should not be empty")
	}
	if DefaultModelPrefix == "" {
		t.Error("DefaultModelPrefix should not be empty")
	}
	if DefaultTokenPrefix == "" {
		t.Error("DefaultTokenPrefix should not be empty")
	}

	// Verify expected values
	if DefaultModelBucket != "koldun_models" {
		t.Errorf("DefaultModelBucket = %v, want koldun_models", DefaultModelBucket)
	}
	if DefaultTokenBucket != "koldun_tokens" {
		t.Errorf("DefaultTokenBucket = %v, want koldun_tokens", DefaultTokenBucket)
	}
	if DefaultModelPrefix != "model/" {
		t.Errorf("DefaultModelPrefix = %v, want model/", DefaultModelPrefix)
	}
	if DefaultTokenPrefix != "token/" {
		t.Errorf("DefaultTokenPrefix = %v, want token/", DefaultTokenPrefix)
	}
}
