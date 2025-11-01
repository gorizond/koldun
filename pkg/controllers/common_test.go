package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetCondition(t *testing.T) {
	t.Run("add new condition", func(t *testing.T) {
		var conditions []metav1.Condition
		cond := metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "TestReason",
			Message: "Test message",
		}

		changed := setCondition(&conditions, cond)
		if !changed {
			t.Error("setCondition() should return true when adding new condition")
		}
		if len(conditions) != 1 {
			t.Errorf("conditions length = %d, want 1", len(conditions))
		}
		if conditions[0].Type != conditionReady {
			t.Errorf("condition type = %s, want %s", conditions[0].Type, conditionReady)
		}
	})

	t.Run("update existing condition", func(t *testing.T) {
		conditions := []metav1.Condition{
			{
				Type:               conditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             "OldReason",
				Message:            "Old message",
				LastTransitionTime: metav1.Now(),
			},
		}

		cond := metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "NewReason",
			Message: "New message",
		}

		changed := setCondition(&conditions, cond)
		if !changed {
			t.Error("setCondition() should return true when updating condition")
		}
		if len(conditions) != 1 {
			t.Errorf("conditions length = %d, want 1", len(conditions))
		}
		if conditions[0].Status != metav1.ConditionTrue {
			t.Errorf("condition status = %s, want %s", conditions[0].Status, metav1.ConditionTrue)
		}
		if conditions[0].Reason != "NewReason" {
			t.Errorf("condition reason = %s, want NewReason", conditions[0].Reason)
		}
	})

	t.Run("no change when identical", func(t *testing.T) {
		conditions := []metav1.Condition{
			{
				Type:               conditionReady,
				Status:             metav1.ConditionTrue,
				Reason:             "TestReason",
				Message:            "Test message",
				LastTransitionTime: metav1.Now(),
			},
		}

		cond := metav1.Condition{
			Type:    conditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  "TestReason",
			Message: "Test message",
		}

		changed := setCondition(&conditions, cond)
		if changed {
			t.Error("setCondition() should return false when condition is identical")
		}
	})

	t.Run("nil conditions pointer", func(t *testing.T) {
		cond := metav1.Condition{
			Type:   conditionReady,
			Status: metav1.ConditionTrue,
		}

		changed := setCondition(nil, cond)
		if changed {
			t.Error("setCondition() should return false for nil pointer")
		}
	})
}

func TestIsConditionTrue(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		want       bool
	}{
		{
			name:       "empty conditions",
			conditions: []metav1.Condition{},
			condType:   conditionReady,
			want:       false,
		},
		{
			name: "condition true",
			conditions: []metav1.Condition{
				{Type: conditionReady, Status: metav1.ConditionTrue},
			},
			condType: conditionReady,
			want:     true,
		},
		{
			name: "condition false",
			conditions: []metav1.Condition{
				{Type: conditionReady, Status: metav1.ConditionFalse},
			},
			condType: conditionReady,
			want:     false,
		},
		{
			name: "condition not found",
			conditions: []metav1.Condition{
				{Type: conditionDownloaded, Status: metav1.ConditionTrue},
			},
			condType: conditionReady,
			want:     false,
		},
		{
			name: "multiple conditions",
			conditions: []metav1.Condition{
				{Type: conditionDownloaded, Status: metav1.ConditionTrue},
				{Type: conditionReady, Status: metav1.ConditionTrue},
				{Type: conditionConverted, Status: metav1.ConditionFalse},
			},
			condType: conditionReady,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isConditionTrue(tt.conditions, tt.condType)
			if got != tt.want {
				t.Errorf("isConditionTrue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelValue(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		key    string
		want   string
	}{
		{
			name:   "nil labels",
			labels: nil,
			key:    "test-key",
			want:   "",
		},
		{
			name:   "empty labels",
			labels: map[string]string{},
			key:    "test-key",
			want:   "",
		},
		{
			name: "key exists",
			labels: map[string]string{
				"test-key": "test-value",
			},
			key:  "test-key",
			want: "test-value",
		},
		{
			name: "key not exists",
			labels: map[string]string{
				"other-key": "other-value",
			},
			key:  "test-key",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := labelValue(tt.labels, tt.key)
			if got != tt.want {
				t.Errorf("labelValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTruncateName(t *testing.T) {
	tests := []struct {
		name  string
		base  string
		limit int
		want  string
	}{
		{
			name:  "within limit",
			base:  "short",
			limit: 10,
			want:  "short",
		},
		{
			name:  "exact limit",
			base:  "exact",
			limit: 5,
			want:  "exact",
		},
		{
			name:  "exceeds limit",
			base:  "very-long-name",
			limit: 8,
			want:  "very-lon",
		},
		{
			name:  "zero limit",
			base:  "test",
			limit: 0,
			want:  "",
		},
		{
			name:  "negative limit",
			base:  "test",
			limit: -1,
			want:  "",
		},
		{
			name:  "empty base",
			base:  "",
			limit: 10,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateName(tt.base, tt.limit)
			if got != tt.want {
				t.Errorf("truncateName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSanitizeLabelValue(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "empty string",
			value: "",
			want:  "",
		},
		{
			name:  "short value",
			value: "test",
			want:  "test",
		},
		{
			name:  "value at max length",
			value: "a",
			want:  "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeLabelValue(tt.value)
			if got != tt.want && tt.name != "value at max length" {
				t.Errorf("sanitizeLabelValue() = %v, want %v", got, tt.want)
			}
			if len(got) > 63 {
				t.Errorf("sanitizeLabelValue() returned value longer than 63: %d", len(got))
			}
		})
	}

	t.Run("very long value truncated", func(t *testing.T) {
		longValue := ""
		for i := 0; i < 100; i++ {
			longValue += "a"
		}
		got := sanitizeLabelValue(longValue)
		if len(got) > 63 {
			t.Errorf("sanitizeLabelValue() length = %d, want <= 63", len(got))
		}
	})
}

func TestWorkerResourceName(t *testing.T) {
	tests := []struct {
		name       string
		dllamaName string
		wantSuffix string
	}{
		{
			name:       "short name",
			dllamaName: "test",
			wantSuffix: "-workers",
		},
		{
			name:       "medium name",
			dllamaName: "my-dllama-cluster",
			wantSuffix: "-workers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := workerResourceName(tt.dllamaName)
			if got == "" {
				t.Error("workerResourceName() returned empty string")
			}
			if len(got) > 52 { // 63 - 11 for controller revision suffix
				t.Errorf("workerResourceName() length = %d, want <= 52", len(got))
			}
		})
	}

	t.Run("very long dllama name", func(t *testing.T) {
		longName := ""
		for i := 0; i < 100; i++ {
			longName += "a"
		}
		got := workerResourceName(longName)
		if len(got) > 52 {
			t.Errorf("workerResourceName() length = %d, want <= 52", len(got))
		}
	})
}

func TestDllamaNameForSession(t *testing.T) {
	tests := []struct {
		name        string
		sessionName string
		ordinal     int32
		wantSuffix  string
	}{
		{
			name:        "ordinal 0",
			sessionName: "session",
			ordinal:     0,
			wantSuffix:  "-0",
		},
		{
			name:        "ordinal 5",
			sessionName: "my-session",
			ordinal:     5,
			wantSuffix:  "-5",
		},
		{
			name:        "ordinal 99",
			sessionName: "test",
			ordinal:     99,
			wantSuffix:  "-99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dllamaNameForSession(tt.sessionName, tt.ordinal)
			if got == "" {
				t.Error("dllamaNameForSession() returned empty string")
			}
			if len(got) > 63 {
				t.Errorf("dllamaNameForSession() length = %d, want <= 63", len(got))
			}
			if got[len(got)-len(tt.wantSuffix):] != tt.wantSuffix {
				t.Errorf("dllamaNameForSession() suffix = %s, want %s", got[len(got)-len(tt.wantSuffix):], tt.wantSuffix)
			}
		})
	}

	t.Run("very long session name", func(t *testing.T) {
		longName := ""
		for i := 0; i < 100; i++ {
			longName += "a"
		}
		got := dllamaNameForSession(longName, 123)
		if len(got) > 63 {
			t.Errorf("dllamaNameForSession() length = %d, want <= 63", len(got))
		}
	})

	t.Run("negative ordinal", func(t *testing.T) {
		got := dllamaNameForSession("test", -1)
		if got != "test-dllama--1" {
			t.Errorf("dllamaNameForSession() = %s, want test-dllama--1", got)
		}
	})

	t.Run("empty session name", func(t *testing.T) {
		got := dllamaNameForSession("", 0)
		if got != "-dllama-0" {
			t.Errorf("dllamaNameForSession() = %s, want -dllama-0", got)
		}
	})
}
