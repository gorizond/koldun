package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseSizeMeasurement(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    *sizeMeasurement
		wantErr bool
	}{
		{
			name:    "valid measurement",
			payload: `{"bytes":1024,"human":"1 KiB"}`,
			want: &sizeMeasurement{
				Bytes: 1024,
				Human: "1 KiB",
			},
			wantErr: false,
		},
		{
			name:    "valid measurement with large bytes",
			payload: `{"bytes":1073741824,"human":"1 GiB"}`,
			want: &sizeMeasurement{
				Bytes: 1073741824,
				Human: "1 GiB",
			},
			wantErr: false,
		},
		{
			name:    "empty human field",
			payload: `{"bytes":512}`,
			want: &sizeMeasurement{
				Bytes: 512,
				Human: "",
			},
			wantErr: false,
		},
		{
			name:    "invalid json",
			payload: `{invalid}`,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty string",
			payload: ``,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "null json",
			payload: `null`,
			want:    &sizeMeasurement{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSizeMeasurement(tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSizeMeasurement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if got.Bytes != tt.want.Bytes {
				t.Errorf("parseSizeMeasurement().Bytes = %v, want %v", got.Bytes, tt.want.Bytes)
			}
			if got.Human != tt.want.Human {
				t.Errorf("parseSizeMeasurement().Human = %v, want %v", got.Human, tt.want.Human)
			}
		})
	}
}

func TestHasCondition(t *testing.T) {
	conditions := []metav1.Condition{
		{
			Type:   "Ready",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Available",
			Status: metav1.ConditionFalse,
		},
	}

	tests := []struct {
		name       string
		conditions []metav1.Condition
		condType   string
		want       bool
	}{
		{
			name:       "condition exists",
			conditions: conditions,
			condType:   "Ready",
			want:       true,
		},
		{
			name:       "condition exists - second item",
			conditions: conditions,
			condType:   "Available",
			want:       true,
		},
		{
			name:       "condition does not exist",
			conditions: conditions,
			condType:   "Unknown",
			want:       false,
		},
		{
			name:       "empty conditions",
			conditions: []metav1.Condition{},
			condType:   "Ready",
			want:       false,
		},
		{
			name:       "nil conditions",
			conditions: nil,
			condType:   "Ready",
			want:       false,
		},
		{
			name:       "empty condType",
			conditions: conditions,
			condType:   "",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCondition(tt.conditions, tt.condType)
			if got != tt.want {
				t.Errorf("hasCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReuseExistingSizeMeasurement(t *testing.T) {
	tests := []struct {
		name string
		obj  *v1.Model
		upd  *v1.Model
		cond *metav1.Condition
		want bool
	}{
		{
			name: "valid reuse - has bytes and human",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: v1.ModelStatus{
					ConversionSizeBytes:      1024,
					ConversionSizeHuman:      "1 KiB",
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: true,
		},
		{
			name: "valid reuse - only bytes no human",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: v1.ModelStatus{
					ConversionSizeBytes:      512,
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: true,
		},
		{
			name: "generation mismatch",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 2,
				},
				Status: v1.ModelStatus{
					ConversionSizeBytes:      1024,
					ConversionSizeHuman:      "1 KiB",
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "no size data",
			obj: &v1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: v1.ModelStatus{
					ConversionSizeGeneration: 1,
				},
			},
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "nil obj",
			obj:  nil,
			upd:  &v1.Model{},
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "nil updated",
			obj:  &v1.Model{},
			upd:  nil,
			cond: &metav1.Condition{},
			want: false,
		},
		{
			name: "nil condition",
			obj:  &v1.Model{},
			upd:  &v1.Model{},
			cond: nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reuseExistingSizeMeasurement(tt.obj, tt.upd, tt.cond)
			if got != tt.want {
				t.Errorf("reuseExistingSizeMeasurement() = %v, want %v", got, tt.want)
				return
			}

			// Verify condition and status are set correctly when reuse is successful
			if got && tt.cond != nil {
				if tt.cond.Status != metav1.ConditionTrue {
					t.Errorf("condition Status = %v, want True", tt.cond.Status)
				}
				if tt.cond.Reason != "SizingSucceeded" {
					t.Errorf("condition Reason = %v, want SizingSucceeded", tt.cond.Reason)
				}
				if tt.upd != nil && tt.obj != nil {
					if tt.upd.Status.ConversionSizeBytes != tt.obj.Status.ConversionSizeBytes {
						t.Errorf("updated bytes not copied correctly")
					}
					if tt.upd.Status.ConversionSizeGeneration != tt.obj.Status.ConversionSizeGeneration {
						t.Errorf("updated generation not copied correctly")
					}
				}
			}
		})
	}
}
