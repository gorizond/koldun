package controllers

import (
	"fmt"
	"testing"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestScalingParamsFromSession(t *testing.T) {
	tests := []struct {
		name     string
		session  *v1.Session
		expected sessionScalingParams
	}{
		{
			name: "default params - only min",
			session: &v1.Session{
				Spec: v1.SessionSpec{},
			},
			expected: sessionScalingParams{
				min: 1,
			},
		},
		{
			name: "legacy MinIdle and MaxWorkers",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					MinIdle:    3,
					MaxWorkers: 10,
				},
			},
			expected: sessionScalingParams{
				min: 3,
				max: 10,
			},
		},
		{
			name: "scaling config overrides legacy",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					MinIdle:    2,
					MaxWorkers: 5,
					Scaling: &v1.SessionScalingSpec{
						MinDllamas: 4,
						MaxDllamas: 20,
					},
				},
			},
			expected: sessionScalingParams{
				min: 4,
				max: 20,
			},
		},
		{
			name: "full scaling config",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					Scaling: &v1.SessionScalingSpec{
						MinDllamas:           2,
						MaxDllamas:           15,
						DesiredDllamas:       5,
						ScaleUpBacklog:       100,
						ScaleDownIdleSeconds: 300,
					},
				},
			},
			expected: sessionScalingParams{
				min:              2,
				max:              15,
				desired:          5,
				scaleUpThreshold: 100,
				scaleDownIdle:    300 * time.Second,
			},
		},
		{
			name: "max less than min - corrected to min",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					Scaling: &v1.SessionScalingSpec{
						MinDllamas: 10,
						MaxDllamas: 5,
					},
				},
			},
			expected: sessionScalingParams{
				min: 10,
				max: 10, // corrected
			},
		},
		{
			name: "zero min corrected to 1",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					Scaling: &v1.SessionScalingSpec{
						MinDllamas: 0,
						MaxDllamas: 10,
					},
				},
			},
			expected: sessionScalingParams{
				min: 1, // corrected from 0
				max: 10,
			},
		},
		{
			name: "negative min corrected to 1",
			session: &v1.Session{
				Spec: v1.SessionSpec{
					MinIdle: -5,
				},
			},
			expected: sessionScalingParams{
				min: 1, // corrected from negative
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scalingParamsFromSession(tt.session)

			if result.min != tt.expected.min {
				t.Errorf("min: got %d, want %d", result.min, tt.expected.min)
			}
			if result.max != tt.expected.max {
				t.Errorf("max: got %d, want %d", result.max, tt.expected.max)
			}
			if result.desired != tt.expected.desired {
				t.Errorf("desired: got %d, want %d", result.desired, tt.expected.desired)
			}
			if result.scaleUpThreshold != tt.expected.scaleUpThreshold {
				t.Errorf("scaleUpThreshold: got %d, want %d", result.scaleUpThreshold, tt.expected.scaleUpThreshold)
			}
			if result.scaleDownIdle != tt.expected.scaleDownIdle {
				t.Errorf("scaleDownIdle: got %v, want %v", result.scaleDownIdle, tt.expected.scaleDownIdle)
			}
		})
	}
}

func TestSessionScalingParams_ShouldScaleUp(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		params   sessionScalingParams
		state    sessionPoolState
		expected bool
	}{
		{
			name: "scale up when backlog exceeds threshold and no idle workers",
			params: sessionScalingParams{
				min:              1,
				max:              10,
				scaleUpThreshold: 50,
			},
			state: sessionPoolState{
				dllamas:   makeDllamas(3),
				ready:     makeDllamas(3),
				idleReady: makeDllamas(0),
				backlog:   100,
			},
			expected: true,
		},
		{
			name: "don't scale up when at max capacity",
			params: sessionScalingParams{
				min:              1,
				max:              5,
				scaleUpThreshold: 50,
			},
			state: sessionPoolState{
				dllamas:   makeDllamas(5),
				ready:     makeDllamas(5),
				idleReady: makeDllamas(0),
				backlog:   100,
			},
			expected: false,
		},
		{
			name: "don't scale up when idle workers available",
			params: sessionScalingParams{
				min:              1,
				max:              10,
				scaleUpThreshold: 50,
			},
			state: sessionPoolState{
				dllamas:   makeDllamas(3),
				ready:     makeDllamas(3),
				idleReady: makeDllamas(1),
				backlog:   100,
			},
			expected: false,
		},
		{
			name: "don't scale up when backlog below threshold",
			params: sessionScalingParams{
				min:              1,
				max:              10,
				scaleUpThreshold: 50,
			},
			state: sessionPoolState{
				dllamas:   makeDllamas(3),
				ready:     makeDllamas(3),
				idleReady: makeDllamas(0),
				backlog:   30,
			},
			expected: false,
		},
		{
			name: "scale up to desired count",
			params: sessionScalingParams{
				min:     1,
				max:     10,
				desired: 5,
			},
			state: sessionPoolState{
				dllamas: makeDllamas(3),
			},
			expected: true,
		},
		{
			name: "don't scale up when at desired count",
			params: sessionScalingParams{
				min:     1,
				max:     10,
				desired: 5,
			},
			state: sessionPoolState{
				dllamas: makeDllamas(5),
			},
			expected: false,
		},
		{
			name: "don't scale up without threshold configured",
			params: sessionScalingParams{
				min:              1,
				max:              10,
				scaleUpThreshold: 0,
			},
			state: sessionPoolState{
				dllamas:   makeDllamas(3),
				ready:     makeDllamas(3),
				idleReady: makeDllamas(0),
				backlog:   100,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.state.lastActivity = now
			result := tt.params.shouldScaleUp(tt.state)
			if result != tt.expected {
				t.Errorf("shouldScaleUp() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSessionScalingParams_ShouldScaleDown(t *testing.T) {
	now := time.Now()
	pastActivity := now.Add(-10 * time.Minute)

	tests := []struct {
		name     string
		params   sessionScalingParams
		state    sessionPoolState
		expected bool
	}{
		{
			name: "scale down when idle timeout exceeded",
			params: sessionScalingParams{
				min:           1,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: pastActivity,
			},
			expected: true,
		},
		{
			name: "don't scale down when at min capacity",
			params: sessionScalingParams{
				min:           3,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: pastActivity,
			},
			expected: false,
		},
		{
			name: "don't scale down when no idle workers",
			params: sessionScalingParams{
				min:           1,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(0),
				lastActivity: pastActivity,
			},
			expected: false,
		},
		{
			name: "don't scale down when backlog exists",
			params: sessionScalingParams{
				min:           1,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				backlog:      10,
				lastActivity: pastActivity,
			},
			expected: false,
		},
		{
			name: "don't scale down when activity too recent",
			params: sessionScalingParams{
				min:           1,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: now.Add(-2 * time.Minute),
			},
			expected: false,
		},
		{
			name: "don't scale down without scaleDownIdle configured",
			params: sessionScalingParams{
				min:           1,
				scaleDownIdle: 0,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: pastActivity,
			},
			expected: false,
		},
		{
			name: "don't scale down when lastActivity is zero",
			params: sessionScalingParams{
				min:           1,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: time.Time{},
			},
			expected: false,
		},
		{
			name: "don't scale down when at desired count",
			params: sessionScalingParams{
				min:           1,
				desired:       3,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: pastActivity,
			},
			expected: false,
		},
		{
			name: "scale down when above desired count",
			params: sessionScalingParams{
				min:           1,
				desired:       2,
				scaleDownIdle: 5 * time.Minute,
			},
			state: sessionPoolState{
				dllamas:      makeDllamas(3),
				ready:        makeDllamas(3),
				idleReady:    makeDllamas(2),
				lastActivity: pastActivity,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.params.shouldScaleDown(tt.state)
			if result != tt.expected {
				t.Errorf("shouldScaleDown() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComputeSessionPoolState(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		session  *v1.Session
		dllamas  []*v1.Dllama
		expected sessionPoolState
	}{
		{
			name: "empty pool",
			session: &v1.Session{
				Status: v1.SessionStatus{},
			},
			dllamas: []*v1.Dllama{},
			expected: sessionPoolState{
				dllamas:       []*v1.Dllama{},
				ready:         nil,
				idleReady:     nil,
				workerMetrics: map[string]v1.SessionWorker{},
			},
		},
		{
			name: "all workers ready and idle",
			session: &v1.Session{
				Status: v1.SessionStatus{
					Workers: []v1.SessionWorker{
						{Name: "dllama-1", ActiveMessages: 0},
						{Name: "dllama-2", ActiveMessages: 0},
					},
					Backlog:      0,
					LastActivity: &metav1.Time{Time: now},
				},
			},
			dllamas: []*v1.Dllama{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "dllama-1"},
					Status: v1.DllamaStatus{
						Conditions: []metav1.Condition{
							{Type: conditionReady, Status: metav1.ConditionTrue},
						},
						ReadyRoot: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "dllama-2"},
					Status: v1.DllamaStatus{
						Conditions: []metav1.Condition{
							{Type: conditionReady, Status: metav1.ConditionTrue},
						},
						ReadyRoot: true,
					},
				},
			},
			expected: sessionPoolState{
				backlog:      0,
				lastActivity: now,
			},
		},
		{
			name: "mixed ready and busy workers",
			session: &v1.Session{
				Status: v1.SessionStatus{
					Workers: []v1.SessionWorker{
						{Name: "dllama-1", ActiveMessages: 5},
						{Name: "dllama-2", ActiveMessages: 0},
						{Name: "dllama-3", ActiveMessages: 0},
					},
					Backlog:      50,
					LastActivity: &metav1.Time{Time: now},
				},
			},
			dllamas: []*v1.Dllama{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "dllama-1"},
					Status: v1.DllamaStatus{
						Conditions: []metav1.Condition{
							{Type: conditionReady, Status: metav1.ConditionTrue},
						},
						ReadyRoot: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "dllama-2"},
					Status: v1.DllamaStatus{
						Conditions: []metav1.Condition{
							{Type: conditionReady, Status: metav1.ConditionTrue},
						},
						ReadyRoot: true,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "dllama-3"},
					Status: v1.DllamaStatus{
						Conditions: []metav1.Condition{
							{Type: conditionReady, Status: metav1.ConditionFalse},
						},
						ReadyRoot: false,
					},
				},
			},
			expected: sessionPoolState{
				backlog:      50,
				lastActivity: now,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeSessionPoolState(tt.session, tt.dllamas)

			if result.total() != len(tt.dllamas) {
				t.Errorf("total() = %d, want %d", result.total(), len(tt.dllamas))
			}
			if result.backlog != tt.expected.backlog {
				t.Errorf("backlog = %d, want %d", result.backlog, tt.expected.backlog)
			}
			if !result.lastActivity.Equal(tt.expected.lastActivity) {
				t.Errorf("lastActivity = %v, want %v", result.lastActivity, tt.expected.lastActivity)
			}

			// Verify worker metrics are populated
			if len(result.workerMetrics) != len(tt.session.Status.Workers) {
				t.Errorf("workerMetrics count = %d, want %d",
					len(result.workerMetrics), len(tt.session.Status.Workers))
			}
		})
	}
}

func TestSessionPoolState_Counts(t *testing.T) {
	dllamas := makeDllamas(5)

	state := sessionPoolState{
		dllamas:   dllamas,
		ready:     dllamas[:3],
		idleReady: dllamas[:2],
	}

	if state.total() != 5 {
		t.Errorf("total() = %d, want 5", state.total())
	}
	if state.readyCount() != 3 {
		t.Errorf("readyCount() = %d, want 3", state.readyCount())
	}
	if state.idleCount() != 2 {
		t.Errorf("idleCount() = %d, want 2", state.idleCount())
	}
	if state.busyCount() != 1 {
		t.Errorf("busyCount() = %d, want 1 (3 ready - 2 idle)", state.busyCount())
	}
}

func TestChooseScaleDownCandidate(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name         string
		state        sessionPoolState
		expectedName string
	}{
		{
			name: "no idle workers - return nil",
			state: sessionPoolState{
				idleReady: []*v1.Dllama{},
			},
			expectedName: "",
		},
		{
			name: "choose oldest by creation time when no heartbeats",
			state: sessionPoolState{
				idleReady: []*v1.Dllama{
					{ObjectMeta: metav1.ObjectMeta{
						Name:              "dllama-new",
						CreationTimestamp: metav1.Time{Time: now.Add(-1 * time.Hour)},
					}},
					{ObjectMeta: metav1.ObjectMeta{
						Name:              "dllama-old",
						CreationTimestamp: metav1.Time{Time: now.Add(-5 * time.Hour)},
					}},
				},
				workerMetrics: map[string]v1.SessionWorker{
					"dllama-new": {},
					"dllama-old": {},
				},
			},
			expectedName: "dllama-old",
		},
		{
			name: "choose by oldest heartbeat",
			state: sessionPoolState{
				idleReady: []*v1.Dllama{
					{ObjectMeta: metav1.ObjectMeta{
						Name:              "dllama-1",
						CreationTimestamp: metav1.Time{Time: now.Add(-1 * time.Hour)},
					}},
					{ObjectMeta: metav1.ObjectMeta{
						Name:              "dllama-2",
						CreationTimestamp: metav1.Time{Time: now.Add(-2 * time.Hour)},
					}},
				},
				workerMetrics: map[string]v1.SessionWorker{
					"dllama-1": {LastHeartbeat: &metav1.Time{Time: now.Add(-10 * time.Minute)}},
					"dllama-2": {LastHeartbeat: &metav1.Time{Time: now.Add(-30 * time.Minute)}},
				},
			},
			expectedName: "dllama-2",
		},
		{
			name: "prefer worker without heartbeat over recent heartbeat",
			state: sessionPoolState{
				idleReady: []*v1.Dllama{
					{ObjectMeta: metav1.ObjectMeta{
						Name:              "dllama-no-hb",
						CreationTimestamp: metav1.Time{Time: now.Add(-1 * time.Hour)},
					}},
					{ObjectMeta: metav1.ObjectMeta{
						Name:              "dllama-with-hb",
						CreationTimestamp: metav1.Time{Time: now.Add(-2 * time.Hour)},
					}},
				},
				workerMetrics: map[string]v1.SessionWorker{
					"dllama-no-hb":   {},
					"dllama-with-hb": {LastHeartbeat: &metav1.Time{Time: now.Add(-5 * time.Minute)}},
				},
			},
			expectedName: "dllama-no-hb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := chooseScaleDownCandidate(tt.state)

			if tt.expectedName == "" {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("expected dllama named %q, got nil", tt.expectedName)
				return
			}

			if result.Name != tt.expectedName {
				t.Errorf("chooseScaleDownCandidate() name = %q, want %q", result.Name, tt.expectedName)
			}
		})
	}
}

// Helper function to create dummy dllamas for testing
func makeDllamas(count int) []*v1.Dllama {
	result := make([]*v1.Dllama, count)
	for i := 0; i < count; i++ {
		result[i] = &v1.Dllama{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("dllama-%d", i+1),
			},
		}
	}
	return result
}
