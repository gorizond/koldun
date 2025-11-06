package controllers

import (
	"reflect"

	"github.com/rancher/wrangler/v3/pkg/apply"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/runtime"
)

// gomockApply wraps fakeapply.FakeApply so we keep the builder semantics while
// asserting ApplyObjects interactions in tests.
type gomockApply struct {
	*fakeapply.FakeApply
	ctrl     *gomock.Controller
	recorder *gomockApplyMockRecorder
}

type gomockApplyMockRecorder struct {
	mock *gomockApply
}

func newGomockApply(ctrl *gomock.Controller) *gomockApply {
	mock := &gomockApply{
		FakeApply: &fakeapply.FakeApply{},
		ctrl:      ctrl,
	}
	mock.recorder = &gomockApplyMockRecorder{mock: mock}
	return mock
}

func (m *gomockApply) EXPECT() *gomockApplyMockRecorder {
	return m.recorder
}

func (m *gomockApply) ApplyObjects(objs ...runtime.Object) error {
	m.ctrl.T.Helper()

	args := make([]interface{}, len(objs))
	for i, obj := range objs {
		args[i] = obj
	}

	ret := m.ctrl.Call(m, "ApplyObjects", args...)
	if len(ret) == 0 {
		return nil
	}
	if ret[0] == nil {
		return nil
	}
	if err, ok := ret[0].(error); ok {
		return err
	}
	return ret[0].(error)
}

func (mr *gomockApplyMockRecorder) ApplyObjects(objs ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApplyObjects", reflect.TypeOf((*gomockApply)(nil).ApplyObjects), objs...)
}

func (m *gomockApply) WithOwner(obj runtime.Object) apply.Apply {
	m.FakeApply.WithOwner(obj)
	return m
}

func (m *gomockApply) WithSetID(id string) apply.Apply {
	m.FakeApply.WithSetID(id)
	return m
}

func (m *gomockApply) WithSetOwnerReference(controller, block bool) apply.Apply {
	m.FakeApply.WithSetOwnerReference(controller, block)
	return m
}

func (m *gomockApply) WithDefaultNamespace(ns string) apply.Apply {
	m.FakeApply.WithDefaultNamespace(ns)
	return m
}
