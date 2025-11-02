package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIngressHandlerOnRemove(t *testing.T) {
	var handler ingressHandler

	ing := &v1.Ingress{ObjectMeta: objMeta("demo", "ingress")}

	result, err := handler.onRemove("demo/ingress", ing)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != ing {
		t.Fatalf("expected ingress pointer to be returned")
	}

	nilResult, err := handler.onRemove("demo/ingress", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nilResult != nil {
		t.Fatalf("expected nil to be returned when object is nil")
	}
}

func TestRootHandlerOnRemove(t *testing.T) {
	var handler rootHandler

	root := &v1.Root{ObjectMeta: objMeta("demo", "root")}

	result, err := handler.onRemove("demo/root", root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != root {
		t.Fatalf("expected root pointer to be returned")
	}
}

func TestWorkerHandlerOnRemove(t *testing.T) {
	var handler workerHandler

	worker := &v1.Worker{ObjectMeta: objMeta("demo", "worker")}

	result, err := handler.onRemove("demo/worker", worker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != worker {
		t.Fatalf("expected worker pointer to be returned")
	}
}

func objMeta(ns, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name}
}
