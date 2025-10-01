package koldunv1

import (
	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/schemes"
)

func init() {
	schemes.Register(v1.AddToScheme)
}

type Interface interface {
	Dllama() generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	Model() generic.ControllerInterface[*v1.Model, *v1.ModelList]
	Root() generic.ControllerInterface[*v1.Root, *v1.RootList]
	Worker() generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
	Ingress() generic.ControllerInterface[*v1.Ingress, *v1.IngressList]
	Session() generic.ControllerInterface[*v1.Session, *v1.SessionList]
}

type version struct {
	controllerFactory controller.SharedControllerFactory
}

func New(controllerFactory controller.SharedControllerFactory) Interface {
	return &version{controllerFactory: controllerFactory}
}

func (v *version) Dllama() generic.ControllerInterface[*v1.Dllama, *v1.DllamaList] {
	return generic.NewController[*v1.Dllama, *v1.DllamaList](v1.SchemeGroupVersion.WithKind("Dllama"), "dllamas", true, v.controllerFactory)
}

func (v *version) Model() generic.ControllerInterface[*v1.Model, *v1.ModelList] {
	return generic.NewController[*v1.Model, *v1.ModelList](v1.SchemeGroupVersion.WithKind("Model"), "models", true, v.controllerFactory)
}

func (v *version) Root() generic.ControllerInterface[*v1.Root, *v1.RootList] {
	return generic.NewController[*v1.Root, *v1.RootList](v1.SchemeGroupVersion.WithKind("Root"), "roots", true, v.controllerFactory)
}

func (v *version) Worker() generic.ControllerInterface[*v1.Worker, *v1.WorkerList] {
	return generic.NewController[*v1.Worker, *v1.WorkerList](v1.SchemeGroupVersion.WithKind("Worker"), "workers", true, v.controllerFactory)
}

func (v *version) Ingress() generic.ControllerInterface[*v1.Ingress, *v1.IngressList] {
	return generic.NewController[*v1.Ingress, *v1.IngressList](v1.SchemeGroupVersion.WithKind("Ingress"), "ingresses", true, v.controllerFactory)
}

func (v *version) Session() generic.ControllerInterface[*v1.Session, *v1.SessionList] {
	return generic.NewController[*v1.Session, *v1.SessionList](v1.SchemeGroupVersion.WithKind("Session"), "sessions", true, v.controllerFactory)
}
