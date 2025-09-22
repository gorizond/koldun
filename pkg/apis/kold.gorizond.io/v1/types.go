package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the Kubernetes API group for the custom resources managed by the operator.
	GroupName = "kold.gorizond.io"
	// Version is the API version supported by this package.
	Version = "v1"
)

// SchemeGroupVersion is group version used to register these objects.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

var (
	// SchemeBuilder collects functions that add the custom resources to a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme adds the custom resources to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers the custom resource definitions with the supplied scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Dllama{},
		&DllamaList{},
		&Model{},
		&ModelList{},
		&Root{},
		&RootList{},
		&Worker{},
		&WorkerList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=dll

// Dllama describes a distributed-llama topology managed by the operator.
type Dllama struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DllamaSpec   `json:"spec,omitempty"`
	Status DllamaStatus `json:"status,omitempty"`
}

// DllamaSpec captures the desired state for a distributed-llama deployment.
type DllamaSpec struct {
	// ModelRef references the model artifact that should be available on the cluster nodes.
	ModelRef string `json:"modelRef"`
	// ReplicaPower is the exponent that defines the worker fan-out (totalNodes = 2^ReplicaPower).
	ReplicaPower int32 `json:"replicaPower"`
	// RootImage is the container image used for the root coordinator process.
	RootImage string `json:"rootImage"`
	// WorkerImage is the container image used for the worker processes.
	WorkerImage string `json:"workerImage"`
	// LaunchArgs are appended to distributed-llama launch command for both root and workers.
	LaunchArgs []string `json:"launchArgs,omitempty"`
	// CacheSpec describes optional cache service configuration for model artifacts.
	CacheSpec *CacheSpec `json:"cacheSpec,omitempty"`
}

// CacheSpec contains information about the shared cache used to store model weights.
type CacheSpec struct {
	// Endpoint is the URL of the cache backend (e.g. S3 compatible object storage).
	Endpoint string `json:"endpoint"`
	// Bucket is the object store bucket where models are stored.
	Bucket string `json:"bucket"`
	// SecretRef references a Secret containing credentials for the cache backend.
	SecretRef *SecretReference `json:"secretRef,omitempty"`
}

// SecretReference identifies a Secret in the same namespace unless Namespace is set.
type SecretReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// DllamaStatus provides high-level status for the distributed-llama deployment.
type DllamaStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	// ReadyWorkers reflects how many workers are currently reported as ready.
	ReadyWorkers int32 `json:"readyWorkers,omitempty"`
	// ReadyRoot indicates whether the root pod is reported as ready.
	ReadyRoot bool `json:"readyRoot,omitempty"`
}

// +kubebuilder:object:root=true

// DllamaList is a list of Dllama resources.
type DllamaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Dllama `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=mdl

// Model represents the lifecycle of downloading and caching a language model artifact.
type Model struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelSpec   `json:"spec,omitempty"`
	Status ModelStatus `json:"status,omitempty"`
}

// ModelSpec holds the desired model artifact configuration.
type ModelSpec struct {
	// SourceURL points to the upstream model source. For Hugging Face, this is the
	// repository URL like https://huggingface.co/mistralai/Mistral-7B-v0.3. The operator
	// will download all repository files into the cache backend.
	SourceURL string `json:"sourceUrl"`
	// LocalPath is the path/prefix on the cache backend where the artifacts will reside.
	LocalPath string `json:"localPath"`
	// CacheSpec optionally overrides the cache information for this model.
	CacheSpec *CacheSpec `json:"cacheSpec,omitempty"`
	// LaunchOptions mirror the arguments passed to distributed-llama's download sequence.
	LaunchOptions []string `json:"launchOptions,omitempty"`
	// Download contains configuration for how the model artifacts are fetched and prepared.
	Download *ModelDownloadSpec `json:"download,omitempty"`
	// Conversion optionally describes how cached artifacts should be converted post-download.
	Conversion *ModelConversionSpec `json:"conversion,omitempty"`
}

// ModelStatus reports whether the model artifact is ready for consumption.
type ModelStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	// ArtifactSizeBytes records the size of the downloaded artifact, if known.
	ArtifactSizeBytes int64 `json:"artifactSizeBytes,omitempty"`
	// DownloadJobName is the name of the Kubernetes Job responsible for downloading the model.
	DownloadJobName string `json:"downloadJobName,omitempty"`
	// DownloadState is a high-level state for the downloader pipeline (Pending, Running, Succeeded, Failed).
	DownloadState string `json:"downloadState,omitempty"`
	// ConversionJobName is the conversion Job associated with this model generation.
	ConversionJobName string `json:"conversionJobName,omitempty"`
	// ConversionState mirrors DownloadState for the conversion pipeline.
	ConversionState string `json:"conversionState,omitempty"`
}

// +kubebuilder:object:root=true

// ModelList represents a list of Model resources.
type ModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Model `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=rt

// Root models the root coordinator that orchestrates distributed-llama workers.
type Root struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RootSpec   `json:"spec,omitempty"`
	Status RootStatus `json:"status,omitempty"`
}

// RootSpec captures desired configuration for the root pod.
type RootSpec struct {
	ModelRef  string     `json:"modelRef"`
	Image     string     `json:"image"`
	Args      []string   `json:"args,omitempty"`
	CacheSpec *CacheSpec `json:"cacheSpec,omitempty"`
	// WorkerSelector allows selecting Worker resources belonging to this Root.
	WorkerSelector map[string]string `json:"workerSelector,omitempty"`
}

// RootStatus indicates readiness for the root component.
type RootStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Endpoint           string             `json:"endpoint,omitempty"`
}

// +kubebuilder:object:root=true

// RootList holds a list of Root resources.
type RootList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Root `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=wrk

// Worker models an individual distributed-llama worker process.
type Worker struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkerSpec   `json:"spec,omitempty"`
	Status WorkerStatus `json:"status,omitempty"`
}

// WorkerSpec captures desired state for a worker pod.
type WorkerSpec struct {
	ModelRef  string     `json:"modelRef"`
	Image     string     `json:"image"`
	Args      []string   `json:"args,omitempty"`
	CacheSpec *CacheSpec `json:"cacheSpec,omitempty"`
	// RootRef references the coordinating Root resource.
	RootRef string `json:"rootRef"`
	// Slot is the worker slot index relative to the root (starting at 0).
	Slot int32 `json:"slot"`
}

// WorkerStatus provides readiness indicators for a worker.
type WorkerStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

// WorkerList represents a list of Worker resources.
type WorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Worker `json:"items"`
}

func (in *Dllama) DeepCopyInto(out *Dllama) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}

func (in *Dllama) DeepCopy() *Dllama {
	if in == nil {
		return nil
	}
	out := new(Dllama)
	in.DeepCopyInto(out)
	return out
}

func (in *Dllama) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *DllamaList) DeepCopyInto(out *DllamaList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Dllama, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *DllamaList) DeepCopy() *DllamaList {
	if in == nil {
		return nil
	}
	out := new(DllamaList)
	in.DeepCopyInto(out)
	return out
}

func (in *DllamaList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *DllamaSpec) DeepCopy() DllamaSpec {
	if in == nil {
		return DllamaSpec{}
	}
	out := *in
	if in.LaunchArgs != nil {
		out.LaunchArgs = make([]string, len(in.LaunchArgs))
		copy(out.LaunchArgs, in.LaunchArgs)
	}
	if in.CacheSpec != nil {
		cacheCopy := *in.CacheSpec.DeepCopy()
		out.CacheSpec = &cacheCopy
	}
	return out
}

func (in *DllamaStatus) DeepCopy() *DllamaStatus {
	if in == nil {
		return nil
	}
	out := new(DllamaStatus)
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
	return out
}

func (in *CacheSpec) DeepCopy() *CacheSpec {
	if in == nil {
		return nil
	}
	out := new(CacheSpec)
	*out = *in
	if in.SecretRef != nil {
		secretCopy := *in.SecretRef
		out.SecretRef = &secretCopy
	}
	return out
}

func (in *Model) DeepCopyInto(out *Model) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = *in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}

func (in *Model) DeepCopy() *Model {
	if in == nil {
		return nil
	}
	out := new(Model)
	in.DeepCopyInto(out)
	return out
}

func (in *Model) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ModelList) DeepCopyInto(out *ModelList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Model, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *ModelList) DeepCopy() *ModelList {
	if in == nil {
		return nil
	}
	out := new(ModelList)
	in.DeepCopyInto(out)
	return out
}

func (in *ModelList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ModelSpec) DeepCopy() *ModelSpec {
	if in == nil {
		return nil
	}
	out := new(ModelSpec)
	*out = *in
	if in.LaunchOptions != nil {
		out.LaunchOptions = make([]string, len(in.LaunchOptions))
		copy(out.LaunchOptions, in.LaunchOptions)
	}
	if in.CacheSpec != nil {
		cacheCopy := *in.CacheSpec.DeepCopy()
		out.CacheSpec = &cacheCopy
	}
	if in.Download != nil {
		downloadCopy := *in.Download.DeepCopy()
		out.Download = &downloadCopy
	}
	if in.Conversion != nil {
		conversionCopy := *in.Conversion.DeepCopy()
		out.Conversion = &conversionCopy
	}
	return out
}

func (in *ModelStatus) DeepCopy() *ModelStatus {
	if in == nil {
		return nil
	}
	out := new(ModelStatus)
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
	return out
}

func (in *Root) DeepCopyInto(out *Root) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = *in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}

func (in *Root) DeepCopy() *Root {
	if in == nil {
		return nil
	}
	out := new(Root)
	in.DeepCopyInto(out)
	return out
}

func (in *Root) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *RootList) DeepCopyInto(out *RootList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Root, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *RootList) DeepCopy() *RootList {
	if in == nil {
		return nil
	}
	out := new(RootList)
	in.DeepCopyInto(out)
	return out
}

func (in *RootList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *RootSpec) DeepCopy() *RootSpec {
	if in == nil {
		return nil
	}
	out := new(RootSpec)
	*out = *in
	if in.Args != nil {
		out.Args = make([]string, len(in.Args))
		copy(out.Args, in.Args)
	}
	if in.CacheSpec != nil {
		cacheCopy := *in.CacheSpec.DeepCopy()
		out.CacheSpec = &cacheCopy
	}
	if in.WorkerSelector != nil {
		out.WorkerSelector = make(map[string]string, len(in.WorkerSelector))
		for k, v := range in.WorkerSelector {
			out.WorkerSelector[k] = v
		}
	}
	return out
}

func (in *RootStatus) DeepCopy() *RootStatus {
	if in == nil {
		return nil
	}
	out := new(RootStatus)
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
	return out
}

func (in *Worker) DeepCopyInto(out *Worker) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = *in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}

func (in *Worker) DeepCopy() *Worker {
	if in == nil {
		return nil
	}
	out := new(Worker)
	in.DeepCopyInto(out)
	return out
}

func (in *Worker) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *WorkerList) DeepCopyInto(out *WorkerList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Worker, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *WorkerList) DeepCopy() *WorkerList {
	if in == nil {
		return nil
	}
	out := new(WorkerList)
	in.DeepCopyInto(out)
	return out
}

func (in *WorkerList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *WorkerSpec) DeepCopy() *WorkerSpec {
	if in == nil {
		return nil
	}
	out := new(WorkerSpec)
	*out = *in
	if in.Args != nil {
		out.Args = make([]string, len(in.Args))
		copy(out.Args, in.Args)
	}
	if in.CacheSpec != nil {
		cacheCopy := *in.CacheSpec.DeepCopy()
		out.CacheSpec = &cacheCopy
	}
	return out
}

func (in *WorkerStatus) DeepCopy() *WorkerStatus {
	if in == nil {
		return nil
	}
	out := new(WorkerStatus)
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
	return out
}

// ModelDownloadSpec describes how to acquire model artifacts.
type ModelDownloadSpec struct {
	// Image is the container image used for the download Job.
	Image string `json:"image,omitempty"`
	// Command overrides the container entrypoint when set.
	Command []string `json:"command,omitempty"`
	// Args provides optional arguments for the container entrypoint.
	Args []string `json:"args,omitempty"`
	// HuggingFaceTokenSecretRef references a Secret with token for private model download.
	HuggingFaceTokenSecretRef *SecretReference `json:"huggingFaceTokenSecretRef,omitempty"`
	// Memory specifies memory limit for the download job container (e.g. "128Mi", "1Gi").
	Memory string `json:"memory,omitempty"`
	// ChunkMaxMiB caps multipart chunk size in MiB (defaults to 64)
	ChunkMaxMiB int32 `json:"chunkMaxMiB,omitempty"`
	// Concurrency sets the number of parallel parts used by S3 transfer (defaults to 1)
	Concurrency int32 `json:"concurrency,omitempty"`
}

func (in *ModelDownloadSpec) DeepCopy() *ModelDownloadSpec {
	if in == nil {
		return nil
	}
	out := new(ModelDownloadSpec)
	*out = *in
	if in.Command != nil {
		out.Command = make([]string, len(in.Command))
		copy(out.Command, in.Command)
	}
	if in.Args != nil {
		out.Args = make([]string, len(in.Args))
		copy(out.Args, in.Args)
	}
	if in.HuggingFaceTokenSecretRef != nil {
		secretCopy := *in.HuggingFaceTokenSecretRef
		out.HuggingFaceTokenSecretRef = &secretCopy
	}
	return out
}

// ModelConversionSpec configures post-download conversion of cached artifacts (e.g. GGUF export).
type ModelConversionSpec struct {
	// Image is the container image used for the conversion Job.
	Image string `json:"image,omitempty"`
	// Command overrides the container entrypoint when set.
	Command []string `json:"command,omitempty"`
	// Args provides optional arguments for the container entrypoint.
	Args []string `json:"args,omitempty"`
	// WeightsFloatType dictates the target precision (for example, "q40").
	WeightsFloatType string `json:"weightsFloatType,omitempty"`
	// OutputPath optionally overrides the cache destination for converted artifacts (may be s3:// URI).
	OutputPath string `json:"outputPath,omitempty"`
	// Memory specifies memory limit for the conversion job container.
	Memory string `json:"memory,omitempty"`
	// RcloneImage overrides the image used for rclone-based syncing/mounting (defaults to rclone/rclone:1.67).
	RcloneImage string `json:"rcloneImage,omitempty"`
	// ToolsImage overrides the image used for lightweight operations (defaults to alpine:3.18).
	ToolsImage string `json:"toolsImage,omitempty"`
}

func (in *ModelConversionSpec) DeepCopy() *ModelConversionSpec {
	if in == nil {
		return nil
	}
	out := new(ModelConversionSpec)
	*out = *in
	if in.Command != nil {
		out.Command = make([]string, len(in.Command))
		copy(out.Command, in.Command)
	}
	if in.Args != nil {
		out.Args = make([]string, len(in.Args))
		copy(out.Args, in.Args)
	}
	return out
}
