package v1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the Kubernetes API group for the custom resources managed by the operator.
	GroupName = "koldun.gorizond.io"
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
		&Ingress{},
		&IngressList{},
		&Session{},
		&SessionList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=dll
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="ReadyWorkers",type=integer,JSONPath=`.status.readyWorkers`
// +kubebuilder:printcolumn:name="ReplicaPower",type=integer,JSONPath=`.spec.replicaPower`

// Dllama describes a distributed-llama topology managed by the operator.
type Dllama struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DllamaSpec   `json:"spec,omitempty"`
	Status DllamaStatus `json:"status,omitempty"`
}

// DllamaSpec captures the desired state for a distributed-llama deployment.
type DllamaSpec struct {
	// ModelRef points to an existing Model resource that must be prepared before workers are created.
	ModelRef ModelReference `json:"modelRef"`
	// ReplicaPower controls how many Worker resources are spawned once the referenced model is ready.
	ReplicaPower int32 `json:"replicaPower"`
	// RootImage is the container image used for the root coordinator process.
	RootImage string `json:"rootImage"`
	// WorkerImage is the container image used for the worker processes.
	WorkerImage string `json:"workerImage"`
	// NATS configures connectivity inherited by runtime pods.
	NATS *DllamaNATSConfig `json:"nats,omitempty"`
}

// DllamaNATSConfig configures NATS connectivity for the LLM sidecar spawned by the root deployment.
type DllamaNATSConfig struct {
	// URL is the NATS endpoint.
	URL string `json:"url"`
	// CredentialsSecret references a Secret containing NATS credentials (optional).
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`
}

// Validate ensures required fields are set.
func (c *DllamaNATSConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("nats config is nil")
	}
	if strings.TrimSpace(c.URL) == "" {
		return fmt.Errorf("nats url is required")
	}
	return nil
}

// ToRootConfig converts the dllama NATS configuration into a RootNATSConfig.
func (c *DllamaNATSConfig) ToRootConfig() *RootNATSConfig {
	if c == nil {
		return nil
	}
	return &RootNATSConfig{
		URL:               c.URL,
		CredentialsSecret: c.CredentialsSecret,
	}
}

// ToWorkerConfig converts the dllama NATS configuration into a WorkerNATSConfig.
func (c *DllamaNATSConfig) ToWorkerConfig() *WorkerNATSConfig {
	if c == nil {
		return nil
	}
	return &WorkerNATSConfig{
		URL:               c.URL,
		CredentialsSecret: c.CredentialsSecret,
	}
}

// DeepCopy for DllamaSpec should include NATS.
func (in *DllamaSpec) DeepCopy() DllamaSpec {
	if in == nil {
		return DllamaSpec{}
	}
	out := *in
	if in.NATS != nil {
		natsCopy := *in.NATS.DeepCopy()
		out.NATS = &natsCopy
	}
	return out
}

// DeepCopy for DllamaNATSConfig.
func (in *DllamaNATSConfig) DeepCopy() *DllamaNATSConfig {
	if in == nil {
		return nil
	}
	out := new(DllamaNATSConfig)
	*out = *in
	if in.CredentialsSecret != nil {
		secretCopy := *in.CredentialsSecret
		out.CredentialsSecret = &secretCopy
	}
	return out
}

// ModelReference identifies a Model custom resource that should be used by this Dllama deployment.
type ModelReference struct {
	// APIGroup optionally overrides the default API group for the referenced Model resource.
	APIGroup string `json:"apiGroup,omitempty"`
	// Kind must be set to "Model".
	Kind string `json:"kind"`
	// Name is the metadata.name of the referenced Model resource.
	Name string `json:"name"`
	// Namespace allows referencing a Model in another namespace. Defaults to the Dllama namespace when empty.
	Namespace string `json:"namespace,omitempty"`
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
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Download",type=string,JSONPath=`.status.downloadState`
// +kubebuilder:printcolumn:name="Conversion",type=string,JSONPath=`.status.conversionState`
// +kubebuilder:printcolumn:name="OutputPVC",type=string,priority=1,JSONPath=`.status.outputPVCName`

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
	// ObjectStorage describes access to object storage buckets used for downloading and conversion output.
	ObjectStorage *ModelObjectStorageSpec `json:"objectStorage,omitempty"`
	// PipProxy optionally sets HTTP proxy used by pip inside python containers (e.g., Dragonfly dfdaemon HTTP proxy)
	PipProxy string `json:"pipProxy,omitempty"`
	// LaunchOptions mirror the arguments passed to distributed-llama's download sequence.
	LaunchOptions []string `json:"launchOptions,omitempty"`
	// Download contains configuration for how the model artifacts are fetched and prepared.
	Download *ModelDownloadSpec `json:"download,omitempty"`
	// Conversion optionally describes how cached artifacts should be converted post-download.
	Conversion *ModelConversionSpec `json:"conversion,omitempty"`
	// PV optionally describes a template for PersistentVolume/PersistentVolumeClaim used to mount S3 via CSI
	PV *ModelPVSpec `json:"pv,omitempty"`
	// ReplicaPower defines the desired worker fan-out (power of two) for Dllama instances referencing this model.
	ReplicaPower int32 `json:"replicaPower,omitempty"`
	// PreConverted indicates the model files are already in dllama format in the object storage.
	// When true, download and conversion jobs are skipped and the model is immediately marked Ready.
	PreConverted bool `json:"preConverted,omitempty"`
	// PreConvertedSizeBytes is the total size in bytes of pre-converted model files.
	// Required when PreConverted is true for registry sync to work.
	PreConvertedSizeBytes int64 `json:"preConvertedSizeBytes,omitempty"`
	// PreConvertedSizeHuman is a human-readable size (e.g., "805 MB") for pre-converted models.
	// If not provided, it will be calculated from PreConvertedSizeBytes.
	PreConvertedSizeHuman string `json:"preConvertedSizeHuman,omitempty"`
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
	// ConversionSizeJobName references the sizing Job that inspects converted artifacts.
	ConversionSizeJobName string `json:"conversionSizeJobName,omitempty"`
	// ConversionSizeState tracks the state of the sizing job (Pending, Running, Succeeded, Failed).
	ConversionSizeState string `json:"conversionSizeState,omitempty"`
	// ConversionSizeBytes stores the total size in bytes of converted artifacts measured from the output PVC.
	ConversionSizeBytes int64 `json:"conversionSizeBytes,omitempty"`
	// ConversionSizeHuman stores a human-readable representation of the converted artifact size (e.g. 1.2G).
	ConversionSizeHuman string `json:"conversionSizeHuman,omitempty"`
	// ConversionSizeGeneration records the Model generation for which the size measurement was produced.
	ConversionSizeGeneration int64 `json:"conversionSizeGeneration,omitempty"`
	// ConversionSizeForceToken mirrors the annotation used to force a sizing rerun that produced the recorded measurement.
	ConversionSizeForceToken string `json:"conversionSizeForceToken,omitempty"`
	// OutputPVCName is the PersistentVolumeClaim that stores converted artifacts.
	OutputPVCName string `json:"outputPVCName,omitempty"`
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
	// NATS contains connection information inherited by the LLM sidecar.
	NATS *RootNATSConfig `json:"nats,omitempty"`
	// Memory allows overriding root memory planning parameters.
	Memory *RootMemorySpec `json:"memory,omitempty"`
}

// RootMemorySpec customizes root memory calculations.
type RootMemorySpec struct {
	// OverheadMaxRatio overrides the maximum memory multiplier for the root.
	OverheadMaxRatio *float64 `json:"overheadMaxRatio,omitempty"`
}

// RootStatus indicates readiness for the root component.
type RootStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Endpoint           string             `json:"endpoint,omitempty"`
}

// RootNATSConfig configures NATS connectivity for the LLM sidecar spawned by the root deployment.
type RootNATSConfig struct {
	// URL is the NATS endpoint (may include inline credentials).
	URL string `json:"url"`
	// CredentialsSecret references a Secret containing connection credentials.
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`
}

// GetURL returns the configured NATS URL or empty string.
func (c *RootNATSConfig) GetURL() string {
	if c == nil {
		return ""
	}
	return c.URL
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
	// NATS configures worker connectivity.
	NATS *WorkerNATSConfig `json:"nats,omitempty"`
}

// WorkerNATSConfig configures NATS connectivity for worker pods.
type WorkerNATSConfig struct {
	URL string `json:"url"`
	// CredentialsSecret references a Secret containing credentials.
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`
}

// DeepCopy extends WorkerSpec to copy the NATS configuration.
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
	if in.NATS != nil {
		natsCopy := *in.NATS.DeepCopy()
		out.NATS = &natsCopy
	}
	return out
}

// DeepCopy for WorkerNATSConfig.
func (in *WorkerNATSConfig) DeepCopy() *WorkerNATSConfig {
	if in == nil {
		return nil
	}
	out := new(WorkerNATSConfig)
	*out = *in
	if in.CredentialsSecret != nil {
		secretCopy := *in.CredentialsSecret
		out.CredentialsSecret = &secretCopy
	}
	return out
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

// ModelObjectStorageSpec configures the buckets backing model downloads and converted outputs.
type ModelObjectStorageSpec struct {
	// Endpoint is the URL of the object storage backend (e.g. S3 compatible object storage).
	Endpoint string `json:"endpoint"`
	// SecretRef references a Secret containing credentials for the storage backend.
	SecretRef *SecretReference `json:"secretRef,omitempty"`
	// BucketForSource holds the original Hugging Face snapshot and any downloaded artifacts.
	BucketForSource string `json:"bucketForSource"`
	// BucketForConvert stores converted artifacts generated after running conversion jobs.
	BucketForConvert string `json:"bucketForConvert"`
}

func (in *ModelObjectStorageSpec) DeepCopy() *ModelObjectStorageSpec {
	if in == nil {
		return nil
	}
	out := new(ModelObjectStorageSpec)
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
	if in.ObjectStorage != nil {
		storageCopy := *in.ObjectStorage.DeepCopy()
		out.ObjectStorage = &storageCopy
	}
	if in.Download != nil {
		downloadCopy := *in.Download.DeepCopy()
		out.Download = &downloadCopy
	}
	if in.Conversion != nil {
		conversionCopy := *in.Conversion.DeepCopy()
		out.Conversion = &conversionCopy
	}
	if in.PV != nil {
		pvCopy := *in.PV.DeepCopy()
		out.PV = &pvCopy
	}
	return out
}

// ModelPVSpec allows configuring the generated PV/PVC. Any omitted fields use sensible defaults.
type ModelPVSpec struct {
	// StorageClassName for the PV (default: "csi-s3")
	StorageClassName string `json:"storageClassName,omitempty"`
	// Capacity for the PV, e.g. "10Gi" (default: "10Gi")
	Capacity string `json:"capacity,omitempty"`
	// AccessModes for the PV, e.g. ["ReadWriteMany"] (default: ["ReadWriteMany"])
	AccessModes []string `json:"accessModes,omitempty"`
	// ReclaimPolicy for the PV (default: "Retain")
	ReclaimPolicy string `json:"reclaimPolicy,omitempty"`
	// CSIDriver name (default: "ru.yandex.s3.csi")
	CSIDriver string `json:"csiDriver,omitempty"`
	// CSIMounter (e.g., "geesefs"), placed into volumeAttributes.mounter
	CSIMounter string `json:"csiMounter,omitempty"`
	// CSIOptions (arbitrary string passed into volumeAttributes.options)
	CSIOptions string `json:"csiOptions,omitempty"`
	// Additional volumeAttributes appended to CSI volumeAttributes
	VolumeAttributes map[string]string `json:"volumeAttributes,omitempty"`

	// CSISecretName overrides the default name of the CSI secret (default: csi-s3-secret)
	CSISecretName string `json:"csiSecretName,omitempty"`
	// CSISecretNamespace overrides the default namespace of the CSI secret (default: kube-system)
	CSISecretNamespace string `json:"csiSecretNamespace,omitempty"`

	// PVCStorageClassName (default: "") disables dynamic provisioning for static binding
	PVCStorageClassName string `json:"pvcStorageClassName,omitempty"`
	// PVCCapacity request (default: same as PV Capacity or "10Gi")
	PVCCapacity string `json:"pvcCapacity,omitempty"`
	// PVCAccessModes (default: ["ReadWriteMany"])
	PVCAccessModes []string `json:"pvcAccessModes,omitempty"`
}

func (in *ModelPVSpec) DeepCopy() *ModelPVSpec {
	if in == nil {
		return nil
	}
	out := new(ModelPVSpec)
	*out = *in
	if in.AccessModes != nil {
		out.AccessModes = make([]string, len(in.AccessModes))
		copy(out.AccessModes, in.AccessModes)
	}
	if in.VolumeAttributes != nil {
		out.VolumeAttributes = make(map[string]string, len(in.VolumeAttributes))
		for k, v := range in.VolumeAttributes {
			out.VolumeAttributes[k] = v
		}
	}
	if in.PVCAccessModes != nil {
		out.PVCAccessModes = make([]string, len(in.PVCAccessModes))
		copy(out.PVCAccessModes, in.PVCAccessModes)
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
	if in.NATS != nil {
		natsCopy := *in.NATS.DeepCopy()
		out.NATS = &natsCopy
	}
	if in.Memory != nil {
		memCopy := *in.Memory.DeepCopy()
		out.Memory = &memCopy
	}
	return out
}

func (in *RootMemorySpec) DeepCopy() *RootMemorySpec {
	if in == nil {
		return nil
	}
	out := new(RootMemorySpec)
	*out = *in
	if in.OverheadMaxRatio != nil {
		value := *in.OverheadMaxRatio
		out.OverheadMaxRatio = &value
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

func (in *RootNATSConfig) DeepCopy() *RootNATSConfig {
	if in == nil {
		return nil
	}
	out := new(RootNATSConfig)
	*out = *in
	if in.CredentialsSecret != nil {
		secretCopy := *in.CredentialsSecret
		out.CredentialsSecret = &secretCopy
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

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sess

// Session models a user conversation with lifecycle and worker pool state.
type Session struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SessionSpec   `json:"spec,omitempty"`
	Status SessionStatus `json:"status,omitempty"`
}

// SessionSpec captures desired configuration for a conversation session.
type SessionSpec struct {
	// Hash is the unique identifier computed from token/chat metadata.
	Hash string `json:"hash"`
	// ModelRef identifies the model that should serve this session.
	ModelRef ModelReference `json:"modelRef"`
	// RootImage is the image used by root Dllama coordinators.
	RootImage string `json:"rootImage"`
	// WorkerImage is the image used by worker Dllama pods.
	WorkerImage string `json:"workerImage"`
	// DispatcherImage is the image that runs the per-session dispatcher deployment.
	DispatcherImage string `json:"dispatcherImage,omitempty"`
	// DispatcherMetricsListen enables the dispatcher metrics listener (host:port or :port form).
	DispatcherMetricsListen string `json:"dispatcherMetricsListen,omitempty"`
	// ReplicaPower controls worker topology for spawned Dllamas (power of two).
	ReplicaPower int32 `json:"replicaPower,omitempty"`
	// MinIdle enforces a minimum number of idle Dllama workers kept ready (deprecated in favour of Scaling.MinDllamas).
	MinIdle int32 `json:"minIdle,omitempty"`
	// MaxWorkers bounds the number of concurrent Dllama worker sets (deprecated in favour of Scaling.MaxDllamas).
	MaxWorkers int32 `json:"maxWorkers,omitempty"`
	// Scaling controls automatic creation/removal of Dllama resources for the session.
	Scaling *SessionScalingSpec `json:"scaling,omitempty"`
	// Queue defines NATS subjects/streams used for request backlog.
	Queue *SessionQueueSpec `json:"queue,omitempty"`
	// NATS contains session specific overrides for runtime connectivity.
	NATS *SessionNATSConfig `json:"nats,omitempty"`
	// TTL controls when the session and its resources should be garbage collected when idle.
	TTL *metav1.Duration `json:"ttl,omitempty"`
}

// SessionScalingSpec defines desired pool sizing behaviour for a session.
type SessionScalingSpec struct {
	MinDllamas           int32 `json:"minDllamas,omitempty"`
	MaxDllamas           int32 `json:"maxDllamas,omitempty"`
	ScaleUpBacklog       int32 `json:"scaleUpBacklog,omitempty"`
	ScaleDownIdleSeconds int32 `json:"scaleDownIdleSeconds,omitempty"`
	DesiredDllamas       int32 `json:"desiredDllamas,omitempty"`
}

// SessionQueueSpec configures NATS subjects used for session backlogs.
type SessionQueueSpec struct {
	// BacklogSubject stores pending user messages.
	BacklogSubject string `json:"backlogSubject,omitempty"`
	// ResponseSubjectPrefix prefixes per-message response subjects.
	ResponseSubjectPrefix string `json:"responseSubjectPrefix,omitempty"`
	// AssignmentsBucket stores per-request dispatcher assignments for idempotency.
	AssignmentsBucket string `json:"assignmentsBucket,omitempty"`
	// DllamaSubjectPrefix prefixes per-dllama request subjects (must end with '.').
	DllamaSubjectPrefix string `json:"dllamaSubjectPrefix,omitempty"`
	// StateStream stores worker heartbeat events for dispatcher recovery.
	StateStream string `json:"stateStream,omitempty"`
	// AckTimeout determines when a message lease is considered expired.
	AckTimeout *metav1.Duration `json:"ackTimeout,omitempty"`
}

// SessionNATSConfig configures overrides for the session runtime.
type SessionNATSConfig struct {
	URL string `json:"url,omitempty"`
	// CredentialsSecret references a Secret containing connection credentials.
	CredentialsSecret *SecretReference `json:"credentialsSecret,omitempty"`
}

// SessionStatus reflects current backlog and worker state for a session.
type SessionStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ReadyWorkers       int32              `json:"readyWorkers,omitempty"`
	BusyWorkers        int32              `json:"busyWorkers,omitempty"`
	AvailableWorkers   int32              `json:"availableWorkers,omitempty"`
	Backlog            int64              `json:"backlog,omitempty"`
	InFlight           int64              `json:"inFlight,omitempty"`
	ActiveRequests     int32              `json:"activeRequests,omitempty"`
	Workers            []SessionWorker    `json:"workers,omitempty"`
	LastActivity       *metav1.Time       `json:"lastActivity,omitempty"`
}

// SessionWorker summarises state for a Dllama worker set handling the session.
type SessionWorker struct {
	Name           string       `json:"name"`
	Phase          string       `json:"phase,omitempty"`
	Endpoint       string       `json:"endpoint,omitempty"`
	Ready          bool         `json:"ready,omitempty"`
	Healthy        bool         `json:"healthy,omitempty"`
	ActiveMessages int32        `json:"activeMessages,omitempty"`
	LastHeartbeat  *metav1.Time `json:"lastHeartbeat,omitempty"`
}

// +kubebuilder:object:root=true

// SessionList contains multiple Session resources.
type SessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Session `json:"items"`
}

func (in *Session) DeepCopyInto(out *Session) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = *in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}

func (in *Session) DeepCopy() *Session {
	if in == nil {
		return nil
	}
	out := new(Session)
	in.DeepCopyInto(out)
	return out
}

func (in *Session) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *SessionSpec) DeepCopy() *SessionSpec {
	if in == nil {
		return nil
	}
	out := new(SessionSpec)
	*out = *in
	if in.Queue != nil {
		out.Queue = in.Queue.DeepCopy()
	}
	if in.Scaling != nil {
		scalingCopy := *in.Scaling.DeepCopy()
		out.Scaling = &scalingCopy
	}
	if in.NATS != nil {
		natsCopy := *in.NATS.DeepCopy()
		out.NATS = &natsCopy
	}
	if in.TTL != nil {
		d := *in.TTL
		out.TTL = &d
	}
	return out
}

func (in *SessionScalingSpec) DeepCopy() *SessionScalingSpec {
	if in == nil {
		return nil
	}
	out := new(SessionScalingSpec)
	*out = *in
	return out
}

func (in *SessionQueueSpec) DeepCopy() *SessionQueueSpec {
	if in == nil {
		return nil
	}
	out := new(SessionQueueSpec)
	*out = *in
	if in.AckTimeout != nil {
		d := *in.AckTimeout
		out.AckTimeout = &d
	}
	return out
}

func (in *SessionNATSConfig) DeepCopy() *SessionNATSConfig {
	if in == nil {
		return nil
	}
	out := new(SessionNATSConfig)
	*out = *in
	if in.CredentialsSecret != nil {
		secretCopy := *in.CredentialsSecret
		out.CredentialsSecret = &secretCopy
	}
	return out
}

func (in *SessionStatus) DeepCopy() *SessionStatus {
	if in == nil {
		return nil
	}
	out := new(SessionStatus)
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
	if in.Workers != nil {
		out.Workers = make([]SessionWorker, len(in.Workers))
		for i := range in.Workers {
			in.Workers[i].DeepCopyInto(&out.Workers[i])
		}
	}
	if in.LastActivity != nil {
		t := *in.LastActivity
		out.LastActivity = &t
	}
	return out
}

func (in *SessionWorker) DeepCopyInto(out *SessionWorker) {
	*out = *in
	if in.LastHeartbeat != nil {
		t := *in.LastHeartbeat
		out.LastHeartbeat = &t
	}
}

func (in *SessionWorker) DeepCopy() *SessionWorker {
	if in == nil {
		return nil
	}
	out := new(SessionWorker)
	in.DeepCopyInto(out)
	return out
}

func (in *SessionList) DeepCopyInto(out *SessionList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Session, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *SessionList) DeepCopy() *SessionList {
	if in == nil {
		return nil
	}
	out := new(SessionList)
	in.DeepCopyInto(out)
	return out
}

func (in *SessionList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
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

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=ing
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Host",type=string,JSONPath=`.spec.route.host`
// +kubebuilder:printcolumn:name="Service",type=string,priority=1,JSONPath=`.spec.service.type`
// +kubebuilder:printcolumn:name="ReplicaPower",type=integer,priority=1,JSONPath=`.spec.backend.replicaPower`

// Ingress defines an HTTP entrypoint backed by a managed backend deployment.
type Ingress struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IngressSpec   `json:"spec,omitempty"`
	Status IngressStatus `json:"status,omitempty"`
}

// IngressSpec describes the desired state for the backend ingress deployment.
type IngressSpec struct {
	Backend IngressBackendSpec  `json:"backend"`
	Route   IngressRouteSpec    `json:"route"`
	Service *IngressServiceSpec `json:"service,omitempty"`
}

// IngressBackendSpec defines runtime configuration for the backend deployment.
type IngressBackendSpec struct {
	Image           string `json:"image"`
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
	RootImage       string `json:"rootImage"`
	WorkerImage     string `json:"workerImage"`
	DispatcherImage string `json:"dispatcherImage,omitempty"`
	// DispatcherMetricsListen configures --dispatcher-metrics-listen for session-managed dispatchers.
	DispatcherMetricsListen string `json:"dispatcherMetricsListen,omitempty"`
	// ReplicaPower overrides the replica power configured on models when ensuring Sessions.
	ReplicaPower    int32                       `json:"replicaPower,omitempty"`
	HashSecret      string                      `json:"hashSecret,omitempty"`
	AllowAnonymous  bool                        `json:"allowAnonymous,omitempty"`
	NATS            IngressNATSConfig           `json:"nats"`
	ConversationTTL *metav1.Duration            `json:"conversationTTL,omitempty"`
	ResponseTimeout *metav1.Duration            `json:"responseTimeout,omitempty"`
	SessionScaling  *IngressSessionScalingSpec  `json:"sessionScaling,omitempty"`
	ExtraArgs       []string                    `json:"extraArgs,omitempty"`
	Resources       corev1.ResourceRequirements `json:"resources,omitempty"`
	RootMemory      *IngressRootMemorySpec      `json:"rootMemory,omitempty"`
}

// IngressRootMemorySpec customises root memory calculation.
type IngressRootMemorySpec struct {
	OverheadMaxRatio *float64 `json:"overheadMaxRatio,omitempty"`
}

// DeepCopy for IngressRootMemorySpec.
func (in *IngressRootMemorySpec) DeepCopy() *IngressRootMemorySpec {
	if in == nil {
		return nil
	}
	out := new(IngressRootMemorySpec)
	*out = *in
	if in.OverheadMaxRatio != nil {
		value := *in.OverheadMaxRatio
		out.OverheadMaxRatio = &value
	}
	return out
}

// IngressSessionScalingSpec controls auto-scaling behaviour for Session-managed Dllamas.
type IngressSessionScalingSpec struct {
	MinDllamas           int32 `json:"minDllamas,omitempty"`
	MaxDllamas           int32 `json:"maxDllamas,omitempty"`
	ScaleUpBacklog       int32 `json:"scaleUpBacklog,omitempty"`
	ScaleDownIdleSeconds int32 `json:"scaleDownIdleSeconds,omitempty"`
}

// IngressNATSConfig configures connectivity with the conversation control plane.
type IngressNATSConfig struct {
	URL                string `json:"url"`
	ConversationBucket string `json:"kvBucket,omitempty"`
	ModelsBucket       string `json:"modelsBucket,omitempty"`
	TokensBucket       string `json:"tokensBucket,omitempty"`
	ModelPrefix        string `json:"modelPrefix,omitempty"`
	TokenPrefix        string `json:"tokenPrefix,omitempty"`
	TTLPrefix          string `json:"ttlPrefix,omitempty"`
}

// IngressServiceSpec configures the Service exposing the backend deployment.
type IngressServiceSpec struct {
	Type string `json:"type,omitempty"`
	Port int32  `json:"port,omitempty"`
}

// IngressRouteSpec configures the Kubernetes Ingress resource fronting the backend Service.
type IngressRouteSpec struct {
	Host             string            `json:"host"`
	Path             string            `json:"path,omitempty"`
	PathType         string            `json:"pathType,omitempty"`
	IngressClassName string            `json:"ingressClassName,omitempty"`
	Annotations      map[string]string `json:"annotations,omitempty"`
	TLS              []IngressTLSSpec  `json:"tls,omitempty"`
}

// IngressTLSSpec mirrors networking.k8s.io/v1 IngressTLS configuration.
type IngressTLSSpec struct {
	SecretName string   `json:"secretName,omitempty"`
	Hosts      []string `json:"hosts,omitempty"`
}

// IngressStatus reports readiness details for the managed backend ingress.
type IngressStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	BackendServiceName string             `json:"backendServiceName,omitempty"`
	IngressName        string             `json:"ingressName,omitempty"`
}

// +kubebuilder:object:root=true

// IngressList represents a list of Ingress resources.
type IngressList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Ingress `json:"items"`
}

func (in *Ingress) DeepCopyInto(out *Ingress) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = *in.Spec.DeepCopy()
	out.Status = *in.Status.DeepCopy()
}

func (in *Ingress) DeepCopy() *Ingress {
	if in == nil {
		return nil
	}
	out := new(Ingress)
	in.DeepCopyInto(out)
	return out
}

func (in *Ingress) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *IngressSpec) DeepCopy() *IngressSpec {
	if in == nil {
		return nil
	}
	out := new(IngressSpec)
	*out = *in
	out.Backend = *in.Backend.DeepCopy()
	out.Route = *in.Route.DeepCopy()
	if in.Service != nil {
		serviceCopy := *in.Service.DeepCopy()
		out.Service = &serviceCopy
	}
	return out
}

func (in *IngressBackendSpec) DeepCopy() *IngressBackendSpec {
	if in == nil {
		return nil
	}
	out := new(IngressBackendSpec)
	*out = *in
	if in.ConversationTTL != nil {
		d := *in.ConversationTTL
		out.ConversationTTL = &d
	}
	if in.ResponseTimeout != nil {
		d := *in.ResponseTimeout
		out.ResponseTimeout = &d
	}
	if in.SessionScaling != nil {
		scalingCopy := *in.SessionScaling.DeepCopy()
		out.SessionScaling = &scalingCopy
	}
	if in.ExtraArgs != nil {
		out.ExtraArgs = make([]string, len(in.ExtraArgs))
		copy(out.ExtraArgs, in.ExtraArgs)
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.RootMemory != nil {
		memCopy := *in.RootMemory.DeepCopy()
		out.RootMemory = &memCopy
	}
	return out
}

func (in *IngressSessionScalingSpec) DeepCopy() *IngressSessionScalingSpec {
	if in == nil {
		return nil
	}
	out := new(IngressSessionScalingSpec)
	*out = *in
	return out
}

func (in *IngressNATSConfig) DeepCopy() *IngressNATSConfig {
	if in == nil {
		return nil
	}
	out := new(IngressNATSConfig)
	*out = *in
	return out
}

func (in *IngressServiceSpec) DeepCopy() *IngressServiceSpec {
	if in == nil {
		return nil
	}
	out := new(IngressServiceSpec)
	*out = *in
	return out
}

func (in *IngressRouteSpec) DeepCopy() *IngressRouteSpec {
	if in == nil {
		return nil
	}
	out := new(IngressRouteSpec)
	*out = *in
	if in.Annotations != nil {
		out.Annotations = make(map[string]string, len(in.Annotations))
		for k, v := range in.Annotations {
			out.Annotations[k] = v
		}
	}
	if in.TLS != nil {
		out.TLS = make([]IngressTLSSpec, len(in.TLS))
		for i := range in.TLS {
			out.TLS[i] = *in.TLS[i].DeepCopy()
		}
	}
	return out
}

func (in IngressTLSSpec) DeepCopy() *IngressTLSSpec {
	out := new(IngressTLSSpec)
	*out = in
	if in.Hosts != nil {
		out.Hosts = make([]string, len(in.Hosts))
		copy(out.Hosts, in.Hosts)
	}
	return out
}

func (in *IngressStatus) DeepCopy() *IngressStatus {
	if in == nil {
		return nil
	}
	out := new(IngressStatus)
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		copy(out.Conditions, in.Conditions)
	}
	return out
}

func (in *IngressList) DeepCopyInto(out *IngressList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Ingress, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *IngressList) DeepCopy() *IngressList {
	if in == nil {
		return nil
	}
	out := new(IngressList)
	in.DeepCopyInto(out)
	return out
}

func (in *IngressList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
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
	// ConvertWeights controls execution of HF weight conversion (handled by convert-hf.py).
	ConvertWeights string `json:"convertWeights,omitempty"`
	// OutputPath optionally overrides the cache destination for converted artifacts (may be s3:// URI).
	OutputPath string `json:"outputPath,omitempty"`
	// Memory specifies memory limit for the conversion job container.
	Memory string `json:"memory,omitempty"`
	// RcloneImage overrides the image used for rclone-based syncing/mounting (defaults to rclone/rclone:1.67).
	RcloneImage string `json:"rcloneImage,omitempty"`
	// ToolsImage overrides the image used for lightweight operations (defaults to alpine:3.18).
	ToolsImage string `json:"toolsImage,omitempty"`
	// ConverterVersion selects the distributed-llama converter release tag (default: v0.16.2).
	ConverterVersion string `json:"converterVersion,omitempty"`
	// Dependencies enumerates additional Python packages required for conversion, keyed by package name with optional version.
	Dependencies map[string]string `json:"dependencies,omitempty"`
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
	if in.Dependencies != nil {
		out.Dependencies = make(map[string]string, len(in.Dependencies))
		for k, v := range in.Dependencies {
			out.Dependencies[k] = v
		}
	}
	return out
}
