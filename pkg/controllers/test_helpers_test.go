package controllers

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/nats-io/nats.go"
	"github.com/rancher/wrangler/v3/pkg/apply"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	"github.com/rancher/wrangler/v3/pkg/apply/injectors"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/objectset"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeApply is a lightweight apply.Apply test spy that records builder state
// while delegating behaviour to wrangler's in-memory FakeApply.
type fakeApply struct {
	delegate          *fakeapply.FakeApply
	appliedObjects    []runtime.Object
	setID             string
	defaultNamespace  string
	owner             runtime.Object
	ownerKey          string
	ownerGVK          schema.GroupVersionKind
	context           context.Context
	strictCaching     bool
	restrictCluster   bool
	rateLimitingQPS   float32
	ignorePrevApplied bool
}

func newFakeApply() *fakeApply {
	return &fakeApply{delegate: &fakeapply.FakeApply{}}
}

func (f *fakeApply) Apply(set *objectset.ObjectSet) error {
	return f.delegate.Apply(set)
}

func (f *fakeApply) ApplyObjects(objs ...runtime.Object) error {
	f.appliedObjects = append(f.appliedObjects, objs...)
	return f.delegate.ApplyObjects(objs...)
}

func (f *fakeApply) WithContext(ctx context.Context) apply.Apply {
	f.context = ctx
	f.delegate.WithContext(ctx)
	return f
}

func (f *fakeApply) WithCacheTypes(igs ...apply.InformerGetter) apply.Apply {
	f.delegate.WithCacheTypes(igs...)
	return f
}

func (f *fakeApply) WithCacheTypeFactory(factory apply.InformerFactory) apply.Apply {
	f.delegate.WithCacheTypeFactory(factory)
	return f
}

func (f *fakeApply) WithSetID(id string) apply.Apply {
	f.setID = id
	f.delegate.WithSetID(id)
	return f
}

func (f *fakeApply) WithOwner(obj runtime.Object) apply.Apply {
	f.owner = obj
	f.delegate.WithOwner(obj)
	return f
}

func (f *fakeApply) WithOwnerKey(key string, gvk schema.GroupVersionKind) apply.Apply {
	f.ownerKey = key
	f.ownerGVK = gvk
	f.delegate.WithOwnerKey(key, gvk)
	return f
}

func (f *fakeApply) WithInjector(injs ...injectors.ConfigInjector) apply.Apply {
	f.delegate.WithInjector(injs...)
	return f
}

func (f *fakeApply) WithInjectorName(names ...string) apply.Apply {
	f.delegate.WithInjectorName(names...)
	return f
}

func (f *fakeApply) WithPatcher(gvk schema.GroupVersionKind, patchers apply.Patcher) apply.Apply {
	f.delegate.WithPatcher(gvk, patchers)
	return f
}

func (f *fakeApply) WithReconciler(gvk schema.GroupVersionKind, reconciler apply.Reconciler) apply.Apply {
	f.delegate.WithReconciler(gvk, reconciler)
	return f
}

func (f *fakeApply) WithStrictCaching() apply.Apply {
	f.strictCaching = true
	f.delegate.WithStrictCaching()
	return f
}

func (f *fakeApply) WithDynamicLookup() apply.Apply {
	f.delegate.WithDynamicLookup()
	return f
}

func (f *fakeApply) WithRestrictClusterScoped() apply.Apply {
	f.restrictCluster = true
	f.delegate.WithRestrictClusterScoped()
	return f
}

func (f *fakeApply) WithDefaultNamespace(ns string) apply.Apply {
	f.defaultNamespace = ns
	f.delegate.WithDefaultNamespace(ns)
	return f
}

func (f *fakeApply) WithListerNamespace(ns string) apply.Apply {
	f.delegate.WithListerNamespace(ns)
	return f
}

func (f *fakeApply) WithRateLimiting(qps float32) apply.Apply {
	f.rateLimitingQPS = qps
	f.delegate.WithRateLimiting(qps)
	return f
}

func (f *fakeApply) WithNoDelete() apply.Apply {
	f.delegate.WithNoDelete()
	return f
}

func (f *fakeApply) WithNoDeleteGVK(gvks ...schema.GroupVersionKind) apply.Apply {
	f.delegate.WithNoDeleteGVK(gvks...)
	return f
}

func (f *fakeApply) WithGVK(gvks ...schema.GroupVersionKind) apply.Apply {
	f.delegate.WithGVK(gvks...)
	return f
}

func (f *fakeApply) WithSetOwnerReference(controller, block bool) apply.Apply {
	f.delegate.WithSetOwnerReference(controller, block)
	return f
}

func (f *fakeApply) WithIgnorePreviousApplied() apply.Apply {
	f.ignorePrevApplied = true
	f.delegate.WithIgnorePreviousApplied()
	return f
}

func (f *fakeApply) WithDiffPatch(gvk schema.GroupVersionKind, namespace, name string, patch []byte) apply.Apply {
	f.delegate.WithDiffPatch(gvk, namespace, name, patch)
	return f
}

func (f *fakeApply) FindOwner(obj runtime.Object) (runtime.Object, error) {
	return f.delegate.FindOwner(obj)
}

func (f *fakeApply) PurgeOrphan(obj runtime.Object) error {
	return f.delegate.PurgeOrphan(obj)
}

func (f *fakeApply) DryRun(objs ...runtime.Object) (apply.Plan, error) {
	return f.delegate.DryRun(objs...)
}

// failingApply wraps fakeApply and forces Apply/ApplyObjects to return err
// after recording interactions. This mirrors production failures behaviour for tests.
type failingApply struct {
	fakeApply *fakeApply
	err       error
}

func (f *failingApply) Apply(set *objectset.ObjectSet) error {
	if err := f.fakeApply.Apply(set); err != nil {
		return err
	}
	return f.err
}

func (f *failingApply) ApplyObjects(objs ...runtime.Object) error {
	if err := f.fakeApply.ApplyObjects(objs...); err != nil {
		return err
	}
	return f.err
}

func (f *failingApply) WithContext(ctx context.Context) apply.Apply {
	f.fakeApply.WithContext(ctx)
	return f
}

func (f *failingApply) WithCacheTypes(igs ...apply.InformerGetter) apply.Apply {
	f.fakeApply.WithCacheTypes(igs...)
	return f
}

func (f *failingApply) WithCacheTypeFactory(factory apply.InformerFactory) apply.Apply {
	f.fakeApply.WithCacheTypeFactory(factory)
	return f
}

func (f *failingApply) WithSetID(id string) apply.Apply {
	f.fakeApply.WithSetID(id)
	return f
}

func (f *failingApply) WithOwner(obj runtime.Object) apply.Apply {
	f.fakeApply.WithOwner(obj)
	return f
}

func (f *failingApply) WithOwnerKey(key string, gvk schema.GroupVersionKind) apply.Apply {
	f.fakeApply.WithOwnerKey(key, gvk)
	return f
}

func (f *failingApply) WithInjector(injs ...injectors.ConfigInjector) apply.Apply {
	f.fakeApply.WithInjector(injs...)
	return f
}

func (f *failingApply) WithInjectorName(names ...string) apply.Apply {
	f.fakeApply.WithInjectorName(names...)
	return f
}

func (f *failingApply) WithPatcher(gvk schema.GroupVersionKind, patchers apply.Patcher) apply.Apply {
	f.fakeApply.WithPatcher(gvk, patchers)
	return f
}

func (f *failingApply) WithReconciler(gvk schema.GroupVersionKind, reconciler apply.Reconciler) apply.Apply {
	f.fakeApply.WithReconciler(gvk, reconciler)
	return f
}

func (f *failingApply) WithStrictCaching() apply.Apply {
	f.fakeApply.WithStrictCaching()
	return f
}

func (f *failingApply) WithDynamicLookup() apply.Apply {
	f.fakeApply.WithDynamicLookup()
	return f
}

func (f *failingApply) WithRestrictClusterScoped() apply.Apply {
	f.fakeApply.WithRestrictClusterScoped()
	return f
}

func (f *failingApply) WithDefaultNamespace(ns string) apply.Apply {
	f.fakeApply.WithDefaultNamespace(ns)
	return f
}

func (f *failingApply) WithListerNamespace(ns string) apply.Apply {
	f.fakeApply.WithListerNamespace(ns)
	return f
}

func (f *failingApply) WithRateLimiting(qps float32) apply.Apply {
	f.fakeApply.WithRateLimiting(qps)
	return f
}

func (f *failingApply) WithNoDelete() apply.Apply {
	f.fakeApply.WithNoDelete()
	return f
}

func (f *failingApply) WithNoDeleteGVK(gvks ...schema.GroupVersionKind) apply.Apply {
	f.fakeApply.WithNoDeleteGVK(gvks...)
	return f
}

func (f *failingApply) WithGVK(gvks ...schema.GroupVersionKind) apply.Apply {
	f.fakeApply.WithGVK(gvks...)
	return f
}

func (f *failingApply) WithSetOwnerReference(controller, block bool) apply.Apply {
	f.fakeApply.WithSetOwnerReference(controller, block)
	return f
}

func (f *failingApply) WithIgnorePreviousApplied() apply.Apply {
	f.fakeApply.WithIgnorePreviousApplied()
	return f
}

func (f *failingApply) WithDiffPatch(gvk schema.GroupVersionKind, namespace, name string, patch []byte) apply.Apply {
	f.fakeApply.WithDiffPatch(gvk, namespace, name, patch)
	return f
}

func (f *failingApply) FindOwner(obj runtime.Object) (runtime.Object, error) {
	return f.fakeApply.FindOwner(obj)
}

func (f *failingApply) PurgeOrphan(obj runtime.Object) error {
	return f.fakeApply.PurgeOrphan(obj)
}

func (f *failingApply) DryRun(objs ...runtime.Object) (apply.Plan, error) {
	return f.fakeApply.DryRun(objs...)
}

// fakeKoldInterface allows tests to inject controller stubs into Manager.
type fakeKoldInterface struct {
	dllama  generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	model   generic.ControllerInterface[*v1.Model, *v1.ModelList]
	root    generic.ControllerInterface[*v1.Root, *v1.RootList]
	worker  generic.ControllerInterface[*v1.Worker, *v1.WorkerList]
	ingress generic.ControllerInterface[*v1.Ingress, *v1.IngressList]
	session generic.ControllerInterface[*v1.Session, *v1.SessionList]
}

func (f *fakeKoldInterface) Dllama() generic.ControllerInterface[*v1.Dllama, *v1.DllamaList] {
	return f.dllama
}

func (f *fakeKoldInterface) Model() generic.ControllerInterface[*v1.Model, *v1.ModelList] {
	return f.model
}

func (f *fakeKoldInterface) Root() generic.ControllerInterface[*v1.Root, *v1.RootList] {
	return f.root
}

func (f *fakeKoldInterface) Worker() generic.ControllerInterface[*v1.Worker, *v1.WorkerList] {
	return f.worker
}

func (f *fakeKoldInterface) Ingress() generic.ControllerInterface[*v1.Ingress, *v1.IngressList] {
	return f.ingress
}

func (f *fakeKoldInterface) Session() generic.ControllerInterface[*v1.Session, *v1.SessionList] {
	return f.session
}

type kvRecord struct {
	value    []byte
	revision uint64
	created  time.Time
	deleted  bool
}

// fakeMemoryKV is a simple in-memory KeyValue implementation for controller tests.
type fakeMemoryKV struct {
	bucket      string
	payload     map[string][]byte
	records     map[string]*kvRecord
	putCalls    []string
	deleteCalls []string
	putErr      error
	deleteErr   error
	keysErr     error
	getErrors   map[string]error
	mu          sync.Mutex
}

func (kv *fakeMemoryKV) ensure() {
	if kv.payload == nil {
		kv.payload = make(map[string][]byte)
	}
	if kv.records == nil {
		kv.records = make(map[string]*kvRecord)
	}
}

func (kv *fakeMemoryKV) recordFor(key string) (*kvRecord, bool) {
	record, ok := kv.records[key]
	if ok {
		return record, true
	}
	data, exists := kv.payload[key]
	if !exists || len(data) == 0 {
		return nil, false
	}
	record = &kvRecord{
		value:    append([]byte(nil), data...),
		revision: 1,
		created:  time.Now(),
	}
	kv.records[key] = record
	return record, true
}

func (kv *fakeMemoryKV) getOrCreateRecord(key string) *kvRecord {
	record, ok := kv.records[key]
	if !ok {
		record = &kvRecord{}
		kv.records[key] = record
	}
	return record
}

func (kv *fakeMemoryKV) Get(key string) (nats.KeyValueEntry, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure()
	if err := kv.lookupGetError(key); err != nil {
		return nil, err
	}
	record, ok := kv.records[key]
	if !ok {
		var exists bool
		record, exists = kv.recordFor(key)
		if !exists {
			return nil, nats.ErrKeyNotFound
		}
	}
	if record.deleted {
		return nil, nats.ErrKeyNotFound
	}
	return &fakeKeyValueEntry{
		bucket:   kv.bucket,
		key:      key,
		value:    append([]byte(nil), record.value...),
		revision: record.revision,
		created:  record.created,
		op:       nats.KeyValuePut,
	}, nil
}

func (kv *fakeMemoryKV) GetRevision(key string, revision uint64) (nats.KeyValueEntry, error) {
	entry, err := kv.Get(key)
	if err != nil {
		return nil, err
	}
	if entry.Revision() != revision {
		return nil, nats.ErrKeyNotFound
	}
	return entry, nil
}

func (kv *fakeMemoryKV) Put(key string, value []byte) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure()
	if kv.putErr != nil {
		return 0, kv.putErr
	}
	record := kv.getOrCreateRecord(key)
	record.value = append([]byte(nil), value...)
	record.revision++
	if record.revision == 0 {
		record.revision = 1
	}
	record.created = time.Now()
	record.deleted = false
	kv.payload[key] = append([]byte(nil), value...)
	kv.putCalls = append(kv.putCalls, key)
	return record.revision, nil
}

func (kv *fakeMemoryKV) PutString(key, value string) (uint64, error) {
	return kv.Put(key, []byte(value))
}

func (kv *fakeMemoryKV) Create(key string, value []byte) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure()
	if record, ok := kv.records[key]; ok && !record.deleted {
		return 0, nats.ErrKeyExists
	}
	if data, ok := kv.payload[key]; ok && len(data) > 0 {
		return 0, nats.ErrKeyExists
	}
	return kv.Put(key, value)
}

func (kv *fakeMemoryKV) Update(key string, value []byte, last uint64) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure()
	record, ok := kv.records[key]
	if !ok || record.deleted {
		return 0, nats.ErrKeyNotFound
	}
	if record.revision != last {
		return 0, nats.ErrKeyExists
	}
	return kv.Put(key, value)
}

func (kv *fakeMemoryKV) Delete(key string, _ ...nats.DeleteOpt) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure()
	record, ok := kv.records[key]
	if !ok {
		var exists bool
		record, exists = kv.recordFor(key)
		if !exists {
			return nats.ErrKeyNotFound
		}
	}
	if record.deleted {
		return nats.ErrKeyNotFound
	}
	kv.deleteCalls = append(kv.deleteCalls, key)
	if kv.deleteErr != nil {
		return kv.deleteErr
	}
	record.deleted = true
	record.revision++
	delete(kv.payload, key)
	return nil
}

func (kv *fakeMemoryKV) Purge(key string, opts ...nats.DeleteOpt) error {
	return kv.Delete(key, opts...)
}

func (kv *fakeMemoryKV) Watch(string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("watch not implemented")
}

func (kv *fakeMemoryKV) WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("watch not implemented")
}

func (kv *fakeMemoryKV) WatchFiltered([]string, ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return nil, errors.New("watch not implemented")
}

func (kv *fakeMemoryKV) Keys(...nats.WatchOpt) ([]string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.ensure()
	if kv.keysErr != nil {
		return nil, kv.keysErr
	}
	keys := make([]string, 0, len(kv.payload))
	for key := range kv.payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil, nats.ErrNoKeysFound
	}
	return keys, nil
}

func (kv *fakeMemoryKV) lookupGetError(key string) error {
	if kv.getErrors == nil {
		return nil
	}
	if err, ok := kv.getErrors[key]; ok {
		return err
	}
	return nil
}

func (kv *fakeMemoryKV) ListKeys(...nats.WatchOpt) (nats.KeyLister, error) {
	return nil, errors.New("list keys not implemented")
}

func (kv *fakeMemoryKV) History(string, ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	return nil, errors.New("history not implemented")
}

func (kv *fakeMemoryKV) Bucket() string {
	return kv.bucket
}

func (kv *fakeMemoryKV) PurgeDeletes(...nats.PurgeOpt) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, record := range kv.records {
		if record.deleted {
			delete(kv.records, key)
		}
	}
	return nil
}

func (kv *fakeMemoryKV) Status() (nats.KeyValueStatus, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	values := uint64(0)
	for key, record := range kv.records {
		if record.deleted {
			continue
		}
		if _, ok := kv.payload[key]; !ok {
			continue
		}
		values++
	}
	return &fakeKeyValueStatus{bucket: kv.bucket, values: values}, nil
}

// fakeKeyValueEntry satisfies nats.KeyValueEntry for tests.
type fakeKeyValueEntry struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	created  time.Time
	op       nats.KeyValueOp
}

func (e *fakeKeyValueEntry) Bucket() string             { return e.bucket }
func (e *fakeKeyValueEntry) Key() string                { return e.key }
func (e *fakeKeyValueEntry) Value() []byte              { return append([]byte(nil), e.value...) }
func (e *fakeKeyValueEntry) Revision() uint64           { return e.revision }
func (e *fakeKeyValueEntry) Created() time.Time         { return e.created }
func (e *fakeKeyValueEntry) Delta() uint64              { return 0 }
func (e *fakeKeyValueEntry) Operation() nats.KeyValueOp { return e.op }

// fakeKeyValueStatus implements nats.KeyValueStatus with minimal metadata.
type fakeKeyValueStatus struct {
	bucket string
	values uint64
}

func (s *fakeKeyValueStatus) Bucket() string       { return s.bucket }
func (s *fakeKeyValueStatus) Values() uint64       { return s.values }
func (s *fakeKeyValueStatus) History() int64       { return 1 }
func (s *fakeKeyValueStatus) TTL() time.Duration   { return 0 }
func (s *fakeKeyValueStatus) BackingStore() string { return "memory" }
func (s *fakeKeyValueStatus) Bytes() uint64        { return 0 }
func (s *fakeKeyValueStatus) IsCompressed() bool   { return false }

var _ nats.KeyValue = (*fakeMemoryKV)(nil)
