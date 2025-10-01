package controllers

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	"github.com/rancher/wrangler/v3/pkg/apply"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type sessionHandler struct {
	ctx      context.Context
	apply    apply.Apply
	sessions generic.ControllerInterface[*v1.Session, *v1.SessionList]
	dllamas  generic.ControllerInterface[*v1.Dllama, *v1.DllamaList]
	roots    generic.ControllerInterface[*v1.Root, *v1.RootList]
	workers  generic.ControllerInterface[*v1.Worker, *v1.WorkerList]

	httpClient *http.Client
	log        *logrus.Entry
}

func registerSessionController(ctx context.Context, m *Manager) error {
	handler := &sessionHandler{
		ctx:        ctx,
		apply:      m.Apply(ctx),
		sessions:   m.Kold.Session(),
		dllamas:    m.Kold.Dllama(),
		roots:      m.Kold.Root(),
		workers:    m.Kold.Worker(),
		httpClient: &http.Client{Timeout: 3 * time.Second},
		log:        logrus.StandardLogger().WithField("component", "session-controller"),
	}

	handler.sessions.OnChange(ctx, "koldun-session-controller", handler.onChange)
	handler.sessions.OnRemove(ctx, "koldun-session-controller", handler.onRemove)

	handler.dllamas.OnChange(ctx, "koldun-session-dllama-watch", handler.onRelatedDllama)
	handler.roots.OnChange(ctx, "koldun-session-root-watch", handler.onRelatedRoot)
	handler.workers.OnChange(ctx, "koldun-session-worker-watch", handler.onRelatedWorker)
	return nil
}

func (h *sessionHandler) onChange(key string, sess *v1.Session) (*v1.Session, error) {
	if sess == nil {
		return nil, nil
	}
	if sess.DeletionTimestamp != nil {
		return sess, nil
	}

	if err := h.ensureTopology(sess); err != nil {
		return sess, err
	}

	return h.ensureStatus(sess)
}

func (h *sessionHandler) onRemove(key string, sess *v1.Session) (*v1.Session, error) {
	return sess, nil
}

func (h *sessionHandler) onRelatedDllama(key string, dllama *v1.Dllama) (*v1.Dllama, error) {
	if dllama == nil {
		return nil, nil
	}
	if session := labelValue(dllama.Labels, labelSessionName); session != "" {
		h.sessions.Enqueue(dllama.Namespace, session)
	}
	return dllama, nil
}

func (h *sessionHandler) onRelatedRoot(key string, root *v1.Root) (*v1.Root, error) {
	if root == nil {
		return nil, nil
	}
	if session := labelValue(root.Labels, labelSessionName); session != "" {
		h.sessions.Enqueue(root.Namespace, session)
	}
	return root, nil
}

func (h *sessionHandler) onRelatedWorker(key string, worker *v1.Worker) (*v1.Worker, error) {
	if worker == nil {
		return nil, nil
	}
	if session := labelValue(worker.Labels, labelSessionName); session != "" {
		h.sessions.Enqueue(worker.Namespace, session)
	}
	return worker, nil
}

func (h *sessionHandler) ensureTopology(sess *v1.Session) error {
	selector := labels.SelectorFromSet(map[string]string{labelSessionName: sess.Name})
	dllamas, err := h.dllamas.Cache().List(sess.Namespace, selector)
	if err != nil {
		return err
	}

	for _, dllama := range dllamas {
		if err := h.reconcileDllama(sess, dllama); err != nil {
			return err
		}
	}

	params := scalingParamsFromSession(sess)
	state := computeSessionPoolState(sess, dllamas)

	if int32(state.total()) < params.min {
		if err := h.createDllamaForSession(sess); err != nil {
			return err
		}
		return nil
	}

	if params.shouldScaleUp(state) {
		h.log.WithFields(logrus.Fields{
			"session": sess.Name,
			"hash":    strings.TrimSpace(sess.Spec.Hash),
			"backlog": state.backlog,
			"ready":   state.readyCount(),
			"total":   state.total(),
		}).Info("scaling session up")
		if err := h.createDllamaForSession(sess); err != nil {
			return err
		}
		return nil
	}

	if params.shouldScaleDown(state) {
		idleSeconds := 0.0
		if !state.lastActivity.IsZero() {
			idleSeconds = time.Since(state.lastActivity).Seconds()
		}
		if candidate := chooseScaleDownCandidate(state); candidate != nil {
			h.log.WithFields(logrus.Fields{
				"session":     sess.Name,
				"hash":        strings.TrimSpace(sess.Spec.Hash),
				"dllama":      candidate.Name,
				"idleSeconds": idleSeconds,
				"total":       state.total(),
				"minDllamas":  params.min,
			}).Info("scaling session down")
			if err := h.deleteDllama(sess, candidate); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *sessionHandler) ensureStatus(sess *v1.Session) (*v1.Session, error) {
	updated := sess.DeepCopy()
	if updated.Status.Conditions == nil {
		updated.Status.Conditions = []metav1.Condition{}
	}

	selector := labels.SelectorFromSet(map[string]string{labelSessionName: sess.Name})
	dllamas, err := h.dllamas.Cache().List(sess.Namespace, selector)
	if err != nil {
		return sess, err
	}

	existingWorkers := make(map[string]v1.SessionWorker, len(sess.Status.Workers))
	for _, worker := range sess.Status.Workers {
		existingWorkers[worker.Name] = worker
	}

	var (
		readySets    int32
		busySets     int32
		readyWorkers int32
		workers      []v1.SessionWorker
	)

	for _, dllama := range dllamas {
		workerInfo := existingWorkers[dllama.Name]
		workerInfo.Name = dllama.Name

		ready := isConditionTrue(dllama.Status.Conditions, conditionReady) && dllama.Status.ReadyRoot
		if ready {
			readySets++
			workerInfo.Ready = true
		} else {
			workerInfo.Ready = false
		}

		phase := "Pending"
		if dllama.DeletionTimestamp != nil {
			phase = "Terminating"
		} else if ready {
			if workerInfo.ActiveMessages > 0 {
				busySets++
				phase = "Busy"
			} else {
				phase = "Ready"
			}
		}
		workerInfo.Phase = phase

		readyWorkers += dllama.Status.ReadyWorkers

		if endpoint := h.lookupRootEndpoint(dllama.Namespace, dllama.Name); endpoint != "" {
			workerInfo.Endpoint = endpoint
			workerInfo.Healthy = h.checkHealth(endpoint)
		} else {
			workerInfo.Endpoint = ""
			workerInfo.Healthy = isConditionTrue(dllama.Status.Conditions, conditionReady)
		}

		workers = append(workers, workerInfo)
	}

	sort.Slice(workers, func(i, j int) bool { return workers[i].Name < workers[j].Name })

	available := readySets - busySets
	if available < 0 {
		available = 0
	}

	updated.Status.ObservedGeneration = updated.Generation
	updated.Status.ReadyWorkers = readyWorkers
	updated.Status.BusyWorkers = busySets
	updated.Status.AvailableWorkers = available
	updated.Status.Workers = workers
	updated.Status.ActiveRequests = sess.Status.ActiveRequests
	updated.Status.InFlight = sess.Status.InFlight
	updated.Status.Backlog = sess.Status.Backlog
	updated.Status.LastActivity = sess.Status.LastActivity

	readyCondition := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "WorkersNotReady",
		Message: fmt.Sprintf("Ready worker sets: %d", readySets),
	}
	if readySets > 0 {
		readyCondition.Status = metav1.ConditionTrue
		if busySets > 0 {
			readyCondition.Reason = "WorkersBusy"
			readyCondition.Message = fmt.Sprintf("Ready worker sets: %d (busy: %d)", readySets, busySets)
		} else {
			readyCondition.Reason = "WorkersReady"
			readyCondition.Message = fmt.Sprintf("Ready worker sets: %d", readySets)
		}
	}

	changed := setCondition(&updated.Status.Conditions, readyCondition)
	if updated.Status.ObservedGeneration != sess.Status.ObservedGeneration {
		changed = true
	}
	if updated.Status.ReadyWorkers != sess.Status.ReadyWorkers ||
		updated.Status.BusyWorkers != sess.Status.BusyWorkers ||
		updated.Status.AvailableWorkers != sess.Status.AvailableWorkers ||
		!equality.Semantic.DeepEqual(sess.Status.Workers, workers) {
		changed = true
	}

	if !changed {
		return sess, nil
	}

	return h.sessions.UpdateStatus(updated)
}

func (h *sessionHandler) lookupRootEndpoint(namespace, dllamaName string) string {
	rootName := fmt.Sprintf("%s-root", dllamaName)
	root, err := h.roots.Cache().Get(namespace, rootName)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(root.Status.Endpoint)
}

func (h *sessionHandler) checkHealth(endpoint string) bool {
	if endpoint == "" {
		return false
	}

	url := fmt.Sprintf("http://%s/v1/models", endpoint)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

// sessionScalingParams captures the auto-scaling knobs for a session.
type sessionScalingParams struct {
	min              int32
	max              int32
	scaleUpThreshold int64
	scaleDownIdle    time.Duration
}

func scalingParamsFromSession(sess *v1.Session) sessionScalingParams {
	params := sessionScalingParams{min: 1}

	if sess.Spec.MinIdle > 0 {
		params.min = sess.Spec.MinIdle
	}
	if sess.Spec.MaxWorkers > 0 {
		params.max = sess.Spec.MaxWorkers
	}
	if sess.Spec.Scaling != nil {
		sc := sess.Spec.Scaling
		if sc.MinDllamas > 0 {
			params.min = sc.MinDllamas
		}
		if sc.MaxDllamas > 0 {
			params.max = sc.MaxDllamas
		}
		if sc.ScaleUpBacklog > 0 {
			params.scaleUpThreshold = int64(sc.ScaleUpBacklog)
		}
		if sc.ScaleDownIdleSeconds > 0 {
			params.scaleDownIdle = time.Duration(sc.ScaleDownIdleSeconds) * time.Second
		}
	}

	if params.min <= 0 {
		params.min = 1
	}
	if params.max > 0 && params.max < params.min {
		params.max = params.min
	}

	return params
}

func (p sessionScalingParams) shouldScaleUp(state sessionPoolState) bool {
	if p.scaleUpThreshold <= 0 {
		return false
	}
	if p.max > 0 && int32(state.total()) >= p.max {
		return false
	}
	if state.backlog < p.scaleUpThreshold {
		return false
	}
	if state.idleCount() > 0 {
		return false
	}
	return true
}

func (p sessionScalingParams) shouldScaleDown(state sessionPoolState) bool {
	if p.scaleDownIdle <= 0 {
		return false
	}
	if int32(state.total()) <= p.min {
		return false
	}
	if state.idleCount() == 0 {
		return false
	}
	if state.backlog > 0 {
		return false
	}
	if state.lastActivity.IsZero() {
		return false
	}
	if time.Since(state.lastActivity) < p.scaleDownIdle {
		return false
	}
	return true
}

// sessionPoolState summarises the current dllama pool for scaling decisions.
type sessionPoolState struct {
	dllamas       []*v1.Dllama
	ready         []*v1.Dllama
	idleReady     []*v1.Dllama
	workerMetrics map[string]v1.SessionWorker
	backlog       int64
	lastActivity  time.Time
}

func (s sessionPoolState) total() int { return len(s.dllamas) }

func (s sessionPoolState) readyCount() int { return len(s.ready) }

func (s sessionPoolState) idleCount() int { return len(s.idleReady) }

func (s sessionPoolState) busyCount() int { return len(s.ready) - len(s.idleReady) }

func computeSessionPoolState(sess *v1.Session, dllamas []*v1.Dllama) sessionPoolState {
	workerMetrics := make(map[string]v1.SessionWorker, len(sess.Status.Workers))
	for _, worker := range sess.Status.Workers {
		workerMetrics[worker.Name] = worker
	}

	state := sessionPoolState{
		dllamas:       dllamas,
		workerMetrics: workerMetrics,
		backlog:       sess.Status.Backlog,
	}
	if sess.Status.LastActivity != nil {
		state.lastActivity = sess.Status.LastActivity.Time
	}

	for _, dllama := range dllamas {
		if isConditionTrue(dllama.Status.Conditions, conditionReady) && dllama.Status.ReadyRoot {
			state.ready = append(state.ready, dllama)
			if workerMetrics[dllama.Name].ActiveMessages <= 0 {
				state.idleReady = append(state.idleReady, dllama)
			}
		}
	}

	return state
}

func chooseScaleDownCandidate(state sessionPoolState) *v1.Dllama {
	if len(state.idleReady) == 0 {
		return nil
	}

	sorted := append([]*v1.Dllama(nil), state.idleReady...)
	sort.Slice(sorted, func(i, j int) bool {
		ei := pointerTime(state.workerMetrics[sorted[i].Name].LastHeartbeat)
		ej := pointerTime(state.workerMetrics[sorted[j].Name].LastHeartbeat)
		if ei.IsZero() && ej.IsZero() {
			return sorted[i].CreationTimestamp.Time.Before(sorted[j].CreationTimestamp.Time)
		}
		if ei.IsZero() {
			return true
		}
		if ej.IsZero() {
			return false
		}
		if ei.Equal(ej) {
			return sorted[i].CreationTimestamp.Time.Before(sorted[j].CreationTimestamp.Time)
		}
		return ei.Before(ej)
	})

	return sorted[0]
}

func pointerTime(t *metav1.Time) time.Time {
	if t == nil || t.IsZero() {
		return time.Time{}
	}
	return t.Time
}

func (h *sessionHandler) createDllamaForSession(sess *v1.Session) error {
	spec := desiredDllamaSpecForSession(sess)
	hash := strings.TrimSpace(sess.Spec.Hash)
	hashLabel := sanitizeLabelValue(hash)

	dllama := &v1.Dllama{
		TypeMeta: metav1.TypeMeta{APIVersion: v1.SchemeGroupVersion.String(), Kind: "Dllama"},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-dllama-", sess.Name),
			Namespace:    sess.Namespace,
			Labels: map[string]string{
				labelSessionName:      sanitizeLabelValue(sess.Name),
				labelConversationHash: hashLabel,
				labelModelName:        sanitizeLabelValue(sess.Spec.ModelRef.Name),
			},
			Annotations: map[string]string{
				labelConversationHash: hash,
			},
		},
		Spec: spec,
	}

	ensureOwnerReference(&dllama.ObjectMeta, sess)

	created, err := h.dllamas.Create(dllama)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	if created.Labels == nil || created.Labels[labelDllamaName] != sanitizeLabelValue(created.Name) {
		copy := created.DeepCopy()
		if copy.Labels == nil {
			copy.Labels = map[string]string{}
		}
		copy.Labels[labelDllamaName] = sanitizeLabelValue(created.Name)
		if _, err := h.dllamas.Update(copy); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

func (h *sessionHandler) deleteDllama(sess *v1.Session, dllama *v1.Dllama) error {
	if dllama == nil {
		return nil
	}
	if err := h.dllamas.Delete(dllama.Namespace, dllama.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (h *sessionHandler) reconcileDllama(sess *v1.Session, dllama *v1.Dllama) error {
	updated := dllama.DeepCopy()
	desiredSpec := desiredDllamaSpecForSession(sess)

	changed := false
	if !equality.Semantic.DeepEqual(updated.Spec, desiredSpec) {
		updated.Spec = desiredSpec
		changed = true
	}

	hash := strings.TrimSpace(sess.Spec.Hash)
	labels := map[string]string{
		labelSessionName:      sanitizeLabelValue(sess.Name),
		labelConversationHash: sanitizeLabelValue(hash),
		labelModelName:        sanitizeLabelValue(sess.Spec.ModelRef.Name),
		labelDllamaName:       sanitizeLabelValue(updated.Name),
	}
	if ensureLabels(&updated.ObjectMeta, labels) {
		changed = true
	}

	annotations := map[string]string{labelConversationHash: hash}
	if sess.Spec.Queue != nil {
		prefix := strings.TrimSpace(sess.Spec.Queue.DllamaSubjectPrefix)
		if prefix != "" {
			if !strings.HasSuffix(prefix, ".") {
				prefix += "."
			}
			annotations[annotationSessionQueuePrefix] = prefix
		}
		if bucket := strings.TrimSpace(sess.Spec.Queue.AssignmentsBucket); bucket != "" {
			annotations[annotationSessionAssignmentsBucket] = bucket
		}
		if backlog := strings.TrimSpace(sess.Spec.Queue.BacklogSubject); backlog != "" {
			annotations[annotationSessionBacklogSubject] = backlog
		}
		if stream := strings.TrimSpace(sess.Spec.Queue.StateStream); stream != "" {
			annotations[annotationSessionStateStream] = stream
		}
	}
	if ensureAnnotations(&updated.ObjectMeta, annotations) {
		changed = true
	}

	if ensureOwnerReference(&updated.ObjectMeta, sess) {
		changed = true
	}

	if !changed {
		return nil
	}

	if _, err := h.dllamas.Update(updated); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func desiredDllamaSpecForSession(sess *v1.Session) v1.DllamaSpec {
	spec := v1.DllamaSpec{
		ModelRef:     sess.Spec.ModelRef,
		ReplicaPower: replicaPowerOrDefault(sess.Spec.ReplicaPower),
		RootImage:    sess.Spec.RootImage,
		WorkerImage:  sess.Spec.WorkerImage,
	}
	if sess.Spec.NATS != nil {
		cfg := &v1.DllamaNATSConfig{URL: sess.Spec.NATS.URL}
		if sess.Spec.NATS.CredentialsSecret != nil {
			secretCopy := *sess.Spec.NATS.CredentialsSecret
			cfg.CredentialsSecret = &secretCopy
		}
		spec.NATS = cfg
	} else {
		spec.NATS = nil
	}
	if strings.TrimSpace(spec.ModelRef.Namespace) == "" {
		spec.ModelRef.Namespace = ""
	}
	return spec
}

func ensureLabels(meta *metav1.ObjectMeta, values map[string]string) bool {
	if len(values) == 0 {
		return false
	}
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	changed := false
	for key, value := range values {
		if existing, ok := meta.Labels[key]; !ok || existing != value {
			meta.Labels[key] = value
			changed = true
		}
	}
	return changed
}

func ensureAnnotations(meta *metav1.ObjectMeta, values map[string]string) bool {
	if len(values) == 0 {
		return false
	}
	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}
	changed := false
	for key, value := range values {
		if existing, ok := meta.Annotations[key]; !ok || existing != value {
			meta.Annotations[key] = value
			changed = true
		}
	}
	return changed
}

func ensureOwnerReference(meta *metav1.ObjectMeta, sess *v1.Session) bool {
	ref := metav1.NewControllerRef(sess, v1.SchemeGroupVersion.WithKind("Session"))
	if ref == nil {
		return false
	}
	for i := range meta.OwnerReferences {
		existing := meta.OwnerReferences[i]
		if existing.UID == ref.UID {
			if !equality.Semantic.DeepEqual(existing, *ref) {
				meta.OwnerReferences[i] = *ref
				return true
			}
			return false
		}
	}
	meta.OwnerReferences = append(meta.OwnerReferences, *ref)
	return true
}

func replicaPowerOrDefault(power int32) int32 {
	if power <= 0 {
		return 1
	}
	return power
}
