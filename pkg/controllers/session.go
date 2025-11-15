package controllers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

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

	mu               sync.RWMutex
	resourceSessions map[string]string

	// test hooks
	ensureTopologyFn     func(*v1.Session) error
	ensureStatusFn       func(*v1.Session) (*v1.Session, error)
	lookupRootEndpointFn func(string, string) string
	checkHealthFn        func(string) bool
	createDllamaFn       func(*v1.Session) error
	deleteDllamaFn       func(*v1.Session, *v1.Dllama) error
	ensureDispatcherFn   func(*v1.Session) error
}

const (
	resourceDllama = "dllama"
	resourceRoot   = "root"
	resourceWorker = "worker"
)

var (
	newControllerRef = metav1.NewControllerRef
)

func registerSessionController(ctx context.Context, m *Manager) error {
	handler := &sessionHandler{
		ctx:              ctx,
		apply:            m.Apply(ctx),
		sessions:         m.Kold.Session(),
		dllamas:          m.Kold.Dllama(),
		roots:            m.Kold.Root(),
		workers:          m.Kold.Worker(),
		httpClient:       &http.Client{Timeout: 3 * time.Second},
		log:              logrus.StandardLogger().WithField("component", "session-controller"),
		resourceSessions: map[string]string{},
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
	ns, name := splitNamespaceName(key)
	if dllama == nil {
		session := h.popResourceSession(resourceDllama, ns, name)
		if session == "" {
			session = guessSessionFromDllamaName(name)
		}
		if session != "" && ns != "" {
			h.sessions.Enqueue(ns, session)
		}
		return nil, nil
	}
	if session := labelValue(dllama.Labels, labelSessionName); session != "" {
		h.trackResourceSession(resourceDllama, dllama.Namespace, dllama.Name, session)
		h.sessions.Enqueue(dllama.Namespace, session)
	}
	return dllama, nil
}

func (h *sessionHandler) onRelatedRoot(key string, root *v1.Root) (*v1.Root, error) {
	ns, name := splitNamespaceName(key)
	if root == nil {
		session := h.popResourceSession(resourceRoot, ns, name)
		if session == "" {
			session = guessSessionFromRootName(name)
		}
		if session != "" && ns != "" {
			h.sessions.Enqueue(ns, session)
		}
		return nil, nil
	}
	if session := labelValue(root.Labels, labelSessionName); session != "" {
		h.trackResourceSession(resourceRoot, root.Namespace, root.Name, session)
		h.sessions.Enqueue(root.Namespace, session)
	}
	return root, nil
}

func (h *sessionHandler) onRelatedWorker(key string, worker *v1.Worker) (*v1.Worker, error) {
	ns, name := splitNamespaceName(key)
	if worker == nil {
		session := h.popResourceSession(resourceWorker, ns, name)
		if session == "" {
			session = guessSessionFromWorkerName(name)
		}
		if session != "" && ns != "" {
			h.sessions.Enqueue(ns, session)
		}
		return nil, nil
	}
	if session := labelValue(worker.Labels, labelSessionName); session != "" {
		h.trackResourceSession(resourceWorker, worker.Namespace, worker.Name, session)
		h.sessions.Enqueue(worker.Namespace, session)
	}
	return worker, nil
}

func (h *sessionHandler) ensureTopology(sess *v1.Session) error {
	if hook := h.ensureTopologyFn; hook != nil {
		return hook(sess)
	}

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
		return h.ensureDispatcher(sess)
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
		return h.ensureDispatcher(sess)
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
			return h.ensureDispatcher(sess)
		}
	}

	return h.ensureDispatcher(sess)
}

func (h *sessionHandler) ensureStatus(sess *v1.Session) (*v1.Session, error) {
	if hook := h.ensureStatusFn; hook != nil {
		return hook(sess)
	}

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
	if hook := h.lookupRootEndpointFn; hook != nil {
		return hook(namespace, dllamaName)
	}

	rootName := fmt.Sprintf("%s-root", dllamaName)
	root, err := h.roots.Cache().Get(namespace, rootName)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(root.Status.Endpoint)
}

func (h *sessionHandler) checkHealth(endpoint string) bool {
	if hook := h.checkHealthFn; hook != nil {
		return hook(endpoint)
	}

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
	desired          int32
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
		if sc.DesiredDllamas > 0 {
			params.desired = sc.DesiredDllamas
		}
	}

	if params.max > 0 && params.max < params.min {
		params.max = params.min
	}

	return params
}

func (p sessionScalingParams) shouldScaleUp(state sessionPoolState) bool {
	if p.max > 0 && int32(state.total()) >= p.max {
		return false
	}
	if p.desired > 0 && int32(state.total()) < p.desired {
		return true
	}
	if p.scaleUpThreshold <= 0 {
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
	if p.desired > 0 && int32(state.total()) <= p.desired {
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

func (h *sessionHandler) trackResourceSession(resource, namespace, name, session string) {
	if session == "" || name == "" {
		return
	}
	key := resourceSessionKey(resource, namespace, name)
	h.mu.Lock()
	h.resourceSessions[key] = session
	h.mu.Unlock()
}

func (h *sessionHandler) popResourceSession(resource, namespace, name string) string {
	key := resourceSessionKey(resource, namespace, name)
	h.mu.Lock()
	session := h.resourceSessions[key]
	if key != "" {
		delete(h.resourceSessions, key)
	}
	h.mu.Unlock()
	return session
}

func resourceSessionKey(resource, namespace, name string) string {
	if resource == "" || name == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", resource, namespace, name)
}

func splitNamespaceName(key string) (string, string) {
	if key == "" {
		return "", ""
	}
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return parts[0], parts[1]
}

func guessSessionFromDllamaName(name string) string {
	if name == "" {
		return ""
	}
	if idx := strings.Index(name, "-dllama"); idx > 0 {
		return name[:idx]
	}
	return ""
}

func guessSessionFromRootName(name string) string {
	return guessSessionFromDllamaName(strings.TrimSuffix(name, "-root"))
}

func guessSessionFromWorkerName(name string) string {
	return guessSessionFromDllamaName(strings.TrimSuffix(name, "-workers"))
}

func (h *sessionHandler) enqueueSession(sess *v1.Session) {
	if h.sessions == nil || sess == nil {
		return
	}
	h.sessions.Enqueue(sess.Namespace, sess.Name)
}

func (h *sessionHandler) createDllamaForSession(sess *v1.Session) error {
	if hook := h.createDllamaFn; hook != nil {
		return hook(sess)
	}
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
		h.enqueueSession(sess)
		return err
	}

	if created.Labels == nil || created.Labels[labelDllamaName] != sanitizeLabelValue(created.Name) {
		copy := created.DeepCopy()
		if copy.Labels == nil {
			copy.Labels = map[string]string{}
		}
		copy.Labels[labelDllamaName] = sanitizeLabelValue(created.Name)
		if _, err := h.dllamas.Update(copy); err != nil && !apierrors.IsNotFound(err) {
			h.enqueueSession(sess)
			return err
		}
	}

	return nil
}

func (h *sessionHandler) deleteDllama(sess *v1.Session, dllama *v1.Dllama) error {
	if hook := h.deleteDllamaFn; hook != nil {
		return hook(sess, dllama)
	}
	if dllama == nil {
		return nil
	}
	if err := h.dllamas.Delete(dllama.Namespace, dllama.Name, &metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (h *sessionHandler) ensureDispatcher(sess *v1.Session) error {
	if hook := h.ensureDispatcherFn; hook != nil {
		return hook(sess)
	}
	queue := sess.Spec.Queue
	if queue == nil {
		return nil
	}
	backlogSubject := strings.TrimSpace(queue.BacklogSubject)
	assignmentsBucket := strings.TrimSpace(queue.AssignmentsBucket)
	dllamaPrefix := strings.TrimSpace(queue.DllamaSubjectPrefix)
	if backlogSubject == "" || assignmentsBucket == "" || dllamaPrefix == "" {
		return nil
	}
	dllamaPrefix = ensureTrailingDot(dllamaPrefix)
	statePrefix := dllamaPrefix
	if stream := strings.TrimSpace(queue.StateStream); stream != "" {
		if strings.Contains(stream, ".") {
			statePrefix = ensureTrailingDot(stream)
		}
	}

	if sess.Spec.NATS == nil || strings.TrimSpace(sess.Spec.NATS.URL) == "" {
		h.log.WithField("session", sess.Name).Warn("session missing NATS config for dispatcher")
		return nil
	}

	image := strings.TrimSpace(sess.Spec.DispatcherImage)
	if image == "" {
		image = strings.TrimSpace(sess.Spec.RootImage)
	}
	if image == "" {
		return fmt.Errorf("dispatcher image missing for session %s", sess.Name)
	}

	labels := map[string]string{
		labelSessionName:      sanitizeLabelValue(sess.Name),
		labelConversationHash: sanitizeLabelValue(sess.Spec.Hash),
		labelComponent:        componentDispatcher,
	}

	ackWait := 2 * time.Minute
	if queue.AckTimeout != nil && queue.AckTimeout.Duration > 0 {
		ackWait = queue.AckTimeout.Duration
	}

	queueGroup := fmt.Sprintf("dispatcher-%s", sanitizeIdentifier(sess.Name))

	deployment := desiredDispatcherDeployment(sess, labels, queueGroup, image, backlogSubject, assignmentsBucket, dllamaPrefix, statePrefix, ackWait)

	apply := h.apply.WithOwner(sess).
		WithSetOwnerReference(true, false).
		WithDefaultNamespace(sess.Namespace).
		WithSetID(fmt.Sprintf("session-%s-dispatcher", sess.Name))

	err := apply.ApplyObjects(deployment)
	if err != nil {
		h.enqueueSession(sess)
	}
	return err
}

func desiredDispatcherDeployment(sess *v1.Session, labels map[string]string, queueGroup, image, backlogSubject, assignmentsBucket, dllamaPrefix, statePrefix string, ackWait time.Duration) *appsv1.Deployment {
	selector := map[string]string{}
	for k, v := range labels {
		selector[k] = v
	}

	args := dispatcherArgs(sess, backlogSubject, assignmentsBucket, dllamaPrefix, statePrefix, queueGroup, ackWait)

	container := corev1.Container{
		Name:            "dispatcher",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/koldun"},
		Args:            args,
	}
	if port := dispatcherMetricsPort(sess.Spec.DispatcherMetricsListen); port > 0 {
		container.Ports = []corev1.ContainerPort{{
			Name:          "metrics",
			ContainerPort: port,
			Protocol:      corev1.ProtocolTCP,
		}}
	}

	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-dispatcher", sess.Name),
			Namespace: sess.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(1),
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: pointer.Int64(0),
					Containers:                    []corev1.Container{container},
				},
			},
		},
	}

	return deployment
}

func dispatcherArgs(sess *v1.Session, backlogSubject, assignmentsBucket, dllamaPrefix, statePrefix, queueGroup string, ackWait time.Duration) []string {
	args := []string{
		"dispatcher",
		fmt.Sprintf("--dispatcher-hash=%s", strings.TrimSpace(sess.Spec.Hash)),
		fmt.Sprintf("--dispatcher-nats-url=%s", strings.TrimSpace(sess.Spec.NATS.URL)),
		fmt.Sprintf("--dispatcher-backlog-subject=%s", backlogSubject),
		fmt.Sprintf("--dispatcher-assignments-bucket=%s", assignmentsBucket),
		fmt.Sprintf("--dispatcher-dllama-prefix=%s", dllamaPrefix),
		fmt.Sprintf("--dispatcher-state-prefix=%s", statePrefix),
		fmt.Sprintf("--dispatcher-queue-group=%s", queueGroup),
		fmt.Sprintf("--dispatcher-ack-wait=%s", ackWait.String()),
	}

	if metricsAddr := strings.TrimSpace(sess.Spec.DispatcherMetricsListen); metricsAddr != "" {
		args = append(args, fmt.Sprintf("--dispatcher-metrics-listen=%s", metricsAddr))
	}

	return args
}

func dispatcherMetricsPort(listenAddr string) int32 {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		return 0
	}
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		lastColon := strings.LastIndex(addr, ":")
		if lastColon >= 0 {
			portStr = addr[lastColon+1:]
		} else {
			portStr = addr
		}
	}
	port, err := strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil || port <= 0 || port > 65535 {
		return 0
	}
	return int32(port)
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
	if sess == nil {
		return false
	}
	ref := newControllerRef(sess, v1.SchemeGroupVersion.WithKind("Session"))
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

func ensureTrailingDot(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	if strings.HasSuffix(prefix, ".") {
		return prefix
	}
	return prefix + "."
}

func sanitizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

func replicaPowerOrDefault(power int32) int32 {
	if power <= 0 {
		return 1
	}
	return power
}
