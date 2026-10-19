package ui

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// portForwardTo reaches a Service or a workload on a local port, through
// one of its running pods (kubectl port-forward svc/x does the same).
func (m *Model) portForwardTo(row view.Row) tea.Cmd {
	o := row.Obj
	pods, ok := m.podsBehind(o)
	if !ok {
		m.setFlash("", fmt.Errorf("still loading pods; try again in a moment"))
		return nil
	}
	pod := pickRunningPod(pods)
	if pod == nil {
		m.setFlash("", fmt.Errorf("no pod of %s is running, so there is nothing to connect to", o.GetName()))
		return nil
	}
	if !containersRunning(pod) { // "Running" pods whose containers crash: nothing listens
		m.setFlash("", fmt.Errorf("no pod of %s has its containers running (pod %s keeps crashing or is starting), so there is nothing to connect to. p shows why it stopped", o.GetName(), pod.GetName()))
		return nil
	}
	ns := o.GetNamespace()
	what := "port of the pod"
	suggestion := ""
	if o.GetKind() == "Service" {
		what = "port of the service"
		ports, _, _ := unstructured.NestedSlice(o.Object, "spec", "ports")
		if len(ports) > 0 {
			p, _, _ := unstructured.NestedInt64(ports[0].(map[string]any), "port")
			suggestion = fmt.Sprintf("%d:%d", suggestLocalPort(int(p)), p)
		}
	} else if ps := view.ContainerPorts(pod); len(ps) > 0 {
		suggestion = fmt.Sprintf("%d:%d", suggestLocalPort(int(ps[0])), ps[0])
	}
	m.prompt = inputPrompt(fmt.Sprintf("Reach %s %s on a local port (through pod %s).\nType <local port>:<%s>, e.g. 8080:80",
		lowerFirst(o.GetKind()), o.GetName(), pod.GetName(), what), suggestion, func(v string) tea.Cmd {
		local, remote, _ := parsePorts(v)
		target := lowerFirst(o.GetKind()) + " " + o.GetName()
		client := m.client // this cluster's, even when a start runs after you switched
		var start func(through *unstructured.Unstructured) (*kube.Forward, error)
		start = func(through *unstructured.Unstructured) (*kube.Forward, error) {
			port := remote
			if o.GetKind() == "Service" {
				p, err := servicePortToPod(o, through, remote)
				if err != nil {
					return nil, err
				}
				port = p
			}
			f, err := m.forwards.Start(client, ns, through.GetName(), local, port)
			if err != nil {
				return nil, err
			}
			f.Target = target
			// Its connection ended (its pod went away, or the connection
			// dropped): through a ready pod of it, waiting up to followWait
			// for one (a single pod's replacement starting).
			f.Again = func() (*kube.Forward, error) {
				var last error
				for deadline := time.Now().Add(followWait); ; time.Sleep(time.Second) {
					if client.Closed() {
						return nil, kube.ErrLeftCluster
					}
					if f.Dropped.Load() {
						return nil, errDropped // you stopped it meanwhile
					}
					pods, _ := podsBehindOn(client, o)
					next := pickReadyPod(pods)
					if next == nil && time.Now().After(deadline) {
						next = pickRunningPod(pods) // none ready in time: one that runs
					}
					if next != nil {
						nf, err := start(next)
						if err == nil || errors.Is(err, kube.ErrLeftCluster) || strings.Contains(err.Error(), "unable to listen") {
							return nf, err // done, or its local port is taken now: trying again won't help
						}
						last = err
					}
					if time.Now().After(deadline) {
						if last != nil {
							return nil, last
						}
						return nil, fmt.Errorf("no pod of %s ran within %s", target, followWait)
					}
				}
			}
			return f, nil
		}
		return func() tea.Msg {
			f, err := start(pod)
			if err != nil && o.GetKind() != "Pod" && strings.Contains(err.Error(), "not found") {
				// The pod went away while you typed: through another one.
				pods, _ := podsBehindOn(client, o)
				var others []*unstructured.Unstructured
				for _, p := range pods {
					if p.GetName() != pod.GetName() {
						others = append(others, p)
					}
				}
				if next := pickRunningPod(others); next != nil {
					f, err = start(next)
				}
			}
			if err != nil && strings.Contains(err.Error(), "not found") {
				return resultMsg{err: fmt.Errorf("pod %s is gone (replaced a moment ago?). F on its deployment or service follows whichever pod is running", pod.GetName())}
			}
			if err != nil {
				return resultMsg{err: fmt.Errorf("port-forward: %w", err)}
			}
			return resultMsg{text: f.Short() + " · :pf to see or stop it"}
		}
	})
	m.prompt.validate = func(v string) error {
		local, _, err := parsePorts(v)
		if err == nil {
			err = m.portMoving(local)
		}
		return err
	}
	m.mode = modePrompt
	return nil
}

// podsBehind are the pods a Service selects, or a workload runs. ok is
// false while pods are still loading.
func (m *Model) podsBehind(o *unstructured.Unstructured) ([]*unstructured.Unstructured, bool) {
	return podsBehindOn(m.client, o)
}

// podsBehindOn is podsBehind on one cluster's client (a forward's own).
func podsBehindOn(client *kube.Client, o *unstructured.Unstructured) ([]*unstructured.Unstructured, bool) {
	if client == nil || client.Closed() {
		return nil, true
	}
	gvr, served := client.Resources.Served(kube.Pods)
	if !served {
		return nil, true
	}
	client.Cache.Watch(gvr)
	if !client.Cache.Synced(gvr) {
		return nil, false
	}
	var match func(*unstructured.Unstructured) bool
	if o.GetKind() == "Service" {
		sel, _, _ := unstructured.NestedStringMap(o.Object, "spec", "selector")
		if len(sel) == 0 {
			return nil, true
		}
		s := labels.SelectorFromSet(sel)
		match = func(p *unstructured.Unstructured) bool { return s.Matches(labels.Set(p.GetLabels())) }
	} else if res, ok := client.Resources.ByKind(groupOf(o), o.GetKind()); ok && view.For(res).Drill != nil {
		// The object's own kind, not the screen you're on: a forward started
		// from the apps list reconnects later from anywhere.
		d := view.For(res).Drill
		if dr := d(o); dr != nil {
			match = dr.Match
		}
	}
	if match == nil {
		return nil, true
	}
	var out []*unstructured.Unstructured
	for _, p := range client.Cache.List(gvr, o.GetNamespace()) {
		if match(p) {
			out = append(out, p)
		}
	}
	return out, true
}

// pickRunningPod prefers a ready pod, then any running one.
// containersRunning: at least one of a pod's containers runs now (not
// waiting to restart after a crash).
func containersRunning(pod *unstructured.Unstructured) bool {
	statuses, _, _ := unstructured.NestedSlice(pod.Object, "status", "containerStatuses")
	for _, c := range statuses {
		if cm, ok := c.(map[string]any); ok {
			if _, running, _ := unstructured.NestedMap(cm, "state", "running"); running {
				return true
			}
		}
	}
	return false
}

func pickRunningPod(pods []*unstructured.Unstructured) *unstructured.Unstructured {
	var running *unstructured.Unstructured
	for _, p := range pods {
		phase, _, _ := unstructured.NestedString(p.Object, "status", "phase")
		if phase != "Running" || p.GetDeletionTimestamp() != nil { // one being deleted is gone in a moment
			continue
		}
		conds, _, _ := unstructured.NestedSlice(p.Object, "status", "conditions")
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			if cm["type"] == "Ready" && cm["status"] == "True" {
				return p
			}
		}
		if running == nil {
			running = p
		}
	}
	return running
}

// servicePortToPod turns a Service port into the pod port it sends to
// (targetPort, which may name a container port).
func servicePortToPod(svc, pod *unstructured.Unstructured, port int) (int, error) {
	ports, _, _ := unstructured.NestedSlice(svc.Object, "spec", "ports")
	for _, p := range ports {
		pm, _ := p.(map[string]any)
		n, _, _ := unstructured.NestedInt64(pm, "port")
		if int(n) != port {
			continue
		}
		switch t := pm["targetPort"].(type) {
		case int64:
			return int(t), nil
		case float64:
			return int(t), nil
		case string:
			if v, err := strconv.Atoi(t); err == nil {
				return v, nil
			}
			containers, _, _ := unstructured.NestedSlice(pod.Object, "spec", "containers")
			for _, c := range containers {
				cps, _, _ := unstructured.NestedSlice(c.(map[string]any), "ports")
				for _, cp := range cps {
					cpm, _ := cp.(map[string]any)
					if cpm["name"] == t {
						v, _, _ := unstructured.NestedInt64(cpm, "containerPort")
						return int(v), nil
					}
				}
			}
			return 0, fmt.Errorf("the service sends port %d to a port named %q, which pod %s doesn't have", port, t, pod.GetName())
		}
		return port, nil // no targetPort: the same number
	}
	var have []string
	for _, p := range ports {
		n, _, _ := unstructured.NestedInt64(p.(map[string]any), "port")
		have = append(have, fmt.Sprint(n))
	}
	return 0, fmt.Errorf("service %s has no port %d (it has %s)", svc.GetName(), port, strings.Join(have, ", "))
}

// groupOf is an object's API group ("apps" for a Deployment).
func groupOf(o *unstructured.Unstructured) string {
	gv, _ := schema.ParseGroupVersion(o.GetAPIVersion())
	return gv.Group
}

// refused is a connection the pod turned away: nothing listens on that
// port. Other failures (a dropped network, a laptop that slept) say
// nothing about the pod.
func refused(failing string) bool { return strings.Contains(strings.ToLower(failing), "refused") }

// forwardStopWords say what to check when a forward to a pod still
// running stopped: a sentence for the message, and a note for the list of
// stopped forwards. The port only when the pod refused a connection.
func forwardStopWords(failing string, podPort int) (hint, note string) {
	if refused(failing) {
		return fmt.Sprintf("Is something listening on port %d in the pod?", podPort),
			fmt.Sprintf("is something listening on port %d?", podPort)
	}
	return "The connection to the cluster may have dropped (a network change, or the computer sleeping).",
		"the network may have dropped"
}

// followWait is how long a forward to a service or workload waits for a
// pod of it to run after its own went away.
const followWait = 30 * time.Second

// follows says a forward that ended starts again through a pod of what it
// reaches (a service, a deployment): not a forward to one pod, not when its
// pod is still up and refused a connection (nothing listens: it would
// again; a pod going away in a rollout often refuses first), and not one
// that keeps ending within moments of starting.
func follows(f *kube.Forward, podGone bool) bool {
	return f.Again != nil && (podGone || !refused(f.Failing())) && f.Moves < 3
}

// portMoving: a forward on that local port is connecting again through a
// pod of what it reaches; a new one there would fight it for the port.
func (m *Model) portMoving(port int) error {
	if f := m.moving[port]; f != nil {
		return fmt.Errorf("localhost:%d is connecting again through a pod of %s; ctrl-d on it in :pf stops it first", port, f.Target)
	}
	if m.stopping[port] != nil {
		return fmt.Errorf("localhost:%d is still stopping; try again in a moment", port)
	}
	return nil
}

// errDropped: a move you stopped (ctrl-d) before it found a pod.
var errDropped = errors.New("stopped")

// pickReadyPod is a running pod of them that is ready, not being deleted.
func pickReadyPod(pods []*unstructured.Unstructured) *unstructured.Unstructured {
	if p := pickRunningPod(pods); p != nil && podReady(p) {
		return p
	}
	return nil
}

// forwardPodGone says a forward's pod is gone or going: not listed, being
// deleted, or listed under its name but a new pod (a StatefulSet's).
func (m *Model) forwardPodGone(pods schema.GroupVersionResource, f *kube.Forward) bool {
	p, alive := m.client.Cache.Get(pods, f.Namespace, f.Pod)
	return podGoneFor(f, p, alive)
}

func podGoneFor(f *kube.Forward, p *unstructured.Unstructured, listed bool) bool {
	return !listed || p.GetDeletionTimestamp() != nil || f.PodUID != "" && p.GetUID() != f.PodUID
}
