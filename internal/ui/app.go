// Package ui is kubed's Bubble Tea interface. It reads cluster state through
// kube and enrich, and turns it into rows through view.
package ui

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/config"
	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/pricing"
	"github.com/s3cretagent/kubed/internal/view"
)

// Options are the command-line settings.
type Options struct {
	Kubeconfig     string
	Context        string
	Namespace      string
	Resource       string // view to open first, e.g. "pods"
	ReadOnly       bool
	Mouse          bool           // kubed handles clicks (selecting text then needs Option/Shift)
	ProdPattern    *regexp.Regexp // production contexts / clusters by name
	NonProdPattern *regexp.Regexp // clearly not production; checked first
	State          *config.State  // remembered between runs; nil = remember nothing
	// Discount is the percent off on-demand list prices for a cluster, by
	// any of its names (context, cluster name). nil = list prices.
	Discount func(names ...string) float64
}

type mode int

const (
	modeTable mode = iota
	modePager
	modePrompt
	modeCommand
	modeFilter
	modeShell  // in-panel shell has the keyboard
	modeSearch // typing a search term for the pager
	modeFind   // typing in the f box; results update live
)

// locKind separates real API resources from kubed's own lists.
type locKind int

const (
	locResource locKind = iota
	locContexts
	locForwards
	locResources  // ctrl-a picker
	locContainers // enter on a pod
	locHome       // gh dashboard
	locHelm       // gH releases
	locFind       // f results
	locWaste      // :waste right-sizing
)

// location is one entry of the navigation history (Esc goes back).
type location struct {
	kind   locKind
	res    kube.Resource
	drill  *view.Drill
	filter string
	cursor int
	picked string // the row the cursor was on (rowID): back finds it again if rows moved
	ns     string // the namespace it showed; esc back restores it

	query    string // locFind: what was searched
	fromFind bool   // opened from a search result: esc goes straight back
	sort     sortMode
	desc     bool
	sorted   bool   // sort and desc were saved on leaving: restore them
	via      bool   // opened only to run an app's action on its workload: esc goes straight back
	app      string // the app this list belongs to ("pods of app auth"): w stays on it
	jumped   bool   // reached by a jump (g…, :name): another jump replaces it

	podNS, podName string // locContainers
}

// Model is the whole application state.
type Model struct {
	opts          Options
	kc            *kube.Kubeconfig
	client        *kube.Client
	enr           *enrich.Enricher
	prices        *pricing.Store
	priceTbl      *pricing.Table
	priceStale    bool
	priceMsg      string
	rates         rates // what this AWS account really pays (rates.go)
	connectedAt   time.Time
	pickRestarted bool                 // the container picker puts restarted containers first
	pickShell     bool                 // the container picker offers only running containers
	secretErrs    map[string]secretErr // ExternalSecret errors by ns/name (secreterr.go)
	tlsSecrets    map[string]tlsSecret // whether ingresses' TLS secrets exist, by ns/name (tlssecret.go)
	argoChecks    map[string]argoCheck // why ArgoCD calls an app unhealthy (appcheck.go)
	crashChecks   map[string]string    // the error a crashed run ended with, by crashKey (appcheck.go)
	crashInFlight map[string]time.Time // crash logs being read, or read with nothing yet
	forwards      *kube.Forwards

	context      string
	namespace    string            // "" = all namespaces
	underHelp    *pager            // logs you opened help from; esc goes back to them
	foundAll     int               // how many things matched the last find (the list shows at most maxFound)
	chosenNS     string            // the namespace you picked (at start or :ns); lists you open with : or g show it
	prevNS       string            // the namespace before the last :ns, for :ns -
	prevContext  string            // the cluster before the last switch, for :ctx -
	nsByContext  map[string]string // the namespace you last chose on each cluster this session
	recentNS     []string          // most recent first; digits 1-9 in the header
	connecting   bool
	connectStart time.Time
	contextNote  string // why the cluster list opened by itself
	connErr      error

	loc     location
	history []location
	tbl     table

	mode   mode
	term   *terminal // in-panel shell, while modeShell
	pager  *pager
	prompt *prompt
	input  textinput.Model // command bar and filter
	sugs   []suggestion    // ":" completions for the current input
	sugSel int             // highlighted completion; -1 = none picked
	gKey   bool            // "g" pressed, waiting for the jump key
	env    environment     // production or not, and how we know
	asked  bool            // asked the user this session already

	cmdHistory []string // ":" commands this session, newest last
	histPos    int      // position while browsing history with ↑/↓

	home   enrich.Summary // cluster problems from the last refresh
	wastes []enrich.Waste // :waste from the last refresh
	apps   []*view.App    // home's apps from the last refresh

	helm        []kube.HelmRelease // gH list, refreshed every helmRefresh
	helmAt      time.Time
	helmErr     error
	helmNS      string // the namespace the Helm list was read for
	helmLoading bool

	flash    string
	flashErr bool
	flashTip bool
	// dropPager: the next text screen to arrive was asked for on a screen
	// you have since left.
	dropPager bool
	// loadWaiting is the action waiting for a list to load (whenLoaded),
	// 0 for none; leaving the screen drops it. loadSeq numbers them.
	loadWaiting, loadSeq int
	loadBusy             string         // its "loading …" line: another action replacing it took over
	loadOwnsBusy         bool           // the wait put its own line there (no other action was on its way)
	loadWhere            string         // the screen it was asked on (whereNow): it runs only there
	loadWhat             string         // what it waits for ("db's versions"), for saying it loaded too late
	loadPending          func() tea.Cmd // loaded while help was open: runs when help closes, if still there
	// keepPlace: the logs about to open are the same ones read again (t,
	// h, r), so your search and place carry over; not another run (l, p).
	keepPlace      bool
	keysQuietUntil time.Time // keys are ignored until then (a shell just closed)
	pendingSelect  string    // a row to pick once its list has loaded (openNamed)
	pendingGVR     schema.GroupVersionResource
	quitArmed      bool // q was pressed with ports open: the next key, if q, quits
	hadPrevNS      bool // a namespace was chosen before this one (:ns - goes back to it)
	// stoppedForwards: forwards that ended and couldn't go on, shown in :pf.
	owners          []string              // ownersLoading's last answer
	ownersAt        time.Time             // and when
	moving          map[int]*kube.Forward // ended, finding a pod to go on through, by local port
	stopping        map[int]*kube.Forward // moving ones you stopped, until their search ends
	endedHeld       []endedForward        // ended by themselves, waiting a moment to learn why (see holdEnded)
	stoppedForwards []stoppedForward
	// nearlyFullSince: when each node's memory first went over 90%, so the
	// Watch line doesn't blink for a node that hovers around it.
	nearlyFullSince map[string]time.Time
	// inputFresh: the filter box shows the old filter; the first letter
	// typed replaces it (arrows or backspace edit it instead).
	inputFresh bool // a note on what to do next, not a result: no ✓
	flashAt    time.Time
	// flashKeep: a command's failure isn't cleared by a key before this.
	flashKeep time.Time
	busy      string // shown while an API call runs, e.g. "describing…"
	stderr    <-chan string
	lastErr   string // last stderr line, shown with connection errors

	width, height int
}

// New builds the model. Connecting happens in Init so the UI shows up first.
func New(opts Options) (*Model, error) {
	view.ReadOnly = opts.ReadOnly // Fix lines offer no change keys
	kc := kube.LoadKubeconfig(opts.Kubeconfig)
	ctx := opts.Context
	if ctx == "" {
		cur, err := kc.CurrentContext()
		if err != nil {
			return nil, fmt.Errorf("kubeconfig: %w", err)
		}
		ctx = cur
	}
	if ctx == "" {
		return nil, fmt.Errorf("no kubeconfig context: pass --context")
	}
	if names, err := kc.Contexts(); err == nil && opts.Context != "" { // --context staging: a short or partial name
		if full, ok := matchContext(names, ctx); ok {
			ctx = full
		}
	}
	ns := opts.Namespace
	switch ns {
	case "all", "-A": // every namespace, as :ns all
		ns = ""
	case "":
		ns = kc.DefaultNamespace(ctx)
	}

	store := pricing.NewStore()
	tbl, stale, err := store.Load()
	if err != nil {
		tbl = nil // prices show as unknown; everything else works
	}

	recent := nonEmpty(ns)
	var history []string
	if opts.State != nil {
		recent = mergeRecent(ns, opts.State.RecentNamespaces[ctx])
		history = opts.State.History
	}

	return &Model{
		opts:       opts,
		cmdHistory: history,
		kc:         kc,
		prices:     store,
		priceTbl:   tbl,
		priceStale: stale,
		forwards:   &kube.Forwards{},
		context:    ctx,
		namespace:  ns,
		chosenNS:   ns,
		recentNS:   recent,
		connecting: true,
		input:      textinput.New(),
	}, nil
}

// --- messages ---

type connectedMsg struct {
	context string
	client  *kube.Client
	err     error
}

type tickMsg time.Time

type pricesMsg struct {
	table *pricing.Table
	err   error
}

// resultMsg reports the outcome of an action in the footer.
// whereNow names the screen you are on, for an action waiting for a list:
// the same list, namespace and mode, with nothing open over it.
func (m *Model) whereNow() string { return m.whereFor(m.mode, m.prompt != nil) }

func (m *Model) whereFor(md mode, prompt bool) string {
	return fmt.Sprintf("%d/%s/%s/%s/%d/%v", m.loc.kind, m.loc.res.GVR.Resource, m.loc.app, m.namespace, md, prompt)
}

// loadedMsg: a list an action waited for has loaded; run the action now.
type loadedMsg struct {
	id  int
	run func() tea.Cmd
}

// whenLoaded runs an action once a list it reads has loaded (at most
// 15s), instead of answering from a half-loaded one.
func (m *Model) whenLoaded(gvr schema.GroupVersionResource, what string, run func() tea.Cmd) tea.Cmd {
	c := m.client
	c.Cache.Watch(gvr)
	m.loadPending = nil // asked again: an older one held behind help is replaced
	if c.Cache.Synced(gvr) || c.Cache.Err(gvr) != nil {
		return run()
	}
	// The footer says so, unless another action's line is there (a
	// describe on its way): that one stays, and leaving still drops it.
	// (A second wait takes over the first one's line too.)
	m.loadOwnsBusy = m.busy == "" || (m.loadWaiting != 0 && m.loadOwnsBusy && m.busy == m.loadBusy)
	if m.loadOwnsBusy {
		m.busy = "loading " + what
	}
	m.loadBusy = m.busy
	m.loadPending = nil
	m.loadWhat = what
	m.loadWhere = m.whereNow()
	if m.mode == modePrompt && m.prompt != nil && m.prompt.kind == promptMenu {
		// Picked from the menu, which closes next: it runs on what's behind it.
		m.loadWhere = m.whereFor(m.modeBehindPrompt(), false)
	}
	m.loadSeq++
	id := m.loadSeq
	m.loadWaiting = id
	return func() tea.Msg {
		for deadline := time.Now().Add(15 * time.Second); !c.Cache.Synced(gvr) && c.Cache.Err(gvr) == nil && time.Now().Before(deadline); {
			time.Sleep(100 * time.Millisecond)
		}
		return loadedMsg{id, run}
	}
}

type resultMsg struct {
	text string
	err  error
	// stopped: a port-forward that ended and couldn't go on, kept in :pf.
	stopped *stoppedForward
	// moved: a forward that followed another pod (next, or nil when it
	// couldn't); moves counts quick restarts in a row.
	moved, next *kube.Forward
	moves       int
	// shellEnd: a shell just closed. Keys typed into it as it went
	// (the rest of "exit") mustn't land on kubed's own keys.
	shellEnd bool
}

// pagerMsg opens text in the pager.
type pagerMsg struct {
	title, content string
	err            error
	editRow        *view.Row      // YAML of something e can edit
	hang           int            // a table: wrap long lines, continuing at this column
	plain          string         // what saving and copying all give, when it differs from the screen (whole names)
	tree           bool           // a tree: wrap keeping its │ lines
	back           func() tea.Cmd // esc reopens what this was opened from (the versions menu)
	secret         bool           // secret values: never saved to a file or copied all at once
	csv            bool           // plain is CSV: saved as .csv
	plainFrom      int            // > 0: plain has one line per screen line from this one (after a header), so c copies the plain line
	diff           bool           // a diff: wrapped rows keep their line's - or +
}

// abandonWait: leaving a screen while something loads for it (a slow
// describe) means its text, when it comes, is no longer wanted there.
func (m *Model) abandonWait() {
	m.loadPending = nil
	if m.loadWaiting != 0 { // a list an action waited for: the action is dropped
		m.loadWaiting = 0
		if m.loadOwnsBusy && m.busy == m.loadBusy { // nothing else was on its way: no text screen to drop
			m.busy = ""
			return
		}
	}
	if m.busy != "" && !strings.HasPrefix(m.busy, "connecting") { // a shell isn't a text screen
		m.busy, m.dropPager = "", true
	}
}

// SetStderr routes lines written to the process's stderr (credential
// helpers) into the footer.
func (m *Model) SetStderr(lines <-chan string) { m.stderr = lines }

// stderrMsg is one line a credential helper wrote to stderr.
type stderrMsg string

func waitStderr(lines <-chan string) tea.Cmd {
	return func() tea.Msg {
		l, ok := <-lines
		if !ok {
			return nil
		}
		return stderrMsg(l)
	}
}

// Init connects to the cluster and refreshes prices when stale.
func (m *Model) Init() tea.Cmd {
	m.connectStart = time.Now()
	cmds := []tea.Cmd{m.connect(m.context), tick()}
	if m.stderr != nil {
		cmds = append(cmds, waitStderr(m.stderr))
	}
	return tea.Batch(cmds...)
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m *Model) connect(context string) tea.Cmd {
	return func() tea.Msg {
		c, err := m.kc.Connect(context)
		return connectedMsg{context: context, client: c, err: err}
	}
}

func (m *Model) refreshPrices() tea.Cmd {
	return func() tea.Msg {
		t, err := m.prices.Refresh(context.Background())
		return pricesMsg{table: t, err: err}
	}
}

// Update is Bubble Tea's event handler.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		if m.pager != nil && m.pager.title == "help" {
			m.relayoutHelp() // one column or two, as the new width takes
		} else if m.pager != nil {
			m.pager.resize(m.pagerWidth(), m.pagerHeight())
		}
		if m.term != nil {
			m.term.resize(m.pagerWidth(), m.pagerHeight())
		}
		if m.prompt != nil && m.prompt.maxItems > 0 { // an open menu fits the new size
			m.prompt.maxItems = m.menuRoom()
		}
		return m, nil

	case stderrMsg:
		m.lastErr = string(msg)
		// While the connection-problem screen shows, it already says this.
		if _, bad := m.problem(); !bad && !m.connecting {
			m.setFlash("", fmt.Errorf("%s", msg))
		}
		return m, waitStderr(m.stderr)

	case tickMsg:
		var again []tea.Cmd
		// A forward's pod that is gone (or going) won't answer again: end it
		// now, rather than at the next connection, which would fail.
		var pods schema.GroupVersionResource
		served := false
		if m.client != nil { // not connected (the cluster picker): no forwards to check
			pods, served = m.client.Resources.Served(kube.Pods)
		}
		if gvr := pods; served {
			for _, f := range m.forwards.List() {
				if !m.client.Cache.SyncedIn(gvr, f.Namespace) {
					continue
				}
				if f.Missing(m.forwardPodGone(gvr, f)) {
					m.forwards.Lost(f)
				}
			}
		}
		ended := m.endedHeld
		m.endedHeld = nil
		for _, f := range m.forwards.TakeEnded() {
			if f.OnClosed() {
				continue // from a connection you left: nothing to say here
			}
			ended = append(ended, endedForward{f, time.Now()})
		}
		for _, e := range ended {
			f := e.f
			podGone := !served // can't tell: as before, try another pod
			if served {
				podGone = m.forwardPodGone(pods, f)
			}
			if mv := m.moving[f.LocalPort]; mv != nil && mv != f {
				m.endedHeld = append(m.endedHeld, e) // the one it just moved to: once the move is settled
				continue
			}
			if m.portTaken(f.LocalPort) {
				continue // you started another on its port meanwhile: that one stands
			}
			if holdEnded(podGone, f.Failing(), time.Since(e.at)) {
				m.endedHeld = append(m.endedHeld, e)
				continue
			}
			// A forward to a service or deployment follows a pod of it,
			// whether its own went away or the connection dropped; it
			// says what happened only if it can't go on.
			if follows(f, podGone) {
				moves := 0
				if time.Since(f.Started) < 10*time.Second { // ended within moments of starting
					moves = f.Moves + 1
				}
				what := "the connection to pod " + f.Pod + " dropped"
				if podGone {
					what = "pod " + f.Pod + " went away"
				}
				if m.moving == nil {
					m.moving = map[int]*kube.Forward{}
				}
				m.moving[f.LocalPort] = f
				again = append(again, func() tea.Msg {
					nf, err := f.Again()
					if errors.Is(err, kube.ErrLeftCluster) {
						return nil // you switched cluster meanwhile: nothing to say here
					}
					msg := resultMsg{moved: f, next: nf, moves: moves}
					if err != nil {
						why := fmt.Sprintf("%s and it couldn't start again: %v", what, err)
						msg.err = fmt.Errorf("port-forward to %s stopped: %s", f.Target, why)
						msg.stopped = &stoppedForward{port: f.LocalPort, podPort: f.PodPort, target: f.Target, why: why, at: time.Now()}
						return msg
					}
					msg.text = what + "; forwarding again " + nf.String()
					return msg
				})
				continue
			}
			if gvr := pods; served {
				if m.forwardPodGone(gvr, f) {
					next, again := "F on another pod (or on its deployment, which follows new pods) starts it again", ""
					if p, listed := m.client.Cache.Get(gvr, f.Namespace, f.Pod); listed && p.GetDeletionTimestamp() == nil {
						next = "A new pod runs under the same name: F on it starts it again" // a StatefulSet's
						again = next
					}
					m.setFlash("", fmt.Errorf("port-forward localhost:%d stopped: pod %s went away. %s", f.LocalPort, f.Pod, next))
					m.noteStopped(stoppedForward{port: f.LocalPort, podPort: f.PodPort, target: orPod(f.Target, f.Pod), why: "pod " + f.Pod + " went away", again: again, at: time.Now()})
					continue
				}
			}
			why := "the connection closed"
			if err := f.Err(); err != nil {
				why = err.Error()
			}
			hint, note := forwardStopWords(f.Failing(), f.PodPort)
			m.setFlash("", fmt.Errorf("port-forward localhost:%d stopped: %s. %s F starts it again", f.LocalPort, why, hint))
			m.noteStopped(stoppedForward{port: f.LocalPort, podPort: f.PodPort, target: orPod(f.Target, f.Pod),
				why: fmt.Sprintf("%s (%s)", why, note), at: time.Now()})
		}

		m.noticeNewPods()
		if m.mode == modePrompt && m.prompt != nil && m.prompt.tick != nil {
			m.prompt.tick()
		}
		if m.mode == modePager && m.pager != nil && m.pager.live != nil {
			m.pager.lines = strings.Split(strings.TrimRight(m.pager.live(), "\n"), "\n")
			m.pager.refresh()
		}
		if m.loc.kind == locFind { // once a second, not on every letter typed
			m.refreshFindApps()
		}
		m.refreshRows()
		if m.loc.kind == locHelm && !m.helmLoading && (time.Since(m.helmAt) > helmRefresh || m.helmNS != m.namespace) { // stale, or another namespace now
			return m, tea.Batch(tick(), m.fetchHelm(), m.backgroundCmds())
		}
		cmds := []tea.Cmd{tick(), m.backgroundCmds(), m.secretErrCmds(), m.argoCheckCmds(), m.crashCheckCmd(), m.tlsSecretCmds()}
		cmds = append(cmds, again...)
		return m, tea.Batch(cmds...)

	case argoCheckMsg:
		m.onArgoCheck(msg)
		return m, nil

	case crashCheckMsg:
		m.onCrashCheck(msg)
		return m, nil

	case secretErrMsg:
		m.onSecretErr(msg)
		return m, nil

	case tlsSecretMsg:
		m.onTLSSecret(msg)
		return m, nil

	case clusterBillMsg:
		m.onClusterBill(msg)
		return m, nil

	case resolvedMsg:
		m.onResolved(msg)
		return m, nil

	case billedMsg:
		m.onBilled(msg)
		return m, nil

	case spotMsg:
		m.onSpot(msg)
		return m, nil

	case helmMsg:
		m.helmLoading, m.helmAt = false, time.Now()
		m.helm, m.helmErr, m.helmNS = msg.rels, msg.err, msg.ns
		m.refreshRows()
		if m.loc.kind == locHelm && msg.ns != m.namespace { // the namespace changed while it read
			return m, m.fetchHelm()
		}
		return m, nil

	case drainMsg:
		m.busy = drainStatus(msg.node, msg.progress)
		if msg.done {
			m.busy = ""
			if msg.err != nil {
				m.setFlash("", msg.err)
			} else {
				m.setFlash(fmt.Sprintf("drained %s: %d pods evicted", msg.node, msg.progress.Evicted), nil)
			}
			return m, nil
		}
		return m, waitDrain(msg.ch)

	case connectedMsg:
		m.onConnected(msg)
		if m.client != nil {
			if ns, ok := m.client.Resources.Served(kube.Namespaces); ok {
				m.client.Cache.Watch(ns) // small, and :ns can suggest names straight away
			}
			return m, m.readMarker()
		}
		return m, nil

	case markerMsg:
		m.onMarker(msg)
		return m, nil

	case pricesMsg:
		if msg.err != nil {
			m.priceMsg = "price refresh failed"
		} else {
			m.priceTbl, m.priceMsg = msg.table, ""
			if m.enr != nil {
				m.enr.SetPrices(msg.table)
				m.enr.SetBilled(m.rates.billed) // its discount is measured against list prices
			}
		}
		return m, nil

	case loadedMsg:
		if msg.id != m.loadWaiting { // you left or asked again meanwhile: not over where you are now
			return m, nil
		}
		m.loadWaiting = 0
		if m.loadOwnsBusy && m.busy == m.loadBusy {
			m.busy = ""
		}
		if m.busy != "" && m.loadOwnsBusy && m.busy != m.loadBusy {
			return m, nil // another action took over the footer meanwhile (d, y): it comes, not this
		}
		if m.whereNow() != m.loadWhere {
			if m.pager != nil && m.pager.title == "help" {
				m.loadPending = msg.run // help is over the screen it was asked on: when help closes
				return m, nil
			}
			// You opened something else meanwhile (a prompt, a menu, : or /,
			// another namespace): it never replaces that, and says so.
			m.setFlash(m.loadWhat+" loaded after you moved on; ask again to see them", nil)
			return m, nil
		}
		return m, msg.run()

	case resultMsg:
		m.busy = ""
		if msg.moved != nil {
			if m.moving[msg.moved.LocalPort] != msg.moved { // you stopped it (ctrl-d) or switched cluster meanwhile
				if m.stopping[msg.moved.LocalPort] == msg.moved {
					delete(m.stopping, msg.moved.LocalPort)
				}
				if msg.next != nil {
					msg.next.Again = nil // never followed again
					m.forwards.Stop(msg.next)
					m.dropHeld(msg.next)
				}
				return m, nil
			}
			delete(m.moving, msg.moved.LocalPort)
			if msg.next != nil {
				msg.next.Moves = msg.moves
			}
		}
		if msg.stopped != nil {
			m.noteStopped(*msg.stopped)
		}
		if msg.shellEnd {
			m.keysQuietUntil = time.Now().Add(shellQuiet)
		}
		m.setFlash(msg.text, msg.err)
		if msg.err != nil { // what you asked for failed: keys pressed while it waited don't hide that
			m.flashKeep = time.Now().Add(failureKeep)
		}
		m.refreshRows()
		return m, nil

	case pagerMsg:
		if m.dropPager { // you left while it was loading: don't open it over where you are now
			m.dropPager = false
			return m, nil
		}
		m.busy = ""
		if msg.err != nil {
			m.setFlash("", msg.err)
			return m, nil
		}
		p := newPager(msg.title, msg.content, m.pagerWidth(), m.pagerHeight())
		p.editRow, p.plain, p.back, p.secret, p.csv = msg.editRow, msg.plain, msg.back, msg.secret, msg.csv
		p.plainFrom, p.diff = msg.plainFrom, msg.diff
		if msg.hang > 0 || msg.tree {
			p.hang, p.wrap, p.tree = msg.hang, true, msg.tree
			p.refresh()
		}
		m.openPager(p)
		return m, nil

	case logLinesMsg:
		if m.underHelp != nil && m.underHelp.logs == msg.ch { // reading help; the logs keep coming
			m.underHelp.append(msg.lines)
			if msg.done {
				m.underHelp.ended = true
				return m, nil
			}
			return m, waitLogs(msg.ch)
		}
		if m.pager == nil || m.pager.logs != msg.ch {
			return m, nil // stream belongs to a closed pager
		}
		if msg.done && m.mayBeGone(m.pager.source) {
			// The stream may have ended with its pod: ask the cluster off
			// the screen's loop, and say how it ended once known.
			held := slices.DeleteFunc(slices.Clone(msg.lines), func(l string) bool { return !lost([]string{l}) })
			m.appendLogs(slices.DeleteFunc(msg.lines, func(l string) bool { return lost([]string{l}) }))
			return m, m.checkPodGone(msg.ch, held)
		}
		m.appendLogs(msg.lines)
		if msg.done {
			m.endLogs(msg.lines, false)
			return m, nil
		}
		return m, waitLogs(msg.ch)

	case podGoneMsg:
		if m.pager == nil || m.pager.logs != msg.ch {
			return m, nil // those logs were closed meanwhile
		}
		if !msg.gone { // the pod is there: the connection's note goes back in
			m.appendLogs(msg.held)
		}
		m.endLogs(msg.held, msg.gone)
		return m, nil

	case shellStartedMsg:
		m.busy = ""
		if msg.err != nil {
			m.setFlash("", fmt.Errorf("shell: %w", msg.err))
			return m, nil
		}
		m.term = newTerminal(msg.title, msg.sess, m.pagerWidth(), m.pagerHeight())
		m.mode = modeShell
		return m, waitTerminal(m.term)

	case terminalMsg:
		if msg.t != m.term {
			return m, nil // an old shell
		}
		if msg.done {
			err := m.term.sess.Err()
			if b := m.term.brokenErr(); b != nil {
				err = b
			}
			r := shellEnded(err)
			m.closeShell()
			m.setFlash(r.text, r.err)
			m.keysQuietUntil = time.Now().Add(shellQuiet) // what you were typing into it stays out of kubed
			return m, nil
		}
		return m, waitTerminal(m.term)

	case editReadyMsg:
		m.busy = ""
		return m, m.onEditReady(msg)

	case editDoneMsg:
		return m, m.onEditDone(msg)

	case tea.MouseMsg:
		return m, m.onMouse(msg)

	case tea.KeyMsg:
		if time.Now().Before(m.keysQuietUntil) {
			// Typed into the shell that just closed: drop it, and keep
			// dropping while the typing goes on ("exit" and enter).
			m.keysQuietUntil = time.Now().Add(shellTyping)
			return m, nil
		}
		if msg.String() != "q" {
			m.quitArmed = false // only q right after the warning quits
		}
		if m.flashErr && time.Since(m.flashAt) > 2*time.Second && !time.Now().Before(m.flashKeep) {
			m.flash = "" // read and moved on: the footer's keys come back
		}
		return m, m.onKey(msg)
	}
	return m, nil
}

func (m *Model) onConnected(msg connectedMsg) {
	if msg.context != m.context {
		if msg.client != nil {
			msg.client.Close()
		}
		return
	}
	m.connecting = false
	if msg.err != nil {
		m.connErr = msg.err
		if p := explainConnErr(msg.err.Error(), "", ""); strings.HasPrefix(p.what, "There is no such cluster") {
			m.jumpTo(location{kind: locContexts}) // nothing to retry: pick one
			m.contextNote = fmt.Sprintf("no cluster called %q; pick one", m.context)
		}
		return
	}
	m.connErr = nil
	m.client = msg.client
	m.connectedAt = time.Now()
	m.enr = enrich.New(m.client.Cache, m.client.Resources, m.priceTbl)
	m.enr.SetUsage(m.client.Metrics)
	m.resetRates(m.client)
	if m.opts.Discount != nil {
		m.enr.SetDiscount(m.opts.Discount(m.context, shortContext(m.context), m.client.Cluster))
	}
	m.client.Cache.SetNamespace(m.namespace)
	m.resolveEnvironment()
	m.saveState()                    // remember -n / the context's namespace even if kubed is killed
	m.client.Cache.Watch(kube.Nodes) // the header sums nodes, cost and usage

	m.history = nil
	switch first := m.opts.Resource; first {
	case "", "home":
		m.jumpTo(location{kind: locHome}) // what's wrong right now
	default:
		if !m.openResource(first, false) {
			m.jumpTo(location{kind: locHome})
		}
	}
}

// switchContext tears down the current connection and connects to another.
func (m *Model) switchContext(ctx string) tea.Cmd {
	m.closeShell()
	if m.client != nil {
		m.client.Close() // first: a forward still connecting then sees it closed
	}
	m.forwards.StopAll()
	m.endedHeld = nil // the old cluster's: never started again in the new one
	m.stoppedForwards = nil
	m.moving, m.stopping = map[int]*kube.Forward{}, map[int]*kube.Forward{}
	m.forwards.TakeEnded() // nor ones that ended just now
	if m.nsByContext == nil {
		m.nsByContext = map[string]string{}
	}
	m.nsByContext[m.context], m.prevContext = m.chosenNS, m.context
	m.client, m.enr = nil, nil
	m.env, m.asked = environment{}, false
	m.context, m.connecting, m.connErr = ctx, true, nil
	m.contextNote = ""
	m.connectStart = time.Now()
	m.namespace = m.kc.DefaultNamespace(ctx)
	if ns, ok := m.nsByContext[ctx]; ok { // back on a cluster: where you were on it
		m.namespace = ns
	}
	m.chosenNS = m.namespace
	m.recentNS = nonEmpty(m.namespace)
	if m.opts.State != nil {
		m.recentNS = mergeRecent(m.namespace, m.opts.State.RecentNamespaces[ctx])
	}
	m.loc, m.history = location{}, nil
	m.tbl = table{}
	// Nothing from the old cluster may show while the new one loads.
	m.apps, m.wastes, m.secretErrs, m.argoChecks, m.tlsSecrets = nil, nil, nil, nil, nil
	m.crashChecks, m.crashInFlight = nil, nil
	m.rates = rates{}
	return m.connect(ctx)
}

// howItStopped says why a container's previous run ended, for the end of
// its previous logs: "ran out of memory (limit 512Mi) at 14:09".
// restarts: the pod's container is started again when it stops (not a
// job's pod that ran once).
func (m *Model) restarts(src logSource) bool {
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return true
	}
	o, ok := m.client.Cache.Get(gvr, src.ns, src.pod)
	if !ok {
		return true
	}
	policy, _, _ := unstructured.NestedString(o.Object, "spec", "restartPolicy")
	phase, _, _ := unstructured.NestedString(o.Object, "status", "phase")
	return policy != "Never" && phase != "Succeeded" && phase != "Failed"
}

// noticeNewPods says, once each, when a workload whose logs you follow
// gets a pod the stream doesn't read (a replacement, a rollout's): r
// reads the pods it has now.
func (m *Model) noticeNewPods() {
	p := m.pager
	if m.mode != modePager || p == nil || !p.isLogs || p.source.pods == nil {
		return
	}
	reading := map[string]bool{}
	for _, s := range p.source.streams {
		reading[s[0]] = true
	}
	if p.noticed == nil {
		p.noticed = map[string]bool{}
	}
	streams, _ := p.source.pods()
	for _, s := range streams {
		if pod := s[0]; !reading[pod] && !p.noticed[pod] {
			p.noticed[pod] = true
			words := "── new pod " + pod + " started; these logs don't follow it yet. r reads the pods there are now"
			if p.ended { // a cron job's next run, after the last one's logs ended
				words = "── new pod " + pod + " started. r reads its logs"
			}
			p.note(styleWarn.Render(words))
		}
	}
}

// notStartedYet says why logs that ended with no line at all have none:
// no container of their pods has started (a pod no node has taken yet).
// "" when one has, or kubed can't tell.
func (m *Model) notStartedYet(src logSource) string {
	if src.previous || m.client == nil {
		return ""
	}
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return ""
	}
	pods := []string{src.pod}
	if src.workload != "" {
		pods = nil
		for _, s := range src.streams {
			if !slices.Contains(pods, s[0]) {
				pods = append(pods, s[0])
			}
		}
	}
	noNode := true
	reason := "" // one pod on a node: why it waits, in words
	for _, name := range pods {
		o, ok := m.client.Cache.Get(gvr, src.ns, name)
		if !ok {
			return ""
		}
		p, err := view.ToPod(o)
		if err != nil || p.Status.Phase != corev1.PodPending {
			return ""
		}
		for _, cs := range append(append([]corev1.ContainerStatus(nil), p.Status.ContainerStatuses...), p.Status.InitContainerStatuses...) {
			if cs.State.Running != nil || cs.State.Terminated != nil || cs.LastTerminationState.Terminated != nil {
				return "" // one ran: it may have logs
			}
		}
		noNode = noNode && p.Spec.NodeName == ""
		if len(pods) == 1 && p.Spec.NodeName != "" && m.enr != nil {
			reason = view.Explain(m.enr, p).Why // "main can't download nope:1: …", "volume cfg: …"
		}
	}
	if len(pods) == 0 {
		return ""
	}
	why := "the container hasn't started yet, so there are no logs"
	if len(pods) > 1 {
		why = "no container has started yet, so there are no logs"
	}
	if noNode {
		why += " (waiting for a node to run on)"
	} else if reason != "" {
		why += "; " + reason
	}
	return why
}

// waitingToRestart: the pod's container (the logs' one) crashed and waits
// for Kubernetes to start it again.
func (m *Model) waitingToRestart(src logSource) bool {
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok || src.pod == "" {
		return false
	}
	o, ok := m.client.Cache.Get(gvr, src.ns, src.pod)
	if !ok {
		return false
	}
	p, err := view.ToPod(o)
	if err != nil {
		return false
	}
	for _, cs := range append(append([]corev1.ContainerStatus(nil), p.Status.ContainerStatuses...), p.Status.InitContainerStatuses...) { // a sidecar or setup step too
		if (cs.Name == src.container || (src.container == "" && len(p.Status.ContainerStatuses) == 1)) &&
			(cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" || cs.State.Terminated != nil && cs.State.Terminated.Reason != "Completed" && cs.RestartCount > 0) {
			return true
		}
	}
	return false
}

func (m *Model) howItStopped(src logSource) string {
	if src.pod == "" || m.client == nil {
		return ""
	}
	gvr, ok := m.client.Resources.Served(kube.Pods)
	if !ok {
		return ""
	}
	o, ok := m.client.Cache.Get(gvr, src.ns, src.pod)
	if !ok {
		return ""
	}
	p, err := view.ToPod(o)
	if err != nil {
		return ""
	}
	for _, cs := range append(append([]corev1.ContainerStatus(nil), p.Status.ContainerStatuses...), p.Status.InitContainerStatuses...) { // a sidecar or setup step too
		// Before a restart: how that run ended. Live logs that ended (a
		// cron job's run, a finished job): how this one ended.
		t := cs.State.Terminated
		if src.previous {
			t = cs.LastTerminationState.Terminated
		}
		if t == nil && !src.previous && cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			t = cs.LastTerminationState.Terminated // waiting to restart: these logs are the run that ended
		}
		mine := cs.Name == src.container || (src.container == "" && len(p.Status.ContainerStatuses) == 1)
		if !mine || t == nil {
			continue
		}
		at := t.FinishedAt.Local().Format("15:04")
		if check, _, killed := m.enr.KilledByCheck(p.Namespace, p.Name, cs.Name, t.FinishedAt.Time, t.ExitCode); killed {
			return "was killed by its " + check + ", at " + at
		}
		if view.KilledByHook(m.enr, p, cs.Name, t.FinishedAt.Time) { // as home says
			return "was killed because its postStart hook failed, at " + at
		}
		switch t.Reason {
		case "OOMKilled":
			return "ran out of memory and was killed, at " + at
		case "Error":
			return fmt.Sprintf("exited with an error (exit %d), at %s", t.ExitCode, at)
		case "Completed":
			return "finished, at " + at
		}
		return fmt.Sprintf("ended (%s, exit %d), at %s", t.Reason, t.ExitCode, at)
	}
	return ""
}

// --- navigation ---

// openResource switches the table to a resource. drill keeps the trail
// (Enter from another view); otherwise it is a jump and starts a new one.
func (m *Model) openResource(name string, drill bool) bool {
	res, ok := m.findKind(name)
	if ok {
		m.openKind(res, drill)
	}
	return ok
}

// findKind is the kind a word names; when there is none it says so.
func (m *Model) findKind(name string) (kube.Resource, bool) {
	if m.client == nil {
		return kube.Resource{}, false
	}
	res, ok := m.client.Resources.Lookup(resolveAlias(name))
	if !ok && m.client.Rediscover() { // maybe a CRD newer than the cached discovery
		res, ok = m.client.Resources.Lookup(resolveAlias(name))
	}
	if !ok {
		if full := resolveAlias(name); full != name || strings.Contains(strings.ToLower(name), "externalsecret") {
			m.setFlash("", fmt.Errorf("%s", notInstalled(strings.ToLower(full))))
			return res, false
		}
		m.setFlash("", fmt.Errorf("there's nothing called %q. ctrl-a lists every kind you can open", name))
		return res, false
	}
	return res, true
}

func (m *Model) openKind(res kube.Resource, drill bool) {
	loc := location{kind: locResource, res: res}
	if drill {
		m.navigate(loc, true)
	} else {
		m.jumpTo(loc)
	}
}

// jumpTo opens a view by jumping (g…, :name, ctrl-a). esc comes back to
// where you jumped from, one jump at a time; home starts the trail afresh.
func (m *Model) jumpTo(loc location) {
	loc.jumped = true
	switch {
	case loc.kind == locHome || loc.kind == locContexts || m.client == nil || m.loc.unset():
		m.history = nil // the start: nothing to go back to
		m.navigate(loc, false)
	case m.trailHas(loc) >= 0: // already on the trail: back to it there, so the trail never repeats
		i := m.trailHas(loc)
		back := m.history[i]
		m.history = m.history[:i]
		m.navigate(back, false)
	default:
		m.navigate(loc, true) // esc comes back to where you jumped from, one jump at a time
		if over := len(m.history) - maxBackSteps; over > 0 {
			m.history = m.history[over:] // a long walk forgets its oldest steps
		}
	}
}

// trailHas is where the trail already holds the list loc opens, or -1.
// Only plain lists count: a drill or an app's list is its own screen.
func (m *Model) trailHas(loc location) int {
	for i, h := range m.history {
		if h.kind == loc.kind && h.kind != locResource && h.kind != locContainers && h.app == "" && loc.app == "" {
			return i
		}
		if h.kind == locResource && loc.kind == locResource && h.res.GVR == loc.res.GVR &&
			h.drill == nil && loc.drill == nil && h.app == "" && loc.app == "" && h.ns == m.namespace {
			return i
		}
	}
	return -1
}

// maxBackSteps is how many screens esc can walk back through.
const maxBackSteps = 30

// unset: no screen has been shown yet (kubed is starting).
func (l location) unset() bool {
	return l.kind == locResource && l.res.GVR.Resource == ""
}

func (m *Model) navigate(loc location, push bool) {
	m.abandonWait()
	if push {
		m.loc.filter, m.loc.cursor, m.loc.picked = m.tbl.filter, m.tbl.cursor, ""
		if r, ok := m.tbl.selected(); ok && r.Obj != nil {
			m.loc.picked = rowID(r)
		}
		m.loc.sort, m.loc.desc, m.loc.sorted = m.tbl.sort, m.tbl.desc, true // coming back keeps your order
		if m.loc.kind == locFind && (m.mode == modeFind || m.loc.query == "") {
			m.loc.query = m.input.Value() // shown again on the way back (a filter typed on the results is not the search)
		}
		m.history = append(m.history, m.loc)
	}
	if loc.kind != locResource || loc.res.GVR != m.pendingGVR {
		m.pendingSelect = "" // you went somewhere else first
	}
	m.loc = loc
	m.loc.ns = m.namespace
	m.tbl.reset()
	m.tbl.clearMarks()
	m.tbl.filter = loc.filter
	m.tbl.jump(loc.cursor)
	m.tbl.sort, m.tbl.desc = sortDefault, false
	if loc.kind == locResource && loc.res.GVR.Resource == kube.Events.Resource {
		m.tbl.sort = sortAge
	}
	if loc.drill != nil && loc.drill.GVR.Resource == kube.Jobs.Resource { // a cronjob's runs: newest first
		m.tbl.sort = sortAge
	}
	if loc.sorted { // back to a list you had sorted
		m.tbl.sort, m.tbl.desc = loc.sort, loc.desc
		m.tbl.touched = true // and the cursor stays on the row you left
	}
	switch {
	case m.client == nil: // not connected: nothing to load yet
	case loc.kind == locResource:
		m.warm(loc.res)
	case loc.kind == locHome:
		m.warmStaged(view.AppStages...)
	case loc.kind == locWaste:
		for _, g := range []schema.GroupVersionResource{kube.Pods, kube.Nodes, kube.ReplicaSets, kube.Deployments} {
			if served, ok := m.client.Resources.Served(g); ok {
				m.client.Cache.Watch(served)
			}
		}
	}
	m.refreshRows()
	if loc.picked != "" { // rows may have come or gone above it meanwhile
		m.tbl.want = loc.picked // still loading: found once it is listed
		for i, r := range m.tbl.rows {
			if r.Obj != nil && rowID(r) == loc.picked {
				m.tbl.jump(i)
				m.tbl.touched, m.tbl.want = true, ""
				break
			}
		}
	}
	if loc.kind == locContexts && loc.cursor == 0 {
		m.selectNamed(m.context) // start on the cluster you're on
	}
}

// applyPendingSelect picks the row openNamed asked for, once it is listed;
// leaving the list drops the wish.
func (m *Model) applyPendingSelect() {
	if m.pendingSelect == "" {
		return
	}
	if m.loc.kind != locResource || m.loc.res.GVR != m.pendingGVR {
		return // not on that list (yet): an object of the same name elsewhere isn't it
	}
	for i, r := range m.tbl.rows {
		if r.Obj != nil && r.Obj.GetName() == m.pendingSelect {
			m.tbl.jump(i)
			m.tbl.touched, m.pendingSelect = true, ""
			return
		}
	}
}

// rowID names a row's object the way the table does when rows change.
func rowID(r view.Row) string {
	return string(r.Obj.GetUID()) + r.Obj.GetKind() + r.Obj.GetNamespace() + "/" + r.Obj.GetName()
}

// warmStaged loads lists in groups, each after the previous one synced:
// they share one connection, and the first group (pods) is what the screen
// needs to show anything.
func (m *Model) warmStaged(groups ...[]schema.GroupVersionResource) {
	c := m.client
	go func() {
		for _, g := range groups {
			var started []schema.GroupVersionResource
			for _, gvr := range g {
				if served, ok := c.Resources.Served(gvr); ok {
					c.Cache.Watch(served)
					started = append(started, served)
				}
			}
			deadline := time.Now().Add(warmupWait)
			for _, gvr := range started {
				for !c.Cache.Synced(gvr) && c.Cache.Err(gvr) == nil && time.Now().Before(deadline) {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()
}

// costsComing: prices are being worked out for this cluster (its AWS
// account is known); the money columns keep their place meanwhile.
func (m *Model) costsComing() bool {
	return m.enr != nil && !m.enr.Costs() && m.rates.viaAWS && m.rates.resolveErr == nil
}

// moneyColumns are left out of every table while kubed shows no costs.
var moneyColumns = map[string]bool{"COST/MO": true, "$/MO": true, "SAVES/MO": true}

// bootWait is the longest the first screen holds back background work.
const bootWait = 10 * time.Second

// booted: the first screen has its data (or waited long enough), so
// background work can't slow it down.
func (m *Model) booted() bool {
	if m.client == nil {
		return false
	}
	return time.Since(m.connectedAt) > bootWait || (m.loc.kind == locHome && m.homeLoading() == "") ||
		(m.loc.kind == locResource && m.client.Cache.Synced(m.loc.res.GVR)) ||
		(m.loc.kind != locHome && m.loc.kind != locResource)
}

// backgroundCmds is work that waits until the first screen is up: the
// daily price list download and reading the account's rates.
func (m *Model) backgroundCmds() tea.Cmd {
	if !m.booted() {
		return nil
	}
	var cmds []tea.Cmd
	if m.priceStale {
		m.priceStale = false
		m.priceMsg = "refreshing prices…"
		cmds = append(cmds, m.refreshPrices())
	}
	return tea.Batch(append(cmds, m.rateCmds())...)
}

// containersPod is the pod whose containers are listed.
func (m *Model) containersPod() (*unstructured.Unstructured, bool) {
	gvr, _ := m.client.Resources.Served(kube.Pods)
	return m.client.Cache.Get(gvr, m.loc.podNS, m.loc.podName)
}

func (m *Model) back() {
	m.abandonWait()
	if len(m.history) == 0 {
		return
	}
	prev := m.history[len(m.history)-1]
	m.history = m.history[:len(m.history)-1]
	// A list a search result was opened in only served to show its menu:
	// going back skips it, to the search results.
	// Home opened from a result is a real stop (the app stays selected),
	// so esc from its pods comes back to it, not past it.
	for prev.fromFind && prev.kind != locHome && len(m.history) > 0 && m.history[len(m.history)-1].kind == locFind {
		prev = m.history[len(m.history)-1]
		m.history = m.history[:len(m.history)-1]
	}
	if prev.ns != m.namespace {
		// Opening an app or a search result moves to its namespace; going
		// back puts you where you were.
		m.switchNamespace(prev.ns)
	}
	m.navigate(prev, false)
	if prev.kind == locFind {
		m.showFound(prev.query, prev.cursor)
		// The row you opened, wherever it is now: rows may have come
		// above it meanwhile. Gone: the same place in the list, as before.
		for i, r := range m.tbl.rows {
			if prev.picked != "" && r.Obj != nil && rowID(r) == prev.picked {
				m.tbl.jump(i)
				m.tbl.touched = true
				break
			}
		}
	}
}

// shellQuiet is how long keys are ignored after a shell closes, and
// shellTyping how long each key typed meanwhile extends it.
const (
	shellQuiet  = 2 * time.Second
	shellTyping = 700 * time.Millisecond
)

// warmupWait bounds how long enrichment informers wait for the main list.
const warmupWait = 10 * time.Second

// warm starts the view's own informer, then (once it has synced) the ones
// its enrichment needs. Starting them together makes the main list share
// bandwidth with large lists like ReplicaSets and show up seconds later.
func (m *Model) warm(res kube.Resource) {
	c := m.client
	c.Cache.Watch(res.GVR)
	go func() {
		deadline := time.Now().Add(warmupWait)
		for !c.Cache.Synced(res.GVR) && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		for _, need := range view.For(res).Needs {
			if gvr, ok := c.Resources.Served(need); ok {
				c.Cache.Watch(gvr)
			}
		}
	}()
}

// refreshRows rebuilds the table from the cache.
func (m *Model) refreshRows() {
	defer m.applyPendingSelect()
	m.tbl.hidden = map[string]bool{}
	m.tbl.problemsFirst = false
	m.tbl.main = ""
	if !m.enr.Costs() && !m.costsComing() {
		for c := range moneyColumns {
			m.tbl.hidden[c] = true
		}
	}
	if m.loc.kind == locHome && m.namespace != "" {
		m.tbl.hidden["NAMESPACE"] = true // the same on every row
	}
	if d := m.loc.drill; d != nil && strings.HasPrefix(d.Title, "on node ") {
		m.tbl.hidden["NODE"] = true // every row is on that node
	}
	switch m.loc.kind {
	case locContexts:
		m.tbl.optional = []string{"REGION"}
		m.tbl.setRows([]string{"CLUSTER", "PRODUCTION?", "REGION", ""}, m.contextRows(), nil)
		return
	case locForwards:
		m.tbl.optional = nil
		m.tbl.main = "STATUS"
		m.tbl.setRows([]string{"LOCAL", "REACHES", "POD PORT", "THROUGH POD", "STATUS"}, m.forwardRows(), nil)
		return
	case locResources:
		m.tbl.optional = []string{"GROUP", "KIND"}
		m.tbl.setRows([]string{"NAME", "SHORT NAMES", "KIND", "GROUP", "NAMESPACED"}, m.resourceRows(), nil)
		return
	case locContainers:
		m.tbl.optional = view.ContainerOptional
		m.tbl.main = "IMAGE"
		var rows []view.Row
		if pod, ok := m.containersPod(); ok {
			rows = view.ContainerRows(m.enr, pod)
		}
		m.tbl.setRows(view.ContainerHeader, rows, nil)
		return
	case locHome:
		m.tbl.optional = []string{"RESTARTS", "DEPLOYED", "WHY", "COST/MO", "NAMESPACE"} // what is wrong outlasts restarts and dates; where an app lives goes last
		m.tbl.main = "WHY"
		if m.client != nil && m.enr != nil && m.homeLoading() == "" {
			m.refreshApps()                // once per refresh; the summary box reuses it
			deployed := map[string]int64{} // A sorts apps by when they were deployed
			for _, a := range m.apps {
				deployed[a.Namespace+"/"+a.Name] = a.LastDeploy.Unix()
			}
			rows := view.AppRows(m.apps)
			// Still loading: "…", not "-" (which means "none"), so the
			// columns hold their place instead of shifting when it arrives.
			rsLoading := false
			if g, ok := m.client.Resources.Served(kube.ReplicaSets); ok && !m.client.Cache.Synced(g) {
				rsLoading = true
			}
			for i := range rows {
				c := rows[i].Cells
				if m.costsComing() && c[6] == "-" {
					c[6] = "…"
				}
				if rsLoading && c[5] == "-" {
					c[5] = "…"
				}
			}
			look := view.Lookups{Secret: m.secretError, Argo: m.argoLookup, ReadOnly: m.opts.ReadOnly}
			for i, a := range m.apps { // WHY says what the details box says
				if why := view.AppWhy(a, look); why != "" && i < len(rows) {
					rows[i].Cells[len(rows[i].Cells)-1] = why
				}
			}
			all := len(rows) // before a label filter: "(1 of 4)"
			// "/-l app=web": apps have no labels of their own; an app shows
			// when one of its pods matches. After WHY is filled, which goes
			// by position in the full list.
			if isLabelFilter(m.tbl.filter) {
				if sel, err := labels.Parse(strings.TrimSpace(strings.TrimPrefix(m.tbl.filter, "-l"))); err == nil {
					kept := rows[:0:0]
					for _, r := range rows {
						a, ok := m.appFor(r)
						if !ok {
							continue
						}
						for _, p := range m.appPodRows(a) {
							if sel.Matches(labels.Set(p.Obj.GetLabels())) {
								kept = append(kept, r)
								break
							}
						}
					}
					rows = kept
				}
			}
			m.tbl.setRows(view.AppHeader, rows, func(r view.Row) int64 {
				return deployed[r.Obj.GetNamespace()+"/"+r.Obj.GetName()]
			})
			m.tbl.total = all // "(1 of 4)", as a text filter says
		}
		return
	case locWaste:
		m.tbl.optional = []string{"MEM PEAK", "CPU PEAK", "KIND", "NAMESPACE", "PODS"}
		m.tbl.main = "INSTEAD (CPU / MEM)" // the point of the screen: never cut first
		if m.enr != nil {
			gvr, _ := m.client.Resources.Served(kube.Pods)
			m.wastes = m.enr.Wastes(m.client.Cache.List(gvr, m.namespace))
			if m.namespace != "" {
				m.tbl.hidden["NAMESPACE"] = true // the same on every row
			}

			m.tbl.setRows(view.WasteHeader, view.WasteRows(m.wastes, m.wasteEarly()), nil)
		}
		return
	case locFind:
		m.tbl.optional = []string{"NAMESPACE"}
		if m.client != nil {
			// The search typed, kept apart from the box: / filters these
			// results with the same box, not a new search.
			query := m.loc.query
			if m.mode == modeFind {
				query = m.input.Value()
			}
			m.tbl.setRows(findHeader, m.findRows(query), nil)
		}
		return
	case locHelm:
		m.tbl.optional = []string{"UPDATED", "NAMESPACE"}
		when := view.HelmTime(m.helm)
		m.tbl.setRows(view.HelmHeader, view.HelmRows(m.helm), func(r view.Row) int64 {
			return when(r.Obj.GetNamespace(), r.Obj.GetName()).Unix()
		})
		return
	}
	if m.client == nil {
		return
	}
	res := m.loc.res
	v := view.For(res)
	ns := ""
	if res.Namespaced {
		ns = m.namespace
	}
	if d := m.loc.drill; d != nil && d.AllNamespaces {
		ns = ""
	}
	objs := m.client.Cache.List(res.GVR, ns)
	if res.GVR == kube.ConfigMaps && !strings.Contains(m.tbl.filter, "root-ca") {
		// Kubernetes puts this one in every namespace; it is never what you look for.
		objs = filterObjs(objs, func(o *unstructured.Unstructured) bool { return o.GetName() != "kube-root-ca.crt" })
	}
	if d := m.loc.drill; d != nil {
		objs = filterObjs(objs, d.Match)
	}
	before := len(objs)
	if isLabelFilter(m.tbl.filter) {
		if sel, err := labels.Parse(strings.TrimSpace(strings.TrimPrefix(m.tbl.filter, "-l"))); err == nil {
			objs = filterObjs(objs, func(o *unstructured.Unstructured) bool { return sel.Matches(labels.Set(o.GetLabels())) })
		}
	}
	showNS := res.Namespaced && (m.namespace == "" || (m.loc.drill != nil && m.loc.drill.AllNamespaces))
	timeOf := func(r view.Row) int64 { return v.Time(r.Obj).Unix() }
	m.tbl.optional = view.Optional(v, showNS)
	m.tbl.main = v.Main
	m.tbl.problemsFirst = true
	rows := view.Rows(v, m.enr, objs, showNS)
	if res.GVR == kube.Namespaces && m.namespace != "" { // mark the one every screen shows
		for i, r := range rows {
			if r.Obj.GetName() == m.namespace && len(r.Cells) > 1 {
				rows[i].Cells = append([]string(nil), r.Cells...)
				rows[i].Cells[1] += "  ✓ showing now"
				rows[i].Tone = view.ToneOK
			}
		}
	}
	m.tbl.setRows(view.Header(v, showNS), rows, timeOf)
	m.tbl.total = before // "N of M" counts rows hidden by a label filter too
}

func filterObjs(objs []*unstructured.Unstructured, match func(*unstructured.Unstructured) bool) []*unstructured.Unstructured {
	out := objs[:0:0]
	for _, o := range objs {
		if match(o) {
			out = append(out, o)
		}
	}
	return out
}

func (m *Model) contextRows() []view.Row {
	if m.kc == nil {
		return nil
	}
	names, err := m.kc.Contexts()
	if err != nil {
		m.setFlash("", err)
	}
	rows := make([]view.Row, 0, len(names))
	for _, n := range names {
		kind, tone := "", view.ToneNormal
		switch env := envFromName(m.opts.ProdPattern, m.opts.NonProdPattern, n); {
		case env.known && env.prod:
			kind, tone = "yes", view.ToneBad
		case env.known:
			kind = "no"
		default:
			kind = "unknown" // its name doesn't say; kubed asks the first time you change something
		}
		if n == m.context && m.env.known { // what kubed decided for the one you're on
			kind = map[bool]string{true: "yes", false: "no"}[m.env.prod]
		}
		name := strings.TrimSuffix(shortContext(n), "-context") // "-context" says nothing
		now := ""
		if n == m.context {
			now = "✓ you're here"
			if tone == view.ToneNormal {
				tone = view.ToneOK
			}
		}
		region := arnRegion(n)
		if region == "" && n == m.context && m.enr != nil { // the one you're on: kubed knows its region
			region = m.enr.ClusterSummary().Region
		}
		rows = append(rows, view.Row{Obj: named(n), Cells: []string{name, kind, orDash(region), now}, Tone: tone})
	}
	return rows
}

func (m *Model) forwardRows() []view.Row {
	fs := m.forwards.List()
	rows := make([]view.Row, 0, len(fs))
	for _, f := range fs {
		status, tone := "Active", view.ToneNormal
		if why := f.Failing(); why != "" { // open, but connections through it fail
			status, tone = "Open, but the last connection failed ("+why+")", view.ToneWarn
			if refused(why) {
				status += ". Is anything listening on that port?"
			}
		}
		if err := f.Err(); err != nil {
			status, tone = err.Error(), view.ToneBad
		}
		reaches := f.Target // "service web"
		if reaches == "" {
			reaches = "pod " + f.Pod
		}
		through := f.Pod
		if f.Target == "" {
			through = "-" // it reaches the pod itself: no need to name it twice
		}
		rows = append(rows, view.Row{Obj: named(fmt.Sprint(f.LocalPort)), Tone: tone, Cells: []string{
			fmt.Sprintf("localhost:%d", f.LocalPort), reaches, fmt.Sprint(f.PodPort), through, status}})
	}
	open := map[int]bool{}
	for _, f := range fs {
		open[f.LocalPort] = true
	}
	for port, f := range m.moving {
		if open[port] {
			continue
		}
		open[port] = true
		rows = append(rows, view.Row{Obj: named(fmt.Sprint(port)), Tone: view.ToneWarn, Cells: []string{
			fmt.Sprintf("localhost:%d", port), f.Target, fmt.Sprint(f.PodPort), "-",
			"Connecting again through a pod of " + f.Target + " (a ready one if there is one within " + followWait.String() + ")"}})
	}
	for _, e := range m.endedHeld { // a moment while kubed learns why it ended
		f := e.f
		if open[f.LocalPort] {
			continue
		}
		open[f.LocalPort] = true
		reaches := f.Target
		if reaches == "" {
			reaches = "pod " + f.Pod
		}
		rows = append(rows, view.Row{Obj: named(fmt.Sprint(f.LocalPort)), Tone: view.ToneWarn, Cells: []string{
			fmt.Sprintf("localhost:%d", f.LocalPort), reaches, fmt.Sprint(f.PodPort), f.Pod,
			"Its connection ended a moment ago; checking whether its pod went away"}})
	}
	// Ones that stopped stay listed with why, so none vanishes unseen.
	for _, st := range m.stoppedForwards {
		if open[st.port] {
			continue // started again on that port
		}
		again := "To start it again, press F on " + st.target
		if strings.HasPrefix(st.target, "pod ") && strings.Contains(st.why, "went away") { // that pod is gone
			again = "F on another pod (or on its deployment, which follows new pods) starts it again"
		}
		if st.again != "" {
			again = st.again
		}
		rows = append(rows, view.Row{Obj: named(fmt.Sprint(st.port)), Tone: view.ToneBad, Cells: []string{
			fmt.Sprintf("localhost:%d", st.port), st.target, fmt.Sprint(st.podPort), "-",
			fmt.Sprintf("Stopped at %s: %s. %s; ctrl-d clears this line", st.at.Format("15:04"), st.why, again)}})
	}
	// By local port, whatever each is doing: a forward that moves to
	// another pod keeps its place, and the cursor stays on it.
	sort.SliceStable(rows, func(i, j int) bool {
		a, _ := strconv.Atoi(rows[i].Obj.GetName())
		b, _ := strconv.Atoi(rows[j].Obj.GetName())
		return a < b
	})
	return rows
}

// stoppedForward is a port-forward that ended and couldn't go on.
// endedForward is a forward that ended by itself, and when kubed saw it.
type endedForward struct {
	f  *kube.Forward
	at time.Time
}

// holdEnded: a forward ended while its pod still looks up. The connection
// can drop before kubed hears the pod is being deleted: wait a few
// seconds to tell a pod that went away (move to another) from a dropped
// connection. A refused connection needs no wait.
func holdEnded(podGone bool, failing string, waited time.Duration) bool {
	return !podGone && !refused(failing) && waited < 3*time.Second
}

type stoppedForward struct {
	port, podPort int
	target, why   string
	again         string // what starts it again, when not the usual
	at            time.Time
}

// noteStopped keeps a stopped forward for :pf, one line per local port.
func (m *Model) noteStopped(st stoppedForward) {
	kept := m.stoppedForwards[:0:0]
	for _, o := range m.stoppedForwards {
		if o.port != st.port {
			kept = append(kept, o)
		}
	}
	kept = append(kept, st)
	m.stoppedForwards = kept
}

// stoppedPort: a :pf row that is a stopped forward's line, not an open one.
func (m *Model) stoppedPort(row view.Row) bool {
	port, _ := strconv.Atoi(row.Obj.GetName())
	if m.portTaken(port) {
		return false
	}
	if m.moving[port] != nil { // going on through another pod: ctrl-d stops it
		return false
	}
	for _, e := range m.endedHeld { // still being looked into: ctrl-d stops it
		if e.f.LocalPort == port {
			return false
		}
	}
	for _, st := range m.stoppedForwards {
		if st.port == port {
			return true
		}
	}
	return false
}

// dropHeld forgets an ended forward waiting to be looked at.
func (m *Model) dropHeld(f *kube.Forward) {
	kept := m.endedHeld[:0:0]
	for _, e := range m.endedHeld {
		if e.f != f {
			kept = append(kept, e)
		}
	}
	m.endedHeld = kept
}

// portTaken: a running forward listens on that local port.
func (m *Model) portTaken(port int) bool {
	if m.moving[port] != nil {
		return true // going on through another pod
	}
	for _, f := range m.forwards.List() {
		if f.LocalPort == port {
			return true
		}
	}
	return false
}

// orPod is a forward's target, or its pod when it reached one directly.
func orPod(target, pod string) string {
	if target != "" {
		return target
	}
	return "pod " + pod
}

func (m *Model) resourceRows() []view.Row {
	if m.client == nil {
		return nil
	}
	all := m.client.Resources.All()
	rows := make([]view.Row, 0, len(all))
	for _, res := range all {
		name := resourceName(res, all)
		scope := "false"
		if res.Namespaced {
			scope = "true"
		}
		rows = append(rows, view.Row{Obj: named(name),
			Cells: []string{name, strings.Join(resourceAliases(res), " "), res.Kind, orDash(res.GVR.Group), scope}})
	}
	return rows
}

// named wraps a plain name so kubed's own lists reuse the table code.
func named(name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetName(name)
	return u
}

// setTip shows a note on what to do next ("pick one, then press +").
func (m *Model) setTip(text string) {
	m.setFlash(text, nil)
	m.flashTip = true
}

func (m *Model) setFlash(text string, err error) {
	m.flashAt, m.flashKeep = time.Now(), time.Time{}
	m.flashTip = false
	if err != nil {
		m.flash, m.flashErr = friendlyError(err.Error()), true
		return
	}
	m.flash, m.flashErr = text, false
}

// friendlyError rewords errors people hit often; others pass through.
func friendlyError(s string) string {
	low := strings.ToLower(s)
	switch {
	case strings.Contains(low, `"/bin/sh"`) && (strings.Contains(low, "no such file or directory") || strings.Contains(low, "executable file not found")):
		return "this container has no shell (/bin/sh isn't in its image)"
	case strings.Contains(low, "container not found"):
		return "that container isn't running right now (it may be crashing). p shows the logs from before it stopped"
	case strings.Contains(low, "permission denied") && strings.Contains(low, "listen"):
		return "only admin users can open local ports under 1024. Pick 1024 or higher, like 8080"
	case strings.Contains(low, "unable to listen on any of the requested ports"):
		return "that local port is already in use. Pick another one"
	case strings.Contains(low, "is forbidden"):
		if i := strings.Index(s, "is forbidden:"); i >= 0 {
			return "your login isn't allowed to do that:" + s[i+len("is forbidden:"):]
		}
		return "your login isn't allowed to do that"
	case strings.Contains(low, "field is immutable") || strings.Contains(low, "pod updates may not change fields other than"):
		words, _ := immutableWords(strings.ReplaceAll(s, "\n", " "))
		return words
	case strings.Contains(low, "the object has been modified"):
		return "someone changed it while you were editing. Open it again to see their change"
	case lostConnection(low):
		// The network, not what you asked. A change sent just before the
		// drop may still have gone through: don't promise it didn't.
		return "lost the connection to the cluster just now (the network dropped). Try again; for a change, check first whether it went through"
	}
	return s
}

// lostConnection: the error (lower case) is the network's, not an answer.
func lostConnection(low string) bool {
	for _, w := range []string{"no route to host", "connection refused", "i/o timeout", "connection reset", "no such host",
		"tls handshake timeout", "context deadline exceeded", "unexpected eof", "client connection lost"} {
		if strings.Contains(low, w) {
			return true
		}
	}
	return false
}

func (m *Model) openPager(p *pager) {
	if m.pager != nil {
		m.pager.close()
	}
	// A text screen took over: what waited for a list doesn't open over it,
	// and its "loading …" line goes with it. Help over the list doesn't
	// replace it: what waits goes on.
	if p.title != "help" {
		if m.loadWaiting != 0 && m.loadOwnsBusy && m.busy == m.loadBusy {
			m.busy = ""
		}
		m.loadWaiting = 0
	}
	m.pager = p
	m.mode = modePager
}

func (m *Model) closeShell() {
	if m.term != nil {
		m.term.close()
	}
	m.term = nil
	m.mode = modeTable
}

// runPending runs an action whose list loaded while help was open, now
// that help closed, if you are back where it was asked.
func (m *Model) runPending() tea.Cmd {
	run := m.loadPending
	m.loadPending = nil
	if run == nil || m.whereNow() != m.loadWhere {
		return nil
	}
	return run()
}

func (m *Model) closePager() {
	if m.pager != nil {
		m.pager.close()
	}
	m.pager = nil
	m.mode = modeTable
	if m.underHelp != nil { // help was opened over logs: back to them
		m.pager, m.underHelp, m.mode = m.underHelp, nil, modePager
	}
}

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

// mergeRecent puts current first, then the remembered namespaces.
func mergeRecent(current string, remembered []string) []string {
	out := nonEmpty(current)
	for _, ns := range remembered {
		if ns != current && len(out) < maxRecentNS {
			out = append(out, ns)
		}
	}
	return out
}

// Notice shows a warning in the footer once the UI starts.
func (m *Model) Notice(text string) { m.setFlash("", errors.New(text)) }

// saveState remembers history and recent namespaces for the next run.
func (m *Model) saveState() {
	s := m.opts.State
	if s == nil {
		return
	}
	s.History = m.cmdHistory
	if len(m.recentNS) > 0 {
		s.RecentNamespaces[m.context] = m.recentNS
	}
	_ = s.Save()
}

// appendLogs adds a stream's lines to the open logs.
func (m *Model) appendLogs(lines []string) {
	m.pager.append(lines)
	if m.pager.readDropped {
		m.pager.readDropped = false
		m.setFlash(fmt.Sprintf("the lines you were reading were dropped: only the newest %d are kept", maxLogLines), nil)
	}
}

// endLogs ends the open logs with a note on how they ended: last is the
// stream's last lines, deleted that its pod was deleted.
func (m *Model) endLogs(last []string, deleted bool) {
	m.pager.ended = true
	end := styleMuted.Render("(no more lines: the log stream ended. r reads it again)")
	if m.pager.source.previous && !m.pager.noEarlier() {
		end = styleMuted.Render("(that's all from before the restart. l shows the live logs)")
	} else if m.pager.source.previous {
		end = "" // kubed's note already says there was no restart
	}
	if why := m.howItStopped(m.pager.source); why != "" {
		if m.waitingToRestart(m.pager.source) {
			why += "; it is waiting to restart (Kubernetes waits longer after each crash). r reads again once it runs"
		}
		end = styleWarn.Render("── end of the run: it " + why)
	}
	if lost(last) {
		end = styleWarn.Render("── the connection to the cluster dropped. r reconnects")
	}
	if deleted {
		end = styleWarn.Render("── the pod was deleted; these are its last lines")
	}
	if why := m.notStartedYet(m.pager.source); why != "" && m.pager.onlyNotes() {
		// The API's "is waiting to start" line says less: this one replaces it.
		m.pager.lines, m.pager.other = nil, nil
		end, m.pager.notStarted = styleWarn.Render("── "+why), true
	}
	if end != "" {
		m.pager.note(end) // still following: lands on the last lines, where a crash is
	}
	m.pager.follow = false
}
