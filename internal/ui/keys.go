package ui

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/s3cretagent/kubed/internal/enrich"
	"github.com/s3cretagent/kubed/internal/kube"
	"github.com/s3cretagent/kubed/internal/view"
)

// aliases kubectl does not define but kubed users will type.
var aliases = map[string]string{
	"np":     "nodepools.karpenter.sh",
	"nc":     "nodeclaims.karpenter.sh",
	"es":     "externalsecrets.external-secrets.io",
	"route":  "httproutes.gateway.networking.k8s.io",
	"routes": "httproutes.gateway.networking.k8s.io",
	"argo":   "applications.argoproj.io",
	"argocd": "applications.argoproj.io",
	"hr":     "httproutes.gateway.networking.k8s.io",
	"events": "events",
}

func resolveAlias(name string) string {
	if a, ok := aliases[strings.ToLower(name)]; ok {
		return a
	}
	return name
}

func (m *Model) onKey(msg tea.KeyMsg) tea.Cmd {
	// Esc pressed twice quickly arrives as one "alt+esc": treat it as two.
	if msg.String() == "alt+esc" && m.mode != modeShell {
		esc := tea.KeyMsg{Type: tea.KeyEsc}
		return tea.Batch(m.onKey(esc), m.onKey(esc))
	}
	if m.mode == modeShell { // every key belongs to the shell except the leave key
		if msg.Type == leaveShellKey {
			m.closeShell()
			m.setFlash("closed the shell", nil)
			return nil
		}
		m.term.send(msg)
		return nil
	}
	// Keys typed quickly (or sent together) can arrive as one message with
	// several runes, e.g. "gN". Outside text inputs, treat each as its own
	// key press so shortcuts like g-jumps still work.
	// Fast typing can arrive as one burst of letters; take them one at a
	// time, so "delete" typed into a prompt is text, not the Delete key.
	if msg.Type == tea.KeyRunes && len(msg.Runes) > 1 && !msg.Paste {
		var cmds []tea.Cmd
		for _, r := range msg.Runes {
			cmds = append(cmds, m.onKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}}))
		}
		return tea.Batch(cmds...)
	}
	if msg.String() == "ctrl+c" && m.mode != modePrompt {
		return m.quit()
	}
	switch m.mode {
	case modePrompt:
		p := m.prompt
		cmd, done := p.update(msg)
		// A prompt's callback may open the next prompt (scale → confirm);
		// only close the prompt if it is still the one that finished.
		if done && m.prompt == p {
			m.prompt, m.mode = nil, m.modeBehindPrompt()
			if p.backTo != nil && msg.String() == "esc" { // back to the screen it was opened from
				return p.backTo()
			}
			// Leaving a search result's menu with esc goes back to the
			// results; picking from it stays there.
			// An app's action on its workload: back to the apps when done.
			// Not while its result is still loading (X, YAML): that would
			// leave before it arrives, and it would be thrown away.
			if m.loc.via && m.pager == nil && m.mode != modePager && m.busy == "" {
				m.back()
			} else if p.foundMenu && msg.String() == "esc" && m.loc.fromFind && len(m.history) > 0 {
				m.back() // as the guide says: esc brings you back to the results
			}
		}
		return cmd
	case modeFind:
		return m.onFindKey(msg)
	case modeCommand, modeFilter, modeSearch:
		return m.onInputKey(msg)
	case modePager:
		return m.onPagerKey(msg)
	}
	return m.onTableKey(msg)
}

func (m *Model) onPagerKey(msg tea.KeyMsg) tea.Cmd {
	p := m.pager
	if m.gKey { // g then a letter jumps from here too
		m.gKey = false
		if msg.String() == "g" { // gg: the top, as in less and vim
			p.vp.GotoTop()
			p.follow = false
			return nil
		}
		if msg.String() == "esc" || m.client == nil {
			return nil
		}
		if !isJump(msg.String()) { // a mistyped jump stays here
			m.setFlash("", fmt.Errorf("no jump for g%s (? lists them; gg goes to the top)", msg.String()))
			return nil
		}
		if m.underHelp != nil { // leaving for another screen: the logs under help close too
			m.underHelp.close()
			m.underHelp = nil
		}
		m.loadPending = nil
		m.closePager()
		return m.jump(msg.String())
	}
	switch msg.String() {
	case "g":
		m.gKey = true
		return nil
	case ":": // a command works from here too, as the guide says: leave for it
		m.loadPending = nil // leaving for elsewhere: what waited behind help is dropped
		if m.underHelp != nil {
			m.underHelp.close()
			m.underHelp = nil
		}
		m.closePager()
		m.startInput(modeCommand, ":", "")
		m.histPos = len(m.cmdHistory)
		m.sugs = m.suggest("")
		return nil
	case "esc", "q":
		if p.search != "" && msg.String() == "esc" {
			p.setSearch("")
			return nil
		}
		m.closePager()
		if p.title == "help" && m.loadPending != nil { // something waited behind help
			return m.runPending()
		}
		if p.back != nil && msg.String() == "esc" { // back to the menu it came from
			return p.back()
		}
		// An app's action on its workload: back to the apps. From a find
		// result, stay on the list you opened it from (esc again: results).
		if m.loc.via {
			m.back()
		}
	case "/":
		m.startInput(modeSearch, "/", "") // a new search; esc keeps the old one
	case "n", "N":
		if p.search == "" {
			m.setFlash("", fmt.Errorf("nothing searched yet: / searches"))
			return nil
		}
		if msg.String() == "n" {
			p.nextMatch(1)
		} else {
			p.nextMatch(-1)
		}
	case "e":
		if p.isLogs {
			p.keepPlace(func() { p.errorsOnly = !p.errorsOnly })
			if p.search != "" {
				p.setSearch(p.search) // matches are positions on screen
			}
		} else if p.editRow != nil {
			if m.writesLocked() {
				m.setFlash("", m.lockedReason())
				return nil
			}
			row := *p.editRow
			return m.edit(row)
		}
	case "t":
		if p.isLogs {
			p.toggleTimes() // shown or hidden, not read again: you stay where you are
			return nil
		}
	case "?":
		if m.underHelp == nil && p.title != "help" {
			m.openHelp()
		}
	case "r":
		if p.isLogs { // read the stream again (after a dropped connection, or to start over)
			src := p.source
			if m.podDeleted(src) { // nothing to read again: the lines read stay
				m.setFlash("", fmt.Errorf("pod %s no longer exists, so its logs can't be read again", src.pod))
				return nil
			}
			if src.pods != nil { // a workload: the pods it has now, not the ones it had
				if streams, all := src.pods(); len(streams) > 0 {
					src.streams, src.allPods = streams, all
				}
			}
			m.keepPlace = true
			return m.openLogs(src)
		}
	case "l":
		if p.isLogs && (p.source.previous || p.source.stoppedRun) { // from a run that ended to the live one
			src := p.source
			src.previous, src.stoppedRun = false, false
			return m.openLogs(src)
		}
	case "h":
		if p.isLogs && !p.source.previous { // further back: 1h, 6h, 24h, then the last lines again
			src := p.source
			src.since = nextSince(src.since)
			m.keepPlace = true
			return m.openLogs(src)
		}
	case "v":
		if p.versions != nil {
			return p.versions()
		}
	case "u":
		if p.undo != nil {
			return p.undo()
		}
	case "j":
		if p.isLogs { // JSON lines tidied, or as they are
			p.keepPlace(func() { p.raw = !p.raw })
		} else { // elsewhere j moves down, as in lists (k needs nothing: the view moves up on it)
			p.vp.ScrollDown(1)
		}
	case "p":
		switch {
		case !p.isLogs:
		case p.source.workload != "" && len(p.source.streams) == 1: // one pod: its run that ended
			one := p.source.streams[0]
			if err := m.podGoneErr(p.source.ns, one[0]); err != nil {
				m.setFlash("", err)
				return nil
			}
			return m.openLogs(m.endedRun(logSource{ns: p.source.ns, pod: one[0], container: one[1], timestamps: p.source.timestamps}))
		case p.source.workload != "" && podCount(p.source.streams) == 1:
			m.setFlash("", fmt.Errorf("its pod runs several containers: open the pod, pick one, then press p"))
		case p.source.workload != "":
			m.setFlash("", fmt.Errorf("these are several pods' logs: open one pod, then press p"))
		case !p.source.previous:
			src := p.source
			src.since = 0 // the run that ended may be older than the window
			// A pod that's gone has no earlier logs: the lines read stay.
			if err := m.podGoneErr(src.ns, src.pod); err != nil {
				m.setFlash("", err)
				return nil
			}
			if src = m.endedRun(src); !src.previous && p.source.since == 0 {
				if m.restarts(src) { // it comes back: say so
					m.setFlash("this is the run that ended: the container is stopped, waiting to restart", nil)
				} else { // a finished job's pod: nothing starts it again
					m.setFlash("this is its only run: it finished and won't run again, so there are no earlier logs", nil)
				}
				return nil
			}
			if pod, ok := m.client.Cache.Get(kube.Pods, src.ns, src.pod); ok && src.previous && !earlierRun(pod, src.container) {
				// Running, or a setup step that finished as it should: the API
				// keeps no run before this one.
				if view.StoppedNow(pod, src.container) {
					m.setFlash("this is its only run: it finished its setup work and won't run again, so there are no earlier logs", nil)
				} else {
					m.setFlash("", fmt.Errorf("%s hasn't restarted, so there are no earlier logs", src.container))
				}
				return nil
			}
			return m.openLogs(src)
		}
	case "ctrl+s":
		if p.secret { // a file in Downloads outlives the screen (and may sync to the cloud)
			m.setFlash("", fmt.Errorf("secret values aren't saved to files. To take one value: / to find it, then c copies that line"))
			return nil
		}
		m.savePager()
	case "c":
		// The line you searched for, else what's on screen; C copies it all.
		if p.search != "" && len(p.matches) > 0 {
			vis, i := p.visible(), p.matches[p.match]
			if p.secret { // the value itself, ready to paste: no indent, and the value when you found its key
				line := strings.TrimSpace(p.lineText(vis[i]))
				if strings.HasSuffix(line, ":") && i+1 < len(vis) {
					return m.copy(strings.TrimSpace(p.lineText(vis[i+1])), "the value of "+strings.TrimSuffix(line, ":"))
				}
				return m.copy(line, "the matching line")
			}
			if line, ok := p.plainLine(i); ok { // whole names and a real time, for a chat
				return m.copy(line, "the matching line")
			}
			return m.copy(p.lineText(vis[i]), "the matching line")
		}
		if p.secret { // one value at a time, never the whole screen of them
			m.setFlash("", fmt.Errorf("/ to find the value you want first; then c copies just that line"))
			return nil
		}
		// The whole lines on screen, not as cut at its edge.
		var lines []string
		vis := p.visible()
		first, last := p.lineAt(p.vp.YOffset), p.lineAt(p.vp.YOffset+p.vp.Height-1)
		for i := max(first, 0); i <= last && i < len(vis); i++ {
			if !p.isNote(vis[i]) { // kubed's own notes aren't the app's lines
				lines = append(lines, strings.TrimRight(ansi.Strip(vis[i]), " "))
			}
		}
		text := strings.TrimRight(strings.Join(lines, "\n"), "\n")
		if strings.TrimSpace(text) == "" { // your clipboard keeps what it had
			m.setFlash("", fmt.Errorf("nothing to copy: no lines are shown"))
			return nil
		}
		return m.copy(text, "what's on screen · C copies all")
	case "C":
		if p.secret {
			m.setFlash("", fmt.Errorf("secret values aren't copied all at once. To take one value: / to find it, then c copies that line"))
			return nil
		}
		what := "all of it"
		if p.isLogs { // not the container's whole history: what kubed read of it
			what = "every line here (" + p.heldWords() + ")"
		}
		if p.errorsOnly {
			what = "every error line here"
			if p.isLogs {
				what += " (" + p.heldWords() + ")"
			}
		}
		text := p.copyText()
		if strings.TrimSpace(text) == "" { // your clipboard keeps what it had
			m.setFlash("", fmt.Errorf("nothing to copy: no lines are shown"))
			return nil
		}
		return m.copy(text, what)
	default:
		return p.update(msg)
	}
	return nil
}

// onFindKey: type to search, ↑/↓ pick, enter opens, esc goes back.
func (m *Model) onFindKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "esc":
		m.mode = modeTable
		m.back()
		return nil
	case "up", "ctrl+p":
		m.tbl.move(-1)
		return nil
	case "down", "ctrl+n":
		m.tbl.move(1)
		return nil
	case "enter":
		row, ok := m.tbl.selected()
		m.mode = modeTable
		if !ok {
			return nil
		}
		return m.openFound(row)
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	m.loc.query = m.input.Value() // the search, kept apart from a filter typed later
	m.tbl.jump(0)
	m.tbl.offset, m.tbl.touched, m.tbl.want = 0, false, "" // a new search starts at its top
	m.tbl.openedAt = time.Now()                            // and stays there: the last search's top row does not pull the cursor
	m.refreshRows()
	return cmd
}

func (m *Model) modeBehindPrompt() mode {
	if m.pager != nil {
		return modePager
	}
	return modeTable
}

func (m *Model) quit() tea.Cmd {
	m.saveState()
	m.closeShell()
	m.forwards.StopAll()
	if m.client != nil {
		m.client.Close()
	}
	return tea.Quit
}

func (m *Model) startInput(md mode, prompt, value string) {
	m.mode = md
	m.input = newInput(value)
	m.input.Prompt = prompt
	m.sugs, m.sugSel = nil, -1
	m.inputFresh = md == modeFilter && value != "" // the old filter shows; typing starts a new one
}

func (m *Model) onInputKey(msg tea.KeyMsg) tea.Cmd {
	if m.inputFresh {
		m.inputFresh = false
		if msg.Type == tea.KeyRunes || msg.Type == tea.KeySpace { // a new filter, not the old one plus this
			m.input.SetValue("")
		}
	}
	switch msg.String() {
	case "esc":
		switch m.mode {
		case modeFilter:
			m.tbl.filter = ""
			m.refreshRows()
		case modeSearch:
			m.mode = modePager
			return nil
		}
		m.mode = modeTable
		return nil
	case "enter":
		value := strings.TrimSpace(m.input.Value())
		if m.mode == modeSearch {
			m.mode = modePager
			m.pager.setSearch(value)
			return nil
		}
		if m.mode == modeCommand && m.sugSel >= 0 && m.sugSel < len(m.sugs) {
			value = m.sugs[m.sugSel].name // picked with ↑/↓
		} else if m.mode == modeCommand && value != "" && !strings.Contains(value, " ") && len(m.sugs) > 0 && !m.knownCommand(value) {
			value = m.sugs[0].name // "cron" runs the first suggestion, cronjobs
		} else if first, rest, ok := strings.Cut(value, " "); m.mode == modeCommand && ok && !m.knownCommand(first) && !ownWords[first] {
			// Only to a kind, or to what the word is a synonym of: never by
			// its first letters to one of kubed's own commands (":c prod"
			// is not ":ctx prod", another cluster).
			if sugs := m.suggest(first); len(sugs) > 0 && (synonyms[first] != "" || !ownWords[sugs[0].name] && m.isKind(sugs[0].name)) {
				value = sugs[0].name + " " + rest // "cron -A" as "cron": cronjobs, then -A
			}
		}
		wasCommand := m.mode == modeCommand
		m.mode, m.sugs = modeTable, nil
		if wasCommand {
			return m.runCommand(value)
		}
		return nil
	case "ctrl+a":
		if m.mode == modeCommand { // the list of every kind, from the : prompt too
			m.mode = modeTable
			if m.client != nil {
				m.jumpTo(location{kind: locResources})
			}
			return nil
		}
	case "tab", "up", "down":
		// ↑/↓ browse history until you type; then they pick suggestions.
		if m.mode == modeCommand && msg.String() != "tab" && (m.input.Value() == "" || m.browsingHistory()) {
			m.browseHistory(msg.String())
			return nil
		}
		if m.mode == modeCommand && len(m.sugs) > 0 {
			m.pickSuggestion(msg.String())
			return nil
		}
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	if m.mode == modeCommand {
		m.sugs, m.sugSel = m.suggest(m.input.Value()), -1
	}
	if m.mode == modeFilter { // filter as you type
		m.tbl.filter = m.input.Value()
		m.tbl.jump(0)
		m.refreshRows()
	}
	return cmd
}

// knownCommand: the word runs something as typed (a command or a kind).
func (m *Model) knownCommand(word string) bool {
	for _, c := range commands {
		if c.name == word {
			return true
		}
	}
	switch word {
	case "quit", "exit", "q!", "all", "pf", "ctx", "ns", "apps", "app", "mouse":
		return true
	}
	if m.client == nil {
		return false
	}
	_, ok := m.client.Resources.Lookup(resolveAlias(word))
	return ok
}

// ownWords are the words runCommand acts on itself, whatever they also
// start (":context x" is never completed to another word).
var ownWords = map[string]bool{
	"q": true, "q!": true, "quit": true, "exit": true, "ctx": true, "context": true, "contexts": true,
	"pf": true, "portforwards": true, "mouse": true, "home": true, "apps": true, "app": true,
	"shipped": true, "deploys": true, "releases-this-week": true, "changes": true, "changed": true,
	"what-changed": true, "waste": true, "rightsize": true, "helm": true, "can-i": true, "cani": true,
	"whoami": true, "ns": true, "namespace": true, "namespaces": true, "help": true,
}

// runCommand handles ":" commands.
func (m *Model) runCommand(line string) tea.Cmd {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	m.remember(line)
	name, args := fields[0], fields[1:]
	if (name == "urls" || name == "hosts") && m.routesFirst() { // as the suggestions say
		name = "httproutes"
	}
	switch name {
	case "q", "q!", "quit", "exit":
		return m.quit()
	case "ctx", "context", "contexts":
		if len(args) > 1 { // one cluster: refuse the rest, change nothing
			m.setFlash("", fmt.Errorf(":ctx takes one cluster; kubed doesn't know %q here", strings.Join(args[1:], " ")))
			return nil
		}
		if len(args) > 0 && args[0] == "-" { // the cluster you were on before
			if m.prevContext == "" {
				m.setFlash("", fmt.Errorf("no earlier cluster in this session"))
				return nil
			}
			return m.switchContext(m.prevContext)
		}
		if len(args) > 0 {
			name, ok := m.resolveContext(args[0])
			if !ok {
				m.setFlash("", fmt.Errorf("there's no cluster called %q. :ctx lists them", args[0]))
				return nil
			}
			return m.switchContext(name)
		}
		m.navigate(location{kind: locContexts}, true) // esc comes back
		return nil
	case "pf", "portforwards":
		m.jumpTo(location{kind: locForwards})
		return nil
	case "mouse":
		// Switch between clicking rows (kubed has the mouse) and selecting
		// text to copy (the terminal has it).
		m.opts.Mouse = !m.opts.Mouse
		if m.opts.Mouse {
			m.setTip("mouse on: click a row to select it, again for its menu. To select text, hold Option (Mac) or Shift. :mouse turns it off")
			return tea.EnableMouseCellMotion
		}
		m.setTip("mouse off: select text with the mouse to copy it. :mouse turns clicking rows back on")
		return tea.DisableMouse
	case "home", "apps", "app": // the home screen is "apps"; ArgoCD's list is :argo
		// -n, -A or a namespace, as for other lists: for this screen.
		if err := flagWithoutValue(args, false); err != nil {
			m.setFlash("", err)
			return nil
		}
		ns, given := listNamespaceArgs(args)
		switch {
		case given && ns != "" && m.client != nil && !m.namespaceExists(ns):
			m.setFlash("", fmt.Errorf("there is no namespace called %q. Type :ns and pick one from the list", ns))
			return nil
		case given:
			m.enterNamespace(ns)
		default:
			m.backToChosenNS()
		}
		m.jumpTo(location{kind: locHome})
		return nil
	case "shipped", "deploys", "releases-this-week":
		if m.client != nil {
			return m.shipped()
		}
		return nil
	case "changes", "changed", "what-changed":
		if m.client != nil {
			return m.changesIn(m.namespace, "", nsWords(m.namespace), nil)
		}
		return nil
	case "waste", "rightsize":
		if m.client != nil {
			m.jumpTo(location{kind: locWaste})
		}
		return nil
	case "helm":
		if m.client == nil {
			return nil
		}
		// -n, -A or a namespace, as for other lists: for this list only.
		if err := flagWithoutValue(args, false); err != nil {
			m.setFlash("", err)
			return nil
		}
		ns, given := listNamespaceArgs(args)
		switch {
		case given && ns != "" && !m.namespaceExists(ns):
			m.setFlash("", fmt.Errorf("there is no namespace called %q. Type :ns and pick one from the list", ns))
			return nil
		case given:
			m.enterNamespace(ns)
		default:
			m.backToChosenNS()
		}
		return m.openHelm()
	case "can-i", "cani", "whoami":
		if m.client != nil {
			return m.canI()
		}
		return nil
	case "ns", "namespace", "namespaces":
		if len(args) > 1 { // one namespace: refuse the rest, change nothing
			m.setFlash("", fmt.Errorf(":ns takes one namespace; kubed doesn't know %q here", strings.Join(args[1:], " ")))
			return nil
		}
		if len(args) > 0 && args[0] == "-" { // the namespace you had before
			if !m.hadPrevNS {
				m.setFlash("", fmt.Errorf("no earlier namespace in this session"))
				return nil
			}
			m.setNamespace(nsArg(m.prevNS))
			return nil
		}
		if len(args) > 0 {
			if !m.namespaceExists(args[0]) {
				m.setFlash("", fmt.Errorf("there is no namespace called %q. Type :ns and pick one from the list", args[0]))
				return nil
			}
			m.setNamespace(args[0])
			return nil
		}
	case "help":
		m.openHelp()
		return nil
	}
	if m.client == nil {
		return nil
	}
	// As kubectl and k9s: ":pods payment", ":pods -n payment", ":pods -A",
	// ":pods -l app=web" (a label filter, not a namespace).
	if len(name) > 1 && strings.HasPrefix(name, "-") { // ":-l app=web": no kind given
		m.setFlash("", fmt.Errorf("give a kind first, as :pods %s", strings.Join(fields, " ")))
		return nil
	}
	if err := flagWithoutValue(args, true); err != nil {
		m.setFlash("", err)
		return nil
	}
	ns, selector, nsGiven := "", "", false
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "-A" || a == "--all-namespaces":
			ns, nsGiven = "", true
		case (a == "-n" || a == "--namespace") && i+1 < len(args):
			i++
			ns, nsGiven = args[i], true
		case (a == "-l" || a == "--selector") && i+1 < len(args):
			i++
			selector = args[i]
		case strings.HasPrefix(a, "-l") && len(a) > 2:
			selector = a[2:]
		case strings.HasPrefix(a, "-"):
			m.setFlash("", fmt.Errorf("kubed doesn't know %q here; :<kind> takes a namespace, -n, -A or -l", a))
			return nil
		default:
			ns, nsGiven = a, true
		}
	}
	if nsGiven && ns != "" && !m.namespaceExists(ns) {
		m.setFlash("", fmt.Errorf("there is no namespace called %q. Type :ns and pick one from the list", ns))
		return nil
	}
	res, ok := m.findKind(name) // a word that names nothing changes nothing
	if !ok {
		return nil
	}
	if nsGiven { // as kubectl: for this list only, not the namespace you chose
		m.enterNamespace(ns)
	} else {
		m.backToChosenNS()
	}
	// The namespace picker comes back to the list you were on.
	m.openKind(res, name == "ns" || name == "namespace" || name == "namespaces")
	if selector != "" {
		m.tbl.filter = "-l " + selector
		m.refreshRows()
	}
	return nil
}

// maxHistory bounds ":" history.
const maxHistory = 50

func (m *Model) remember(line string) {
	if n := len(m.cmdHistory); n > 0 && m.cmdHistory[n-1] == line {
		return
	}
	m.cmdHistory = append(m.cmdHistory, line)
	if len(m.cmdHistory) > maxHistory {
		m.cmdHistory = m.cmdHistory[1:]
	}
	m.saveState()
}

// browseHistory moves through ":" history with ↑ (older) / ↓ (newer).
func (m *Model) browseHistory(key string) {
	if key == "up" && m.histPos > 0 {
		m.histPos--
	}
	if key == "down" && m.histPos < len(m.cmdHistory) {
		m.histPos++
	}
	value := ""
	if m.histPos < len(m.cmdHistory) {
		value = m.cmdHistory[m.histPos]
	}
	m.input.SetValue(value)
	m.input.CursorEnd()
	m.sugs, m.sugSel = nil, -1
}

// maxRecentNS is how many namespaces get a digit shortcut (1-9).
const maxRecentNS = 9

// resolveContext finds a kubeconfig context by its full name or by the
// short name kubed shows ("acme-staging" for an EKS ARN).
func (m *Model) resolveContext(name string) (string, bool) {
	if m.kc == nil {
		return name, true
	}
	names, _ := m.kc.Contexts()
	return matchContext(names, name)
}

// matchContext finds a kubeconfig context by its full name, the short name
// kubed shows ("acme-staging"), or a part only one of them has
// ("staging").
func matchContext(names []string, name string) (string, bool) {
	for _, n := range names {
		if n == name {
			return n, true
		}
	}
	for _, n := range names {
		short := shortContext(n)
		if short == name || strings.TrimSuffix(short, "-context") == name {
			return n, true
		}
	}
	var found []string
	for _, n := range names {
		if strings.Contains(shortContext(n), name) {
			found = append(found, n)
		}
	}
	if len(found) == 1 {
		return found[0], true
	}
	return "", false
}

// namespaceExists checks a typed namespace. When kubed can't tell (you may
// not be allowed to list namespaces), it trusts you.
func (m *Model) namespaceExists(ns string) bool {
	if ns == "all" || ns == "-A" || ns == "" || m.client == nil {
		return true
	}
	if gvr, ok := m.client.Resources.Served(kube.Namespaces); ok && m.client.Cache.Synced(gvr) {
		_, found := m.client.Cache.Get(gvr, "", ns)
		return found
	}
	ctx, cancel := apiCtx()
	defer cancel()
	_, err := m.client.Typed.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	return err == nil || !apierrors.IsNotFound(err)
}

// setNamespace is the namespace you chose (:ns, 0-9): this screen shows it.
func (m *Model) setNamespace(ns string) {
	if ns == "all" || ns == "-A" {
		ns = ""
	}
	if ns != m.chosenNS {
		m.prevNS, m.hadPrevNS = m.chosenNS, true
	}
	m.switchNamespace(ns)
	m.loc.ns = m.namespace
	m.chosenNS = m.namespace
}

// backToChosenNS: opening an app or a search result narrows to its
// namespace for that screen only. A list opened with : or g shows the
// namespace you chose again.
func (m *Model) backToChosenNS() {
	if m.namespace != m.chosenNS {
		m.switchNamespace(m.chosenNS)
	}
}

// enterNamespace moves to a namespace on the way into another screen: the
// screen you leave keeps its own, and esc goes back to it.
func (m *Model) enterNamespace(ns string) {
	cursor := m.tbl.cursor // the screen you leave remembers where you were
	m.switchNamespace(ns)
	m.tbl.jump(cursor)
}

func (m *Model) switchNamespace(ns string) {
	if ns == "all" || ns == "-A" {
		ns = ""
	}
	m.namespace = ns
	if m.client != nil {
		m.client.Cache.SetNamespace(ns)
	}
	if ns != "" {
		recent := []string{ns}
		for _, r := range m.recentNS {
			if r != ns && len(recent) < maxRecentNS {
				recent = append(recent, r)
			}
		}
		m.recentNS = recent
		m.saveState()
	}
	m.tbl.jump(0)
	m.refreshRows()
}

// logWindows are how far back h reads logs, in turn.
var logWindows = []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}

func nextSince(cur time.Duration) time.Duration {
	for _, w := range logWindows {
		if w > cur {
			return w
		}
	}
	return 0
}

// browsingHistory: the input shows a history entry the user has not edited.
func (m *Model) browsingHistory() bool {
	return m.histPos < len(m.cmdHistory) && m.input.Value() == m.cmdHistory[m.histPos]
}

// pickSuggestion moves through completions; tab also writes the pick
// into the input so you can keep typing (e.g. a namespace).
func (m *Model) pickSuggestion(key string) {
	n := len(m.sugs)
	switch key {
	case "up":
		m.sugSel = (m.sugSel - 1 + n) % n
	default: // tab, down
		m.sugSel = (m.sugSel + 1) % n
	}
	if key == "tab" {
		m.input.SetValue(m.sugs[m.sugSel].name)
		m.input.CursorEnd()
	}
}

// isJump: g followed by this key goes somewhere.
func isJump(key string) bool {
	switch key {
	case "g", "h", "H":
		return true
	}
	_, ok := jumps[key]
	return ok
}

// jump handles the key after "g". Jumps start a fresh breadcrumb trail.
func (m *Model) jump(key string) tea.Cmd {
	switch key {
	case "g":
		m.tbl.jump(0)
		return nil
	case "h":
		m.backToChosenNS()
		m.jumpTo(location{kind: locHome})
		return nil
	case "H":
		m.backToChosenNS() // as every other jump: the namespace you chose
		return m.openHelm()
	case "esc":
		return nil // changed your mind
	}
	target, ok := jumps[key]
	if !ok {
		m.setFlash("", fmt.Errorf("no jump for g%s (? lists them)", key))
		return nil
	}
	if key == "i" && m.routesFirst() { // web addresses: what this cluster mostly uses, as :urls
		target = "httproutes"
	}
	if _, served := m.client.Resources.Lookup(target); !served {
		m.setFlash("", fmt.Errorf("%s", notInstalled(target)))
		return nil
	}
	m.backToChosenNS()
	m.openResource(target, false)
	return nil
}

func (m *Model) onTableKey(msg tea.KeyMsg) tea.Cmd {
	if cmd, handled := m.onProblemKey(msg); handled {
		return cmd
	}
	if m.gKey {
		m.gKey = false
		if m.client != nil {
			return m.jump(msg.String())
		}
		return nil
	}
	switch msg.String() {
	case "up", "k":
		m.tbl.move(-1)
	case "down", "j":
		m.tbl.move(1)
	case "pgup", "ctrl+b":
		m.tbl.move(-m.pageSize())
	case "pgdown", "ctrl+f":
		m.tbl.move(m.pageSize())
	case "g":
		m.gKey = true
	case "home":
		m.tbl.jump(0)
	case "G", "end":
		m.tbl.jump(len(m.tbl.rows) - 1)
	case "q":
		// Open ports close with kubed: say so once, then q again quits.
		if n := len(m.forwards.List()) + len(m.moving); n > 0 && !m.quitArmed {
			m.quitArmed = true
			closes := "them"
			if n == 1 {
				closes = "it"
			}
			m.setFlash(fmt.Sprintf("%s open; quitting closes %s. q again to quit", view.Plural(n, "local port"), closes), nil)
			m.flashTip = true // a question, not a success
			return nil
		}
		return m.quit()
	case ":":
		m.startInput(modeCommand, ":", "")
		m.histPos = len(m.cmdHistory)
		m.sugs = m.suggest("") // what you can type, right away
	case "/":
		m.startInput(modeFilter, "/", m.tbl.filter)
	case "f":
		m.startFind()
	case " ", "*":
		if m.loc.kind != locResource && m.loc.kind != locHome && m.loc.kind != locForwards { // nothing to do with several clusters or results at once
			m.setFlash("", fmt.Errorf("rows can't be picked on this screen; keys here act on the row you're on"))
			return nil
		}
		if msg.String() == "*" {
			m.tbl.markAll()
			m.refreshRows()
			return nil
		}
		m.tbl.toggleMark()
		m.tbl.move(1)
		m.refreshRows()
	case "esc":
		switch {
		case m.loc.fromFind && len(m.history) > 0:
			m.back() // straight back to the search results
		case m.tbl.filter != "":
			m.tbl.filter = ""
			m.tbl.touched = true // stay on the row you were on
			m.refreshRows()
		case len(m.tbl.marks) > 0:
			m.tbl.clearMarks()
			m.refreshRows()
		case len(m.history) == 0 && m.loc.kind == locHome && m.client != nil && m.namespace != m.chosenNS:
			// Home in a namespace "for this screen only": esc goes back to the
			// one you chose, as the header says.
			m.backToChosenNS()
			m.jumpTo(location{kind: locHome})
		case len(m.history) == 0 && m.loc.kind != locHome && m.loc.kind != locContexts && m.client != nil:
			// A screen you jumped to (:waste, gp…): esc goes home.
			m.backToChosenNS()
			m.jumpTo(location{kind: locHome})
		default:
			m.back()
		}
	case "?":
		m.openHelp()
	case "ctrl+a":
		if m.client != nil {
			m.jumpTo(location{kind: locResources})
		}
	case "0":
		m.setNamespace("")
	case "1", "2", "3", "4", "5", "6", "7", "8", "9":
		if i := int(msg.String()[0] - '1'); i < len(m.recentNS) {
			if ns := m.recentNS[i]; ns != "" && !m.namespaceExists(ns) { // deleted since
				m.recentNS = append(m.recentNS[:i:i], m.recentNS[i+1:]...)
				m.setFlash("", fmt.Errorf("namespace %s no longer exists; it is off the recent list", ns))
				return nil
			}
			m.setNamespace(m.recentNS[i])
		} else if m.client != nil && m.loc.kind != locContexts { // the cluster picker has no namespaces
			switch n := len(m.recentNS); n {
			case 0:
				m.setFlash("", fmt.Errorf("no namespace on key %s yet: :ns picks one (0 is all)", msg.String()))
			case 1:
				m.setFlash("", fmt.Errorf("no namespace on key %s yet: 1 is your recent one (0 is all)", msg.String()))
			default:
				m.setFlash("", fmt.Errorf("no namespace on key %s yet: 1-%d are your recent ones (0 is all)", msg.String(), n))
			}
		}
	case "N":
		m.toggleSort(sortName)
	case "A":
		if m.tbl.timeOf == nil && sortColumn(m.tbl.header, sortAge) < 0 { // :waste, the namespaces' picks…
			m.setFlash("", fmt.Errorf("this list has no ages to sort by"))
			return nil
		}
		m.toggleSort(sortAge)
	case "ctrl+s":
		if len(m.tbl.rows) == 0 {
			m.setFlash("", fmt.Errorf("nothing to save: the list is empty"))
			return nil
		}
		m.saveList()
	case "$":
		if costColumn(m.tbl.header) < 0 {
			m.setFlash("", fmt.Errorf("this list has no costs to sort by"))
			return nil
		}
		m.toggleSort(sortCost)
	case "C", "M": // as in k9s: CPU, memory
		mode, what := sortCPU, "CPU"
		if msg.String() == "M" {
			mode, what = sortMem, "memory"
		}
		if sortColumn(m.tbl.header, mode) < 0 {
			m.setFlash("", fmt.Errorf("this list shows no %s use to sort by (pods and nodes do)", what))
			return nil
		}
		m.toggleSort(mode)
	case "R":
		if sortColumn(m.tbl.header, sortRestarts) < 0 {
			m.setFlash("", fmt.Errorf("this list has no restarts to sort by"))
			return nil
		}
		m.toggleSort(sortRestarts)
	case "enter":
		return m.onEnter()
	default:
		key := msg.String()
		if key == "-" && m.scalesHere() { // no key of its own: where + scales, say so
			m.setTip("+ scales it: type the new count (lower to scale down)")
			return nil
		}
		if _, ok := m.actionFor(key); !ok {
			// A key that does something elsewhere: say so, instead of
			// silently doing nothing (+ on a daemonset).
			for _, a := range actions {
				if a.key != key {
					continue
				}
				switch {
				case onResource(m):
					m.setFlash("", fmt.Errorf("%s (%s) doesn't work on %s. enter shows what does", displayKey(key), strings.ToLower(a.label), m.loc.res.GVR.Resource))
					return nil
				case m.loc.kind == locFind:
					m.setFlash("", fmt.Errorf("%s (%s) doesn't work on search results: enter opens the result, where its own keys are listed", displayKey(key), strings.ToLower(a.label)))
					return nil
				case m.loc.kind == locHome:
					where := "its pods or workloads, not on a whole app (enter opens the app, where its own keys are listed)" // ctrl-d…
					switch key {
					case "ctrl+k", "s", "S":
						where = "its pods, not on a whole app (enter opens the app, then its pods)"
					case "c", "D":
						where = "nodes: gn opens them"
					case "t", "z":
						where = "cron jobs: gC opens them"
					case "x":
						where = "secrets and config maps: gS or gc opens them"
					}
					label := strings.ToLower(a.label)
					if key == "z" {
						label = "pause or resume" // an app's paused cron jobs say z resumes them
					}
					m.setFlash("", fmt.Errorf("%s (%s) works on %s", displayKey(key), label, where))
					return nil
				}
			}
		}
		return m.runAction(key)
	}
	return nil
}

// scalesHere: + would scale the cursor's row, as the menu offers it.
func (m *Model) scalesHere() bool {
	a, ok := m.actionFor("+")
	if !ok || m.writesLocked() {
		return false
	}
	row, ok := m.tbl.selected()
	if !ok || m.othersPicked() {
		return false
	}
	if a.item != nil {
		it := menuItem{label: a.label}
		a.item(m, row, &it)
		return !it.disabled && !it.hidden
	}
	return true
}

func (m *Model) pageSize() int { return max(m.bodyHeight()-m.detailCap()-4, 1) }

func (m *Model) toggleSort(s sortMode) {
	if m.tbl.sort == s {
		m.tbl.desc = !m.tbl.desc
	} else {
		m.tbl.sort, m.tbl.desc = s, false
	}
	m.tbl.jump(0)
	m.tbl.offset, m.tbl.touched = 0, false // a new order starts at its top
	m.tbl.openedAt = time.Now()            // and stays there: the old row does not pull the cursor back
	m.refreshRows()
}

// onEnter opens the Enter menu. Pick-lists (contexts, the resource
// picker, port-forwards, find results) act straight away instead.
func (m *Model) onEnter() tea.Cmd {
	row, ok := m.tbl.selected()
	if !ok {
		return nil
	}
	switch m.loc.kind {
	case locContexts, locResources, locFind:
		if _, run, ok := m.defaultOpen(row); ok {
			return run()
		}
		return nil
	}
	if isNamespaces(m) && !m.othersPicked() { // :ns: enter switches, as you'd expect from a picker (with others picked, the menu says why not)
		if _, run, ok := m.defaultOpen(row); ok {
			return run()
		}
	}
	return m.menu()
}

// defaultOpen is what "open" means for a row: drill down, or the most
// useful detail for that kind of row. It is the first item of the menu.
func (m *Model) defaultOpen(row view.Row) (string, func() tea.Cmd, bool) {
	do := func(f func()) func() tea.Cmd { return func() tea.Cmd { f(); return nil } }
	switch m.loc.kind {
	case locContexts:
		return "Switch to this context", func() tea.Cmd { return m.switchContext(row.Obj.GetName()) }, true
	case locResources:
		return "Open", do(func() { m.openResource(row.Obj.GetName(), false) }), true
	case locFind:
		return "Open", func() tea.Cmd { return m.openFound(row) }, true
	case locHelm:
		return "Values", func() tea.Cmd { return m.helmShow(row, "values") }, true
	case locWaste:
		return "Open workload", func() tea.Cmd { return m.openWorkload(row.Obj) }, true
	case locHome:
		return "Open app", func() tea.Cmd { return m.openApp(row) }, true
	case locForwards, locContainers:
		return "", nil, false
	}
	gvr := m.loc.res.GVR
	switch {
	case gvr.Group == "" && gvr.Resource == kube.Namespaces.Resource:
		return "Show only this namespace", do(func() {
			m.setNamespace(row.Obj.GetName()) // every screen now shows just this namespace
			if len(m.history) > 0 {
				m.back()
				m.setNamespace(row.Obj.GetName())
			} else {
				m.jumpTo(location{kind: locHome})
			}
			m.setTip("every screen now shows namespace " + row.Obj.GetName() + " (:ns all for every namespace)")
		}), true
	case gvr.Group == "" && gvr.Resource == kube.Events.Resource:
		kind, _, _ := unstructured.NestedString(row.Obj.Object, "involvedObject", "kind")
		name, _, _ := unstructured.NestedString(row.Obj.Object, "involvedObject", "name")
		ns, _, _ := unstructured.NestedString(row.Obj.Object, "involvedObject", "namespace")
		apiVersion, _, _ := unstructured.NestedString(row.Obj.Object, "involvedObject", "apiVersion")
		gv, _ := schema.ParseGroupVersion(apiVersion)
		res, ok := m.client.Resources.ByKind(gv.Group, kind)
		if !ok {
			return "", nil, false
		}
		return "Open " + view.KindWords(strings.ToLower(kind)+" "+name), func() tea.Cmd { return m.openNamed(res.GVR, ns, name) }, true
	case gvr.Group == "" && gvr.Resource == kube.ServiceAccounts.Resource:
		return "What it is allowed to do", func() tea.Cmd { return m.serviceAccountRBAC(row) }, true
	}
	v := view.For(m.loc.res)
	if v.Has(view.CapContainers) {
		return "Containers", do(func() {
			m.navigate(location{kind: locContainers, podNS: row.Obj.GetNamespace(), podName: row.Obj.GetName()}, true)
		}), true
	}
	if v.Drill != nil {
		if d := v.Drill(row.Obj); d != nil {
			if res, ok := m.client.Resources.ByGVR(d.GVR); ok {
				label := "Pods"
				switch d.GVR.Resource {
				case kube.Nodes.Resource:
					label = "Nodes"
				case kube.Jobs.Resource:
					label = "Runs"
				}
				if m.loc.res.GVR.Resource == kube.Services.Resource {
					label = "Pods behind it"
				}
				return label, do(func() { m.navigate(location{kind: locResource, res: res, drill: d}, true) }), true
			}
		}
	}
	return "", nil, false
}

// confirm asks before a single-row write with a custom title (scale, edit).
func (m *Model) confirm(title string, row view.Row, action enrich.Action, run func() tea.Cmd) {
	var warnings []string
	if m.enr != nil {
		warnings = m.enr.Warnings(row.Obj, action)
	}
	m.confirmTitled(title, []view.Row{row}, warnings, run)
}

// ref names the object a row stands for. Rows of kubed's own lists (a
// pod's containers) resolve to the pod.
func (m *Model) ref(row view.Row) kube.Ref {
	res := m.loc.res
	switch {
	case m.loc.kind == locContainers || (res.GVR.Resource == "" && row.Obj.GetKind() == "Pod"):
		res, _ = m.client.Resources.ByGVR(kube.Pods)
	case res.GVR.Resource == "" || (res.Kind != "" && row.Obj.GetKind() != "" && res.Kind != row.Obj.GetKind()):
		// Acting from another screen (an app's workload from home): its
		// own kind, so it says "deployment web", not " web".
		gv, _ := schema.ParseGroupVersion(row.Obj.GetAPIVersion())
		if r, ok := m.client.Resources.ByKind(gv.Group, row.Obj.GetKind()); ok {
			res = r
		}
	}
	return kube.RefOf(res, row.Obj)
}

const helpText = `START HERE
  ↑ ↓     move                 enter   open, or "what can I do with this?"
  esc     go back              q       quit
  f       find anything by name: apps, pods, services, nodes, secrets…
  /       narrow this list as you type
  N A R   sort by name, age, restarts   C M  by CPU, memory   $  by cost
          (press again: the other way)
  mouse   select text to copy it as usual; :mouse lets you click rows
  ?       this help

WHEN SOMETHING IS WRONG
  Failing apps come first. The box under the list says why, and the fix.
  The Watch line at the top names the rest (cron jobs, secrets, nodes)
  with the g key that opens each (on a small screen: "look: gx gn").
  w       what changed in the last 24 hours
  l       logs (the keys inside them are under READING TEXT)
  p       logs from before the last restart
  E       events: what Kubernetes itself said about it
  d       details     y  its YAML     Y  copy its name (a port: its address)

READING TEXT  (logs, YAML, details, what changed)
  / n N   search, next and previous match    G gg    end, start
  c       copy the line you searched for, or what's on screen; C copies all
  ctrl-s  keep a copy in Downloads            w       wrap long lines
  in logs: e only errors · h further back · j as written · t timestamps
           f follow · p before the last restart · l live again · r read again

GO SOMEWHERE
  gh apps (home)    gp pods           gd deployments    gs services
  gn nodes          ge events         ga ArgoCD apps    gx ExternalSecrets
  gN node pools     gc config maps    gS secrets        gi web addresses
  gj jobs           gC cron jobs      gH Helm
  :ns <name>   show one namespace  (:ns all for every namespace)
  :ctx         switch to another cluster
  :changes     what changed in the namespace you're in (all: the cluster)
  :shipped     new versions of the last 7 days (ctrl-s keeps a copy)
  :<kind>      open any kind by name, e.g. :pvc or :cron  (ctrl-a lists them)

ON A POD
  s       a shell inside kubed (ctrl-] closes it)   S   a full-screen shell
  F       reach one of its ports on a local port (:pf lists them;
          F works on services and deployments too)
  X       everything it uses: node, AWS role, config, secrets, volumes
  ctrl-k  force stop: at once, without time to finish its work

ON A DEPLOYMENT
  o       the rollout: old and new version side by side, live
  u       go back to the previous version
  v       every version it keeps; pick one to go back to it
  (on an app, + scale, i image, u go back, o rollout, y YAML and e edit act
   on its deployment)

ON A CRON JOB
  enter   its runs     t  run it now     z  pause or resume it

ON A SECRET
  x       show its values

CHANGE THINGS   (not with --readonly)
  +  scale           r  restart           u       go back a version
  i  change image    t  run a cron job    e       edit the YAML
  c  stop new pods landing on a node (cordon)    D  move a node's pods off (drain)
  ctrl-d  delete
  space  pick a row and move down (* picks every row shown), then act on all
  On --readonly every change is off, and so are shells (s, S), secret values
  and local ports (F), as in Kubernetes' own read-only role. Looking works.
  On production, every change asks you to type its name first, and so does
  showing a secret's values; shells ask yes or no, with a warning.

SAVING MONEY
  :waste   what workloads reserve vs what they use, and what you'd save
           (rough until it has watched for a while)
`

// costHelp and sizingHelp are the help's COST section with and without
// prices (no AWS account for the cluster: no dollars anywhere).
const (
	costHelp = `SAVING MONEY
  :waste   what workloads reserve vs what they use, and what you'd save
           (rough until it has watched for a while)
`
	sizingHelp = `UNUSED CAPACITY
  :waste   what workloads reserve vs what they use, and what to reserve instead
`
)

// helpTextFor is the key help, with or without costs.
func helpTextFor(costs bool) string {
	if costs {
		return helpText
	}
	h := strings.Replace(helpText, costHelp, sizingHelp, 1)
	return strings.Replace(h, "  N A R   sort by name, age, restarts   C M  by CPU, memory   $  by cost", "  N A R   sort by name, age, restarts   C M  by CPU, memory", 1)
}

// helpGap is the gap between a help line's columns: two spaces or more.
var helpGap = regexp.MustCompile(` {2,}`)

// goKey is a GO SOMEWHERE group, its key and words in one: "gh apps (home)".
var goKey = regexp.MustCompile(`^g[A-Za-z] \S`)

// helpGroups splits a help line into its key groups (key, then its
// words): "  ↑ ↓     move   enter   open..." is two. false for a line that
// isn't keys: a heading, a sentence, a line that carries on the one above.
func helpGroups(line string) ([][2]string, bool) {
	if !strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "    ") {
		return nil, false
	}
	toks := helpGap.Split(strings.TrimSpace(line), -1)
	var out [][2]string
	switch {
	case goKey.MatchString(toks[0]):
		for _, t := range toks {
			out = append(out, [2]string{t, ""}) // already one short group each
		}
	case len(toks) < 2:
		return nil, false
	case len(toks)%2 == 0:
		for i := 0; i < len(toks); i += 2 {
			out = append(out, [2]string{toks[i], toks[i+1]})
		}
	default: // its words have a wide gap of their own: one group
		out = append(out, [2]string{toks[0], strings.Join(toks[1:], "  ")})
	}
	return out, true
}

// narrowHelp: below this width the help's two columns don't fit, so it
// is one column, each key's words indented under it.
const narrowHelp = 60

// helpLayout fits the help to width: one column when narrow; else as
// written, a line too long for it split into its key groups (wrapped, a
// group would land in the column of words).
func helpLayout(text string, width int) string {
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		groups, keys := helpGroups(line)
		switch {
		case width < narrowHelp && keys:
			for _, g := range groups {
				b.WriteString("  " + g[0] + "\n")
				if g[1] != "" { // one space: a wide gap is where wrapped rows would hang
					b.WriteString("      " + helpGap.ReplaceAllString(g[1], " ") + "\n")
				}
			}
			continue
		case width < narrowHelp && strings.HasPrefix(line, "    "): // carries on the words above
			line = "      " + strings.TrimSpace(line)
		case width < narrowHelp && !strings.HasPrefix(line, " "): // a heading
			line = helpGap.ReplaceAllString(line, " ")
		case keys && len(groups) > 1 && ansi.StringWidth(line) > width:
			col := 0 // where the first group's words start: the others' line up with them
			if i := strings.Index(line, groups[0][1]); groups[0][1] != "" && i > 0 {
				col = ansi.StringWidth(line[:i])
			}
			for _, g := range groups {
				pad := max(col-2-ansi.StringWidth(g[0]), 2)
				b.WriteString(strings.TrimRight("  "+g[0]+strings.Repeat(" ", pad)+g[1], " ") + "\n")
			}
			continue
		}
		b.WriteString(line + "\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// openHelp shows the help, wrapped to the screen.
func (m *Model) openHelp() {
	if m.pager != nil && m.pager.isLogs { // keep the logs running underneath
		m.underHelp, m.pager = m.pager, nil
	}
	p := newPager("help", m.helpContent(), m.pagerWidth(), m.pagerHeight())
	p.wrap = true
	p.refresh()
	m.openPager(p)
}

// relayoutHelp lays the open help out again for the screen's size, as if
// opened at it, keeping how far through it you were.
func (m *Model) relayoutHelp() {
	p := m.pager
	frac := 0.0
	if room := p.vp.TotalLineCount() - p.vp.Height; room > 0 {
		frac = float64(p.vp.YOffset) / float64(room)
	}
	p.lines = strings.Split(strings.TrimRight(m.helpContent(), "\n"), "\n")
	if p.search != "" {
		p.rematch(0)
	}
	p.resize(m.pagerWidth(), m.pagerHeight())
	if room := p.vp.TotalLineCount() - p.vp.Height; room > 0 {
		p.vp.SetYOffset(int(frac*float64(room) + 0.5))
	}
}

// mouseScroll is how many rows one wheel notch moves.
const mouseScroll = 3

// onMouse: the wheel scrolls; a click selects a row, and a click on the
// selected row opens its menu.
func (m *Model) onMouse(msg tea.MouseMsg) tea.Cmd {
	wheel := 0
	switch msg.Button {
	case tea.MouseButtonWheelUp:
		wheel = -mouseScroll
	case tea.MouseButtonWheelDown:
		wheel = mouseScroll
	}
	switch m.mode {
	case modePager:
		if wheel < 0 {
			m.pager.vp.ScrollUp(-wheel)
			m.pager.follow = false
		} else if wheel > 0 {
			m.pager.vp.ScrollDown(wheel)
		}
		return nil
	case modePrompt:
		return m.onMenuClick(msg)
	case modeTable, modeFind:
	default:
		return nil
	}
	if wheel != 0 {
		m.tbl.move(wheel)
		return nil
	}
	if msg.Button != tea.MouseButtonLeft || msg.Action != tea.MouseActionPress {
		return nil
	}
	// Rows start below the header, the input box, the frame's top border and
	// the column headings.
	first := m.headerHeight() + m.inputBoxHeight() + m.topExtra() + 2
	i := msg.Y - first + m.tbl.offset
	if msg.Y < first || msg.Y >= first+m.tbl.onScreen || i < 0 || i >= len(m.tbl.rows) { // only rows on screen
		return nil
	}
	if i == m.tbl.cursor {
		if m.mode == modeFind {
			m.mode = modeTable
			return m.openFound(m.tbl.rows[i])
		}
		return m.onEnter()
	}
	m.tbl.jump(i)
	return nil
}

// onMenuClick: a click on a menu item highlights it, a second click on
// it picks it, as enter does; one that is off says why at
// once, as its key does. Other prompts are keys only.
func (m *Model) onMenuClick(msg tea.MouseMsg) tea.Cmd {
	p := m.prompt
	if p == nil || p.kind != promptMenu || msg.Button != tea.MouseButtonLeft || msg.Action != tea.MouseActionPress ||
		msg.X < p.boxLeft || msg.X >= p.boxLeft+p.boxWidth {
		return nil
	}
	i, ok := p.itemAt[msg.Y-p.boxTop-1] // the box's top border is the first row
	if !ok {
		return nil
	}
	// The menu opens with an item highlighted: only a second click on
	// the same item picks it.
	again := p.clicked == i && p.choice == p.clicked // not moved away with the keys since
	p.clicked = i
	switch {
	case p.items[i].disabled:
		p.choice, p.problem = i, p.items[i].about
	case !again:
		p.choice, p.problem = i, ""
	default:
		return m.onKey(tea.KeyMsg{Type: tea.KeyEnter})
	}
	return nil
}

// notInstalled says, plainly, that a kind isn't on this cluster.
func notInstalled(kind string) string {
	switch {
	case strings.HasPrefix(kind, "nodepools"), strings.HasPrefix(kind, "nodeclaims"):
		return "this cluster has no node pools: Karpenter isn't installed (its nodes come from elsewhere, like AWS node groups)"
	case strings.HasPrefix(kind, "externalsecrets"):
		return "External Secrets isn't installed on this cluster, so there are no ExternalSecrets"
	case strings.HasPrefix(kind, "applications.argoproj"):
		return "ArgoCD isn't installed on this cluster"
	case strings.HasPrefix(kind, "httproutes"):
		return "this cluster has no Gateway API routes"
	}
	what, _, _ := strings.Cut(kind, ".")
	return what + " isn't installed on this cluster"
}

// nsArg is a namespace as :ns takes it ("" is every namespace).
func nsArg(ns string) string {
	if ns == "" {
		return "all"
	}
	return ns
}

// listNamespaceArgs reads -n NS, -A or a bare namespace after a command;
// given is false when none was typed. "all" means every namespace.
// flagWithoutValue: a command's last word is -n (or -l, where labels
// filter the list) with nothing after it.
func flagWithoutValue(args []string, labels bool) error {
	if len(args) == 0 {
		return nil
	}
	switch args[len(args)-1] {
	case "-n", "--namespace":
		return fmt.Errorf("-n needs a namespace name")
	case "-l", "--selector":
		if labels {
			return fmt.Errorf("-l needs a label selector")
		}
	}
	return nil
}

func listNamespaceArgs(args []string) (ns string, given bool) {
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "-A" || a == "--all-namespaces" || a == "all":
			ns, given = "", true
		case (a == "-n" || a == "--namespace") && i+1 < len(args):
			i++
			ns, given = args[i], true
			if ns == "all" {
				ns = ""
			}
		case !strings.HasPrefix(a, "-"):
			ns, given = a, true
		}
	}
	return ns, given
}

// isKind: the word names a list of a resource kind, not one of kubed's
// own commands.
func (m *Model) isKind(word string) bool {
	if m.client == nil || ownWords[word] {
		return false
	}
	_, ok := m.client.Resources.Lookup(resolveAlias(word))
	return ok
}
