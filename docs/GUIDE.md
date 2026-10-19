# Using kubed

kubed shows what is running in your Kubernetes cluster, tells you in plain
words when something is wrong, and lets you fix it. This guide walks through
it the way you'll use it. You don't need to read it all: start with
**Your first five minutes**, and come back when you need something.

- [Your first five minutes](#your-first-five-minutes)
- [Moving around](#moving-around)
- [Your apps](#your-apps)
- [When something is wrong](#when-something-is-wrong)
- [Logs](#logs)
- [Finding things](#finding-things)
- [Pods: shell and ports](#pods-shell-and-ports)
- [Making changes safely](#making-changes-safely)
- [Production clusters](#production-clusters)
- [Costs](#costs)
- [Clusters and namespaces](#clusters-and-namespaces)
- [Settings](#settings)
- [When kubed can't connect](#when-kubed-cant-connect)
- [Questions](#questions)
- [Every key](#every-key)

## Your first five minutes

1. Open a terminal and run `kubed`. It uses the same cluster as `kubectl`
   (your current context).
2. You land on **your apps**. Apps that are failing are at the top, in red.
   Apps that need a look are next, in yellow. Everything else is healthy.
3. Move with the arrow keys. The box under the list explains the app you
   are on: what's wrong, the real error, and what fixes it. Above the
   list, the **Watch** line names what else needs a look (cron jobs
   failing, secrets not syncing, nodes nearly full, or no node with room
   for new pods) with the keys that open each.
4. Press **enter** on any row. A menu opens over the list with everything
   you can do with it, and the highlighted item says what it does. Pick
   one, or press its key.
5. Press **esc** to go back, **?** for help, and **q** to quit.

The line at the bottom of the screen always lists the keys that work right
now. If you only remember one thing: **enter shows what you can do**.

Want to look around without any risk? Run `kubed --readonly`. Nothing in the
cluster can be changed in that mode; the menus still show the changes,
greyed out and marked "read-only", at the end, so you can see what kubed
could do. Anything else that can't be used right now says why in a word
("no pods"). Looking still works: lists, logs, events, YAML (with the values of
Secrets blanked; values written straight into a deployment's env show,
as they do for Kubernetes' own view role). Like Kubernetes' own read-only role ("view"), read-only
mode also leaves out what can expose or reach more than you meant: shells,
secret values and port-forwards. D on a node still shows what a drain
would move, without moving anything.

## Moving around

| Key | What it does |
|---|---|
| ↑ ↓ (or j k) | move up and down (in logs j shows lines as written, so use ↑ ↓ there) |
| enter | open the row, or show what you can do with it |
| esc | go back to where you were (same list, same namespace), one step at a time along the trail shown above the list; with nothing left to go back to, your apps. Jumping to a list already on the trail goes back to it there |
| f | find anything by name |
| / | narrow the current list as you type |
| ? | help |
| q | quit |

**Copying text:** select it with the mouse as in any terminal, then copy
(⌘C on a Mac). In logs, YAML and other text screens, **c** copies the line
you searched for (or what's on screen), **C** copies all of it and
**ctrl-s** saves it. Secret values are never saved or copied all at once:
search for one, then **c** copies that line. On a list, **Y** copies names and **ctrl-s** saves
the list as CSV. The mouse wheel scrolls.

**Clicking rows:** start kubed with `--mouse`, or type `:mouse`, to click a
row to select it and click again for its menu; in the menu, click an item
to highlight it and again to run it. kubed then has the mouse,
so to select text hold Option (Mac) or Shift (most other terminals) while
you drag. `:mouse` again turns it off.

To jump straight to a kind of thing, press **g** and then a letter:

| Keys | Opens | Keys | Opens |
|---|---|---|---|
| gh | your apps (home) | gp | pods |
| gd | deployments | gs | services |
| gn | nodes | ge | events |
| ga | ArgoCD apps | gx | ExternalSecrets |
| gN | node pools (Karpenter) | gc | config maps |
| gS | secrets | gi | web addresses (ingresses, or gateway routes on a cluster that uses those more) |
| gj | jobs | gC | cron jobs |
| gH | Helm releases | | |

For anything else, type **:**. Before you type anything, it lists kubed's
commands and the lists people open most. Then type a name, for example
`:pvc` or `:cron`, and press enter: kubed opens the first suggestion.
Everyday words work too (`:cordon` opens nodes, `:urls` opens ingresses, or the gateway's
routes when the cluster uses those).
**ctrl-a** lists every kind your cluster has.

The bottom line shows this screen's most used keys. When a message is too
long for one line, it wraps instead of being cut off.

## Your apps

The home screen lists your apps, one row each:

```
╭─────────── apps (81) · 1 failing · 3 to check · problems first ───────────╮
│ APP             NAMESPACE      STATUS        READY   RESTARTS  DEPLOYED   │
│ payments        payments       Rollout stuck 0/2     0         2h ago     │
│ cert-manager    cert-manager   Out of sync   3/3     0         61d ago    │
│ auth            auth           Running       2/2     5         9d ago     │
├──────────────────────────────── about payments ───────────────────────────┤
│ Problem  Rollout stuck: the new version can't start: can't pull image …   │
│ Fix      o to watch the rollout, or u to go back to the previous version  │
│ Pods     0 of 2 ready                                                     │
╰───────────────────────────────────────────────────────────────────────────╯
```

- **An app** is an ArgoCD app if you use ArgoCD. Otherwise kubed groups pods
  by their app label (`app.kubernetes.io/instance`, `release` or `app`).
  On a cluster with ArgoCD, pods it doesn't manage show as one app per
  namespace when you look at all namespaces (so kube-system and the like
  stay one row each); inside that namespace they are split by app label.
- **STATUS** is the most important thing about the app, in a few words.
  An app with nothing running stays listed, last: **Stopped** when it is
  scaled to 0, **Scheduled** when it only runs cron jobs. One that asks for
  copies but has none is a problem and comes first. **Never runs** means a
  cron job's schedule names a date that can't happen (30 February), and
  **Restarting** that three or more of its containers crashed in the last
  day, the last one within 2 hours, even if each pod looks fine on its own
  (crashes longer ago still show in WHY: "crashed in the last day, the last
  time 10h ago").
- **WHY** (on wide screens) says what is wrong, in a few words.
- The title counts apps that are failing, to check, and **idle** (Stopped,
  Scheduled or with nothing running).
- **READY** is how many of its pods are running and ready, out of all it runs now (while a rollout replaces its copies, out of the copies it asks for, as kubectl counts, plus pods no workload asks a count for (a daemon set's, a job's running ones, or ones nothing makes), as they are; a stuck rollout's new copies count). Pods that already finished (a job's runs, evicted ones) aren't counted: its box lists them as leftovers, kept for their logs.
- **DEPLOYED** is when its running version went out. A restart is not a
  deploy, so restarting doesn't change it.
- **COST/MO** (on AWS) is roughly what the app costs a month. See
  [Costs](#costs).

Press **enter** on an app for everything you can do with it:

- open its pods, its logs (all pods at once), its events and what changed,
  each showing only this app, not the rest of its namespace
- **p**: the logs from before a restart: of the pod the headline is about
  if it restarted, else of the pod whose crash its box quotes (the box
  names that pod when it is another one), else of its most restarted pod
- **+** scale, **u** go back to the previous version, **o** watch a
  rollout, **y** YAML, **e** edit: these act on the app's deployment. If the
  app runs several, kubed shows them and you pick one
- **r**: restart the whole app
- **X**: what it uses (web address, services, config, secrets, AWS role), and
  **F** to reach it on a local port
- **Its cron jobs**, for apps that run jobs on a schedule: when each runs
  next and whether its last run worked
- **Its workloads**, **Its config**, **Its secrets**: only the ones this
  app uses

On a pod, **Open its node** shows the machine it runs on: how busy it is,
what it costs and whether it is short of memory or disk.

On an ArgoCD app (**ga**), **o** lists everything it manages, the parts that
differ from Git first, and **Open its app** shows its pods.

## When something is wrong

Start at the top of the apps list. For the selected app, the box under the
list says:

- **Problem**: what is wrong, in words ("Out of memory: limit 256Mi, 14
  restarts in 20m").
- **Error**: the real error message, when there is one.
- **Impact**: whether it hurts right now.
- **Fix**: what usually fixes it, and which key to press next.

Then these keys answer the usual questions:

| Question | Key |
|---|---|
| What changed recently? | **w**: new versions, syncs, crashes, pods replaced or added, nodes coming and going, failing secrets, scaling (a deployment by hand, or by an autoscaler), over the last 24 hours. Kubernetes keeps no history of its own, so kubed works it out from what is left: scaling and some go-back times come from its events, which last about an hour and are merged when many come at once, so older or busy stretches can miss a scale or give a go-back's time only roughly. A version brought back more than once shows its latest go-back only, a stateful set scaled by hand shows as new pods, not as a scale, and a pod replaced (or a scale made) within about 2 minutes of a rollout or restart counts as part of it. Which pods replaced others kubed works out from what it has seen since it started, so a kubed started later may list fewer; and when a pod is replaced within a minute of a scale, kubed can't tell that pod from the copy the scale added, so both read "replaced a pod that went away, or added a copy", as do a scale's pods when kubed started after it. **:changes** shows the namespace you're in (with all namespaces: the whole cluster). **:shipped** lists the new versions of the last 7 days, one line each, with dates (ctrl-s saves it) |
| What is it saying? | **l**: logs (on an app: all its pods at once). Press **e** in the logs for errors only |
| Why did it crash? | **p**: the logs from before the last restart (on an app: of the pod the headline is about if it restarted, else of the pod whose crash its box quotes, else its most restarted pod; on a deployment or a stateful set: its most restarted pod) |
| What did Kubernetes say? | **E**: events. In the events list (**ge**), "to check" counts the warnings, but not a policy check that passed (Kyverno sends those as warnings), so it can be lower than the warnings kubectl lists |
| Is the new version healthy? | **o** on a deployment: old and new version side by side, live |
| Go back to the last version | **u** on an app, a deployment, a stateful set or a daemon set. **v** on any of them lists every version it keeps, to go back further |

Some examples of what kubed tells you:

- "can't pull image: that image or tag doesn't exist"
- "keeps running out of memory: limit 256Mi"
- "no node fits: not enough free CPU on 3 nodes"
- "ExternalSecret can't update secret db since 1 Jun (4 months): key password
  does not exist in /prod/db"
- "a KEDA trigger can't read its metric"

## Logs

Press **l** on an app, a deployment or a pod.

| Key | What it does |
|---|---|
| e | only lines that look like errors, with the indented lines of their stack traces (press again for all lines). A stack trace written inside one JSON line stays on that line. Copying and saving then take only those lines |
| w | wrap long lines (press again to cut them at the edge instead) |
| / | search; **n** and **N** jump between matches |
| j | show lines exactly as written (press again to tidy them) |
| f | keep following new lines (on by default) |
| t | show or hide Kubernetes' time on each line; you stay where you are |
| h | read further back: the last hour, then 6 hours, then 24 hours (press again to go back to the newest lines). It reads the logs again from the end, following; your search stays, and n / N find your matches. kubed keeps the newest 5,000 lines; the title says when older ones were left out, and so do copies and saved files |
| r | read the logs again (after the connection dropped, say), from the end, following; your search stays |
| ? | help, over the logs; esc comes back to them |
| p | (on a pod) logs from before the last restart. If several containers restarted, kubed asks which |
| c | copy the line you searched for, or what's on screen; **C** copies all of it (logs of several pods as "pod/container: line", with full names) |
| l | (in logs from before a restart) go to the live logs |
| ctrl-s | save them to your Downloads folder |
| esc | back |

Turning **e**, **j** or **w** on or off keeps the line you were reading at
the top of the screen.

On an app or a deployment, kubed follows every pod at once, puts their lines
in time order, and starts each line with the end of the pod's name (and the
container's, when a pod has several).

Most apps log one JSON object (or `key=value` pairs) per line. kubed shows
those lines tidied: the time, the level (errors in red), the message, then
the other fields, with logger bookkeeping left out. Logs from before a
restart end with how the container stopped, for example "ran out of memory
and was killed, at 14:09".

## Finding things

- **f** finds anything by part of its name: apps, pods, deployments,
  services, secrets, nodes and namespaces, across the whole cluster, and
  web addresses (an ingress or route by its host name). Names
  that contain what you typed come first, apps and workloads before their
  pods, config and secrets; close matches (the same letters in
  order) follow, greyed. Pick a result and press enter to go there. **esc**
  brings you back to the results; press **f** to change the search. A
  namespace you pick shows its apps on that screen only.
- On an ingress or route, enter offers **Open the service it sends to**;
  enter on the service shows the pods that answer.
- **/** narrows the list you are looking at, matching the columns you
  can see (and a row's full name, and a pod's phase as kubectl says it, so
  `/Running` keeps running pods whose STATUS says why to look). Some extras: `/!test` hides
  rows containing "test", `/^web` or `/api|auth` work as patterns
  (regular expressions), `/-l app=web` filters by label, and `/warnings`
  (or `/problems`) keeps only the rows flagged in colour (with NO_COLOR set,
  they end in "!"), such as warning
  events.
- **:pods -l app=web**, **:pods -n shop** and **:pods -A** work as in
  kubectl: the namespace given is for that list only (**:ns** changes the
  one you chose).
- **N**, **A** and **R** sort by name, by age and by restarts; **C** and
  **M** by CPU and memory used (as in k9s); **$** by cost. Most first;
  press again for the other way.
- **ctrl-s** on a list saves it as a CSV file in Downloads, as filtered
  and sorted, with real dates and the cluster's name; **Y** copies names (with rows picked, all of them).

## Pods: shell and ports

In the pods list, RESTARTS also says when the last one was ("3 (12m ago)").

The nodes list says how each node is doing:

| STATUS | Means |
|---|---|
| Ready | it runs pods and has room |
| Not ready | it stopped answering: its pods may be moved off |
| …, cordoned | no new pods start there (someone is draining it) |
| Nearly full | it uses over 90% of its memory |
| Full | it uses all of its memory: pods get pushed off |
| Busy | it uses over 90% of its CPU: its pods slow down, nothing is pushed off |
| Packed | its pods reserve over 90% of its CPU or memory (while using less): new pods may not fit |
| No pod room | all its pod places are taken |
| Few pod places | 90% or more of its pod places are taken, a few left |

Packed, Few pod places and No pod room are normal when an autoscaler packs nodes. The Watch
line counts a node as nearly full once it has stayed over 90% for 2 minutes
(at once from 95%), and until it drops below 87%.

In pod and node lists,
CPU is in cores (0.25 is a quarter of one core) and
memory in Kubernetes' own units, as kubectl writes them: Mi and Gi (steps of
1024, so 1Gi is 1024Mi). LIMIT USED is how close a pod is to its memory limit;
near 100% it is about to be killed for running out of memory.

On a pod (or one of its containers):

| Key | What it does |
|---|---|
| s | open a shell inside kubed. **ctrl-]** (or `exit`) closes it and returns to kubed. Not available with `--readonly`, since commands in a shell can change things |
| S | open a full-screen shell, for programs like vim or top |
| F | reach one of the pod's ports on a local port (port-forward, localhost). `:pf` lists them, and says when connections through one fail (often: nothing listens on that port); one that stopped stays listed with why until you start it again or clear it with ctrl-d. **F** also works on a service or a deployment: kubed connects through one of its running pods, and goes on through another when that pod goes away or the connection drops (waiting up to 30 seconds for one to be ready, as when its only pod is replaced) |
| X | what this pod uses: the node it runs on, its AWS role, config, secrets, volumes, services, and the workload that made it. A secret or config map marked optional that doesn't exist is shown as fine |
| ctrl-k | force stop: the pod stops at once, without time to finish its work. If something runs it (a deployment), a new one starts; if nothing does, it is gone for good, and kubed says so before you confirm |

## Making changes safely

Every change asks before it happens, says in a line what will happen, and
warns about anything that will undo it. For example, if ArgoCD manages the
app and will put Git's value back, kubed tells you before you scale.

| Key | What it does | Works on |
|---|---|---|
| + | scale (how many copies run, 0 to 1000) | apps (their deployment), deployments, statefulsets |
| r | restart (a rollout restart: new pods replace the old ones one at a time; a deployment with a ready copy stays up, while a one-copy stateful set or a daemon set's pod is down for a moment; its update settings can change that (Recreate stops every pod first, OnDelete replaces nothing until you delete its pods, a partition holds some pods back, a high maxUnavailable lets several or all stop at once), and kubed says so before you confirm. A paused deployment restarts only once it is resumed) | apps, deployments, statefulsets, daemonsets; on a pod it restarts its deployment |
| u | go back to the previous version (a paused deployment takes it once it is resumed, or at once when that version already runs all its copies; kubed says which) | apps, deployments, statefulsets, daemonsets |
| v | list the versions it keeps: see what differs from the running one, or go back to it. A version you went back to says when it came back ("back 8m ago"), or "back ≤8m ago" when what Kubernetes keeps can't give the exact time (its events expired, or were merged with others) and only the version made before it dates it. w and :changes mark that row's age the same way ("≤8m"), and :shipped and Deployed say "after <time>" | deployments, statefulsets, daemonsets |
| i | change the image (on a paused deployment, it rolls out once resumed, or at once when a version with that image already runs all its copies) | deployments and other workloads |
| t | run a cron job now | cron jobs |
| z | pause a cron job (no new runs), or resume it | cron jobs |
| c | cordon or uncordon (stop or allow new pods) | nodes |
| D | drain (move every pod off safely) | nodes |
| e | edit the YAML in your editor; kubed shows what you changed before applying, and sends you back to the editor if the YAML is broken. Warnings (ArgoCD will undo it) are at the top of the file. Also works from the YAML view | anything |
| ctrl-d | delete | anything |

When kubed suggests a value (the current replica count, a port), typing
replaces it; use the arrow keys or backspace to edit it instead.

To act on several rows at once, press **space** on each one (or **\*** for
every row shown), then press the key. These keys act on every picked row:
**r** (restart, on the workload lists), **c** (cordon), **ctrl-k** (force
stop), **ctrl-d** (delete, or stop ports in :pf) and **Y** (copy their
names, on the apps home too). The other keys act on one row; with rows
picked they say so instead, and the enter menu greys them.

Edits are checked by the cluster before they apply. If someone else changed
the same thing in the meantime, your edit stops instead of overwriting theirs.

## Production clusters

On a production cluster the frame and the logo are red, and every change asks you to type
the name of what you are changing before it happens. For several rows at
once you type the action and the count, for example `delete 3`. Opening a
shell or showing a secret's values asks first too.

kubed decides whether a cluster is production in this order:

1. **The cluster says so.** A ConfigMap `kube-public/kubed` with
   `environment: production` (or `staging`). One of your admins can add it
   once for everyone.
2. **Your own rule**, if you set one (see [Settings](#settings)). It always
   wins.
3. **Words in its name.** `prod`, `production` or `prd` mean production.
   `staging`, `stage`, `dev`, `test`, `qa`, `uat`, `sandbox`, `preprod` and
   similar mean not production. Whole words only, so `devops-prod` is
   production. A name with both kinds of word counts as production, to be
   safe.
4. **You tell it.** For a cluster it can't place, kubed asks you once and
   remembers. Until then it plays safe and asks you to type names.

If your production clusters have other names, see [Settings](#settings).

## Costs

On Amazon EKS, kubed shows what your cluster costs, from your own AWS bill.
It needs nothing set up: it uses the same AWS login your kubeconfig uses (or
a profile in `~/.aws/config` for the cluster's account).

| Where | What |
|---|---|
| Header, **Cost** | what the nodes cost per month; then **all-in**, the whole cluster per month (nodes, disks, network, load balancers, logs and the EKS fee); then the same with tax |
| **?** | where the prices come from, and the all-in split by part |
| Apps, **COST/MO** | an app's share of its nodes, by the CPU and memory it reserves |
| **:waste** | workloads that reserve much more than they use, what to reserve instead, and what you would save. Under an hour of watching, the numbers are greyed: too short to act on |

How the numbers are worked out:

- **Spot nodes** use the live price in their zone.
- **On-demand nodes** use what your account actually paid per hour last week,
  so Savings Plans and Reserved Instances are included.
- **All-in** adds up everything AWS bills under the cluster's tags. It only
  counts Kubernetes infrastructure; databases and buckets are left out. It
  appears once the cluster's tags are turned on as cost allocation tags in
  AWS Billing.
- Figures are **before tax**. The All-in line also shows the total with your
  invoice's tax rate.

If kubed can't find the cluster's AWS account, or you can't log in to it,
it shows no costs at all rather than a wrong number. Press **?** to see
which account and login it used.

kubed reads Cost Explorer once a day per cluster (three requests) and keeps
the answer until the next day. AWS charges $0.01 for each request.

## Clusters and namespaces

- **:ctx** lists your clusters (from your kubeconfig), with production ones
  marked. Pick one with enter; esc goes back. **:ctx -** goes back to the
  cluster you were on before, in the namespace you had there.
- **:ns -** goes back to the namespace you had before.
- **:ns payments** shows only the `payments` namespace (start typing after
  `:ns ` and kubed suggests names). **:ns all** shows every namespace. **0**
  is all namespaces and **1** to **9** are the ones you used recently. On
  the namespaces list (`:ns` with nothing after it), the one you're showing
  is marked, and enter on a namespace shows only that one, everywhere.
- Opening an app shows its namespace for that screen only; the header says
  so. **esc**, or any list you open with **:** or **g**, goes back to the
  namespace you chose.
- If your login can only see some namespaces, kubed shows those and asks you
  to pick one when "all namespaces" isn't allowed.

## Settings

Most people never need these.

Start-up options:

```bash
kubed                          # your current cluster, apps first
kubed pods                     # open straight on a list
kubed --context staging -n payments deploy
kubed --readonly               # look only, no changes possible
kubed --version
```

| Option | What it does |
|---|---|
| `--context` | which cluster from your kubeconfig |
| `-n` | which namespace |
| `--kubeconfig` | a different kubeconfig file |
| `--readonly` | turn off every change, and shells, secret values and port-forwards |
| `--prod-pattern` | which cluster names count as production (a regular expression); it wins over kubed's own rules |

The settings file is `~/.config/kubed/config.yaml`:

```yaml
prodPattern: '-p-'              # cluster names that are production (a regular expression)
nonProdPattern: '-s-'           # names that are not
onDemandDiscount: 28            # only if Cost Explorer is off-limits: % off on-demand prices
```

Your patterns are checked before kubed's own rules and always win. The same
can be set with `--prod-pattern`, or `KUBED_PROD_PATTERN` and
`KUBED_NONPROD_PATTERN` in your environment.

kubed remembers your recent namespaces, `:` history and the answers you gave
about clusters in `~/.config/kubed/state.json`.

## When kubed can't connect

kubed says what went wrong and what to do. Press **r** to try again, **c** to
pick another cluster, or **q** to quit.

| It says | Do this |
|---|---|
| Your AWS login has expired | run `aws sso login --profile <name>` (it shows the name), then press **r** |
| Can't reach the cluster | check your VPN or network, then press **r** |
| The AWS profile your kubeconfig uses doesn't exist | add the profile to `~/.aws/config`, or fix the kubeconfig |
| There is no such cluster in your kubeconfig | press **c** and pick one |
| Your login can't list pods | pick a namespace you can see with `:ns <name>`, or ask for access |

## Questions

**Can I break something by looking around?**
No. Nothing changes until you confirm it. Use `--readonly` to be completely
sure.

**Why does it ask me to type a name?**
The cluster is production (or kubed isn't sure yet). Typing the name makes
sure you meant it.

**Why is there no cost column?**
kubed shows costs only on EKS clusters whose AWS account it can find and log
in to. Press **?** to see why.

**ArgoCD says my app is degraded but the pods look fine.**
Select the app. kubed reads everything ArgoCD manages for it and names the
part that fails, for example a KEDA scaler or a certificate.

**Why don't my changes stick?**
If ArgoCD manages the app with self-heal on, it puts Git's version back.
kubed warns you before such changes. Change it in Git instead.

**Where does kubed keep its files?**
Settings in `~/.config/kubed/`. Cached prices and discovery in
`~/Library/Caches/kubed` (macOS) or `~/.cache/kubed` (Linux). If kubed ever
crashes, the next start tells you where the details are.

**Does it send my data anywhere?**
It talks to your cluster and, for costs, to your AWS account. It downloads
public EC2 prices from instances.vantage.sh once a day. Nothing else.

## Every key

| Key | What it does |
|---|---|
| ↑ ↓ j k | move |
| pgup, pgdown (ctrl-b, ctrl-f), home, end, gg, G | a page up or down, the top, the end (in lists and in text) |
| enter | open, or what can I do with this? |
| esc | back |
| q | quit |
| ? | help |
| f | find anything |
| / | filter this list |
| : | type a command or a kind (`:ns`, `:ctx`, `:pf`, `:waste`, `:pods`…) |
| g + letter | jump (see [Moving around](#moving-around)) |
| 0, 1–9 | all namespaces, a recent namespace |
| N, A, R | sort by name, by age, by restarts |
| C, M, $ | sort by CPU, by memory, by cost |
| ctrl-s | save the list (CSV) or the text you're reading |
| space, * | pick a row, pick all rows shown |
| w | what changed in the last 24 hours |
| l, p | logs, logs from before the last restart (in logs: h reads further back) |
| E | events |
| d | details |
| y | the full YAML |
| X | what it uses |
| x | show a secret's values (it asks first), or a config map's contents |
| o, u, v | rollout view (deployments), go back a version (it skips versions that were only restarts), every kept version (v works in the rollout view too). Stateful sets and daemon sets have u and v too |
| s, S | shell inside kubed, full-screen shell |
| F | port-forward (pod, service or deployment) |
| + | scale |
| r | restart (rollout restart; on a pod: its deployment) |
| i | change image |
| t, z | run a cron job now, pause or resume it |
| c, D | stop or allow new pods on a node (cordon), drain a node |
| e | edit |
| ctrl-k | force stop a pod |
| ctrl-d | delete |
| Y | copy the name (with rows picked: all their names) |
| ctrl-a | list every kind of resource |
| ctrl-n, ctrl-p | (in find) the next or previous result |
| b | (in the versions list) go back to that version |

Commands you can type after **:** besides the kinds: `:ns`, `:ctx`, `:pf`,
`:waste`, `:helm`, `:shipped`, `:changes`, `:urls`, `:mouse`, `:help`,
`:apps` (or `:home`), and `:can-i` (or `:whoami`): what your login may do
in this namespace. Some words people type for other tools work too:
`:history` opens what changed, `:rollback` the deployments (then v),
`:releases` Helm, `:top` the pods, `:drain` the nodes.

In the apps' box, beyond Problem, Fix and Error: **Deployed** is when its
running version went out, **Web** the addresses that reach it, **Note** a
tag that can move (like `latest`, so "same tag" may not mean same code), and
**Leftover** pods of an old version or finished runs still around.
