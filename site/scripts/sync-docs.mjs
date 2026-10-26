// Builds the site's guide pages from the repository's own docs, so the
// docs are written once (docs/*.md) and the site never drifts from them.
// docs/GUIDE.md is split at each "## " heading into one page per topic.
import { readFileSync, writeFileSync, mkdirSync, copyFileSync, rmSync } from 'node:fs';

const root = new URL('../../', import.meta.url);
const out = new URL('../src/content/docs/', import.meta.url);
const pub = new URL('../public/', import.meta.url);

// Each section of the guide: its heading, the page it becomes, and the
// title and one-line description that page shows.
export const sections = [
	['Your first five minutes', 'quick-start', 'Quick start', 'Open kubed, read your apps, and learn the one key that matters.'],
	['Moving around', 'moving-around', 'Moving around', 'Keys to move, go back, jump between lists, and use the mouse.'],
	['Your apps', 'your-apps', 'Your apps', 'What each column on the home screen means, and how apps are grouped.'],
	['When something is wrong', 'when-something-is-wrong', 'When something is wrong', 'Find the cause of a problem, what changed, and what fixes it.'],
	['Logs', 'logs', 'Logs', 'Read, follow, search, pause and save logs.'],
	['Finding things', 'finding-things', 'Finding things', 'Find anything by name, filter lists, and sort them.'],
	['Pods: shell and ports', 'pods-shell-and-ports', 'Shells and ports', 'Open a shell in a container and reach a pod on a local port.'],
	['Making changes safely', 'making-changes-safely', 'Making changes safely', 'Change an image, go back a version, restart, scale and edit, with a question first.'],
	['Production clusters', 'production-clusters', 'Production clusters', 'How kubed knows a cluster is production, and what it asks there.'],
	['Costs', 'costs', 'Costs', 'What your cluster, nodes and apps cost on Amazon EKS.'],
	['Clusters and namespaces', 'clusters-and-namespaces', 'Clusters and namespaces', 'Switch clusters and namespaces.'],
	['Settings', 'settings', 'Settings', 'The settings file and what you can change.'],
	["When kubed can't connect", 'when-kubed-cant-connect', "When kubed can't connect", 'What to check when kubed can\'t reach your cluster.'],
	['Questions', 'questions', 'Questions', 'Common questions, answered.'],
	['Every key', 'every-key', 'Every key', 'Every key on every screen, in one place.'],
];

const slugOf = (heading) => heading.toLowerCase().replace(/[^a-z0-9 -]/g, '').trim().replace(/ +/g, '-');
const pageFor = new Map(sections.map(([h, page]) => [slugOf(h), page]));

function front(title, description, edit, extra = '') {
	return [
		'---',
		`title: ${JSON.stringify(title)}`,
		`description: ${JSON.stringify(description)}`,
		`editUrl: https://github.com/s3cretagent/kubed/edit/main/${edit}`,
		extra,
		'---',
		'',
		`<!-- Made from ${edit} by site/scripts/sync-docs.mjs: edit that file, not this one. -->`,
		'',
	].filter((l) => l !== null).join('\n');
}

// A link to another section of the guide becomes a link to its page.
const relink = (md) =>
	md.replace(/\]\(#([a-z0-9-]+)\)/g, (all, anchor) => (pageFor.has(anchor) ? `](/kubed/guide/${pageFor.get(anchor)}/)` : all));

function guide() {
	const md = readFileSync(new URL('docs/GUIDE.md', root), 'utf8');
	const parts = md.split(/^## /m).slice(1);
	rmSync(new URL('guide/', out), { recursive: true, force: true });
	mkdirSync(new URL('guide/', out), { recursive: true });
	for (const part of parts) {
		const heading = part.slice(0, part.indexOf('\n')).trim();
		const section = sections.find(([h]) => h === heading);
		if (!section) throw new Error(`docs/GUIDE.md has a section the site doesn't list: "${heading}"`);
		const [, page, title, description] = section;
		const body = relink(part.slice(part.indexOf('\n') + 1)).replace(/^\n+/, '');
		writeFileSync(new URL(`guide/${page}.md`, out), front(title, description, 'docs/GUIDE.md') + body);
	}
}

function howItWorks() {
	let md = readFileSync(new URL('docs/DEVELOPMENT.md', root), 'utf8');
	md = md.replace(/^# .*\n/, '').replaceAll('](GUIDE.md)', '](/kubed/guide/quick-start/)');
	writeFileSync(new URL('how-it-works.md', out), front('How kubed works', 'Building, testing, and the details behind what kubed shows.', 'docs/DEVELOPMENT.md') + md.replace(/^\n+/, ''));
}

guide();
howItWorks();
mkdirSync(new URL('media/', pub), { recursive: true });
copyFileSync(new URL('docs/media/kubed-demo.gif', root), new URL('media/kubed-demo.gif', pub));
