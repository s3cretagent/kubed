// Builds the site's guide pages from the repository's own docs, so the
// docs are written once (docs/*.md) and the site never drifts from them.
import { readFileSync, writeFileSync, mkdirSync, copyFileSync } from 'node:fs';

const root = new URL('../../', import.meta.url);
const out = new URL('../src/content/docs/', import.meta.url);
const pub = new URL('../public/', import.meta.url);

function page(src, dest, title, description, order, edit) {
	let md = readFileSync(new URL(src, root), 'utf8');
	md = md.replace(/^# .*\n/, ''); // the title comes from the front matter
	md = md.replace(/\n(- \[[^\]]+\]\(#[^)]+\)\n)+/, '\n'); // the site has its own table of contents
	md = md.replaceAll('](GUIDE.md)', '](../guide/)');
	const front = [
		'---',
		`title: ${JSON.stringify(title)}`,
		`description: ${JSON.stringify(description)}`,
		`sidebar: { order: ${order} }`,
		`editUrl: https://github.com/s3cretagent/kubed/edit/main/${edit}`,
		'---',
		'',
		`<!-- Made from ${edit} by scripts/sync-docs.mjs: edit that file, not this one. -->`,
		'',
	].join('\n');
	writeFileSync(new URL(dest, out), front + md.replace(/^\n+/, ''));
}

mkdirSync(out, { recursive: true });
mkdirSync(new URL('media/', pub), { recursive: true });
page('docs/GUIDE.md', 'guide.md', 'Using kubed', 'Everyday use: your apps, fixing problems, logs, changes, production clusters and costs.', 2, 'docs/GUIDE.md');
page('docs/DEVELOPMENT.md', 'how-it-works.md', 'How kubed works', 'Building, testing, and the details behind what kubed shows.', 3, 'docs/DEVELOPMENT.md');
copyFileSync(new URL('docs/media/kubed-demo.gif', root), new URL('media/kubed-demo.gif', pub));
