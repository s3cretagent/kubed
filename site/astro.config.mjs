// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const page = (slug, label) => ({ slug, ...(label ? { label } : {}) });

export default defineConfig({
	site: 'https://s3cretagent.github.io',
	base: '/kubed',
	integrations: [
		starlight({
			title: 'kubed',
			description: 'A terminal app for running Kubernetes: your apps, broken ones first, with what is wrong and how to fix it.',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/s3cretagent/kubed' }],
			editLink: { baseUrl: 'https://github.com/s3cretagent/kubed/edit/main/' },
			sidebar: [
				{ label: 'Get started', items: [page('install'), page('guide/quick-start'), page('guide/moving-around')] },
				{
					label: 'Everyday use',
					items: [page('guide/your-apps'), page('guide/when-something-is-wrong'), page('guide/logs'), page('guide/finding-things'), page('guide/pods-shell-and-ports')],
				},
				{ label: 'Making changes', items: [page('guide/making-changes-safely'), page('guide/production-clusters')] },
				{
					label: 'Setup',
					items: [page('guide/clusters-and-namespaces'), page('guide/costs'), page('guide/settings'), page('guide/when-kubed-cant-connect')],
				},
				{ label: 'Reference', items: [page('guide/every-key'), page('guide/questions')] },
				{ label: 'About', items: [page('compared'), page('how-it-works')] },
			],
		}),
	],
});
