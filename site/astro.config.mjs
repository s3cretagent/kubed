// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
	site: 'https://s3cretagent.github.io',
	base: '/kubed',
	integrations: [
		starlight({
			title: 'kubed',
			description: 'A terminal app for running Kubernetes: your apps, broken ones first, with what is wrong and how to fix it.',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/s3cretagent/kubed' }],
			editLink: { baseUrl: 'https://github.com/s3cretagent/kubed/edit/main/' },
			lastUpdated: false,
			sidebar: [
				{ label: 'Start here', items: [{ label: 'Install', slug: 'install' }] },
				{ label: 'Guide', items: [{ label: 'Using kubed', slug: 'guide' }] },
				{ label: 'Inside kubed', items: [{ label: 'How kubed works', slug: 'how-it-works' }] },
			],
		}),
	],
});
