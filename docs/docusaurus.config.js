// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');


async function createConfig() {
  const { remarkKroki } = await import('remark-kroki');
  //const remarkGridTables = require('remark-grid-tables')


  /** @type {import('@docusaurus/types').Config} */
  const config = {
    title: 'Flowtide',
    tagline: 'Diffierential dataflow streaming engine',
    favicon: 'img/favicon.ico',

    plugins: [require.resolve('docusaurus-lunr-search')],

    // Set the production url of your site here
    url: 'https://koralium.github.io',
    // Set the /<baseUrl>/ pathname under which your site is served
    // For GitHub pages deployment, it is often '/<projectName>/'
    baseUrl: '/flowtide/',

    // GitHub pages deployment config.
    // If you aren't using GitHub pages, you don't need these.
    organizationName: 'koralium', // Usually your GitHub org/user name.
    projectName: 'flowtide', // Usually your repo name.
    trailingSlash: false,
    deploymentBranch: 'gh-pages',

    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',

    // Even if you don't use internalization, you can use this field to set useful
    // metadata like html lang. For example, if your site is Chinese, you may want
    // to replace "en" with "zh-Hans".
    i18n: {
      defaultLocale: 'en',
      locales: ['en'],
    },

    presets: [
      [
        'classic',
        /** @type {import('@docusaurus/preset-classic').Options} */
        ({
          docs: {
            sidebarPath: require.resolve('./sidebars.js'),
            remarkPlugins: [
              [remarkKroki, { server: 'https://kroki.io', alias: ['blockdiag', 'kroki' ]}],
              // [remarkGridTables, {}]
            ],
            //rehypePlugins: [remarkGridTables],
            // Please change this to your repo.
            // Remove this to remove the "edit this page" links.
            editUrl:
              'https://github.com/koralium/flowtide/tree/main/docs',
          },
          blog: {
            showReadingTime: true,
            // Please change this to your repo.
            // Remove this to remove the "edit this page" links.
            editUrl:
              'https://github.com/koralium/flowtide/tree/main/docs',
          },
          theme: {
            customCss: require.resolve('./src/css/custom.css'),
          },
        }),
      ],
    ],

    themeConfig:
      /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
      ({
        // Replace with your project's social card
        //image: 'img/docusaurus-social-card.jpg',
        navbar: {
          title: 'Flowtide.NET',
          // logo: {
          //   alt: 'My Site Logo',
          //   src: 'img/logo.svg',
          // },
          items: [
            {
              type: 'docSidebar',
              sidebarId: 'tutorialSidebar',
              position: 'left',
              label: 'Documentation',
            },
            // {to: '/blog', label: 'Blog', position: 'left'},
            {
              href: 'https://github.com/koralium/flowtide',
              label: 'GitHub',
              position: 'right',
            },
          ],
        },
        footer: {
          style: 'dark',
          links: [
            {
              title: 'Docs',
              items: [
                {
                  label: 'Tutorial',
                  to: '/docs/intro',
                },
              ],
            },
            {
              title: 'Community',
              items: [
              ],
            },
            {
              title: 'More',
              items: [
                // {
                //   label: 'Blog',
                //   to: '/blog',
                // },
                {
                  label: 'GitHub',
                  href: 'https://github.com/koralium/flowtide',
                },
              ],
            },
          ],
          copyright: `Copyright © ${new Date().getFullYear()} Flowtide.NET. Built with Docusaurus.`,
        },
        prism: {
          theme: lightCodeTheme,
          darkTheme: darkCodeTheme,
          additionalLanguages: ['csharp', 'protobuf']
        },
      }),
  };

  return config
}

module.exports = createConfig;
