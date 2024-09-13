import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';


export default function Home(): JSX.Element {
  const {siteConfig} = useDocusaurusContext();

  window.location.href = 'docs/intro.html';
  return (
    <div>If you are not redirected automatically, follow this <a href="docs/intro.html">link</a>.</div>
  );
}
