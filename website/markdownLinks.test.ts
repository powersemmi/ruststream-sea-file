import {describe, expect, it} from 'vitest';
import {resolveMarkdownLinks} from './markdownLinks';

// The hook as Docusaurus calls it: a link it could not resolve, and the file it sits in, relative
// to the site directory.
const hook = resolveMarkdownLinks() as (link: {url: string; sourceFilePath: string}) => string;
const resolve = (url: string, sourceFilePath: string): string => hook({url, sourceFilePath});

const russian = 'i18n/ru/docusaurus-plugin-content-docs/current';

describe('a Markdown link Docusaurus could not resolve', () => {
  it('from a translated page to a neighbour with no translation goes to its address', () => {
    expect(resolve('design.md', `${russian}/architecture/index.md`)).toBe('/architecture/design/');
  });

  it('from a page with no translation to a translated neighbour goes to its address', () => {
    expect(resolve('../index.md#transports', '../docs/architecture/benchmarks/methodology.md')).toBe(
      '/architecture/#transports',
    );
  });

  it('to a file the site does not have fails the build', () => {
    expect(() =>
      resolve('no-such-page.md', '../docs/architecture/benchmarks/methodology.md'),
    ).toThrow("Markdown link 'no-such-page.md'");
  });

  it('outside the documentation fails the build', () => {
    expect(() =>
      resolve('../../../README.md', '../docs/architecture/benchmarks/methodology.md'),
    ).toThrow("Markdown link '../../../README.md'");
  });
});
