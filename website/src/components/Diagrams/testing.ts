import {layoutOf, type DiagramName} from './layouts';
import type {Words} from './words';

/*
 * What the diagram tests read off a layout. Every word is its own key, so a test reads the layout
 * and never a page's wording.
 */

export const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

/** The page each box of a diagram links to, by the box. */
export const hrefs = (name: DiagramName): Record<string, string> =>
  Object.fromEntries(
    layoutOf(name, words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );

/** Where the links of one box lead, in the order the diagram draws them. */
export const from = (name: DiagramName, id: string): string[] =>
  layoutOf(name, words)
    .links.filter((link) => link.from === id)
    .map((link) => link.to);
