import {envelope} from './envelope';
import type {Box, Lane, Link} from './flow';
import {inProcess} from './inProcess';
import {outcomes, retries} from './settlement';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

type Place = (words: Words) => Layout;

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  outcomes,
  retries,
  envelope,
  inProcess,
} satisfies Record<string, Place>;

export type DiagramName = keyof typeof LAYOUTS;

/** The boxes and links of the diagram `name`, with the words of the page. */
export function layoutOf(name: DiagramName, words: Words): Layout {
  return LAYOUTS[name](words);
}
