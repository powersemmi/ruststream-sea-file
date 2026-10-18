import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * What a handler's outcome becomes on a transport that keeps no consumer positions: an
 * acknowledgement and a requeue are reported unsupported, and only a retry that the runtime
 * publishes as a copy brings a message back.
 */
export function outcomes(words: Words): {boxes: Box[]; links: Link[]} {
  const leaf = (id: string, x: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y: 72,
    w: 112,
    h: 88,
    kind: 'bad',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    {id: 'outcome', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    leaf('ack', 0, {href: '/components/settlement/'}),
    leaf('retry', 124),
    leaf('later', 248, {kind: 'good', href: '/components/settlement/retry-after/'}),
    {
      id: 'resume',
      x: 0,
      y: 184,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'resume'),
      href: '/components/positions/start-at/',
    },
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'outcome', to: 'ack', exitAt: along(56)},
    {from: 'outcome', to: 'retry', exitAt: along(180)},
    {from: 'outcome', to: 'later', exitAt: along(304)},
  ];
  return {boxes, links};
}

/*
 * A retry, which neither transport counts: the runtime checks the attempt cap first, then
 * publishes a copy, to the same stream key of the file or to the key the mount site named on a
 * pipe, or a copy to the dead-letter key, or nothing.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'outcome', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    {
      id: 'cap',
      x: 12,
      y: 72,
      w: 336,
      h: 44,
      kind: 'core',
      lines: lines(words, 'cap'),
      href: '/components/settlement/retries/',
    },
    {id: 'copy', x: 0, y: 148, w: 112, h: 88, kind: 'good', lines: lines(words, 'copy')},
    {
      id: 'deadLetter',
      x: 124,
      y: 148,
      w: 112,
      h: 88,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
      href: '/components/settlement/dead-letters/',
    },
    {id: 'dropped', x: 248, y: 148, w: 112, h: 88, kind: 'bad', lines: lines(words, 'dropped')},
    {
      id: 'file',
      x: 0,
      y: 268,
      w: 172,
      h: 58,
      kind: 'broker',
      lines: lines(words, 'file'),
      href: '/components/settlement/retry-after/',
    },
    {
      id: 'pipe',
      x: 188,
      y: 268,
      w: 172,
      h: 58,
      kind: 'broker',
      lines: lines(words, 'pipe'),
      href: '/components/settlement/out-retry/',
    },
    {id: 'crash', x: 0, y: 350, w: 360, h: 44, kind: 'note', lines: lines(words, 'crash')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    {from: 'cap', to: 'copy', exitAt: along(56), label: word(words, 'below')},
    {from: 'cap', to: 'deadLetter', exitAt: along(180), label: word(words, 'atCap')},
    {from: 'cap', to: 'dropped', exitAt: along(304)},
    {from: 'copy', to: 'file', exitAt: 0.3, label: word(words, 'onFile')},
    {from: 'copy', to: 'pipe', exitAt: 0.7, label: word(words, 'onPipe')},
  ];
  return {boxes, links};
}
