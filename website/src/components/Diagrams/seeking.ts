import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A seek from a handler: a command to the subscription's driver task, the position mapped to the
 * client's target, and the client's seek run to completion. A seek that moved the reader opens a
 * new generation, and what was read at the old one never reaches the handler; a refused one
 * leaves a replay where it was and ends a live subscription.
 */
export function seeking(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind: 'core',
    lines: lines(words, id),
    ...fields,
  });
  const half = (id: string, x: number, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y,
    w: 164,
    h: 44,
    kind: 'good',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    wide('handler', 0, {kind: 'service', href: '/components/positions/seek/'}),
    wide('command', 72),
    wide('map', 144, {href: '/components/positions/'}),
    wide('clientSeek', 216, {kind: 'broker'}),
    half('moved', 4, 300),
    half('refused', 192, 300, {kind: 'bad', h: 58}),
    half('dropped', 4, 372, {h: 58}),
    half('next', 4, 458),
    {
      id: 'backlog',
      x: 0,
      y: 530,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'backlog'),
    },
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'handler', to: 'command', label: word(words, 'sends')},
    {from: 'command', to: 'map'},
    {from: 'map', to: 'clientSeek'},
    {from: 'clientSeek', to: 'moved', exitAt: along(86), label: word(words, 'ok')},
    {from: 'clientSeek', to: 'refused', exitAt: along(274), label: word(words, 'error')},
    {from: 'moved', to: 'dropped'},
    {from: 'dropped', to: 'next'},
  ];
  return {boxes, links};
}
