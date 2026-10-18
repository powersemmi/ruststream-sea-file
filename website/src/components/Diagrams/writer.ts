import type {Box, Link} from './flow';
import {chain} from './chain';
import {lines, type Words} from './words';

/*
 * How a publish reaches the file: the crate's publisher hands the message to the client's
 * producer, one writer task per file frames it and places the beacons, and a sink task writes it
 * out. The crate flushes after every publish; the sync to the device runs once, at shutdown.
 */
export function writer(words: Words): {boxes: Box[]; links: Link[]} {
  const {boxes, links} = chain(
    words,
    [
      {key: 'publisher', kind: 'broker', href: '/components/publishing/file-publish/'},
      {key: 'producer', kind: 'broker'},
      {key: 'task', kind: 'broker'},
      {key: 'sink', kind: 'broker'},
      {key: 'file', kind: 'step'},
    ],
    {x: 12, w: 200},
  );
  const sink = boxes.find((box) => box.id === 'sink')!;
  const side = (id: string, y: number, href?: string): Box => ({
    id,
    x: 236,
    y,
    w: 112,
    h: 58,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const flush = side('flush', sink.y - 72);
  const sync = side('sync', sink.y + 14, '/components/broker/shutdown/');
  const last = boxes[boxes.length - 1]!;
  boxes.push(flush, sync, {
    id: 'quota',
    x: 0,
    y: last.y + last.h + 24,
    w: 360,
    h: 44,
    kind: 'bad',
    lines: lines(words, 'quota'),
    href: '/components/broker/file-broker/',
  });
  // Where along the sink's right side each note meets it, level with the note's centre.
  const level = (box: Box): number => (box.y + box.h / 2 - sink.y) / sink.h;
  links.push(
    {
      from: 'flush',
      to: 'sink',
      exit: 'left',
      enter: 'right',
      enterAt: Math.max(0.2, level(flush)),
      dashed: true,
    },
    {
      from: 'sync',
      to: 'sink',
      exit: 'left',
      enter: 'right',
      enterAt: Math.min(0.8, level(sync)),
      dashed: true,
    },
  );
  return {boxes, links};
}
