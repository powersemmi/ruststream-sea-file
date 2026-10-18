import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * How the stdio client carries lines: on the way in, one thread reads standard input and hands
 * each line to the consumers of its stream key; on the way out, one thread writes the lines of
 * every producer to standard output. Under loopback the writing thread also hands its lines to
 * this process's own consumers.
 */
export function pipe(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (id: string, x: number, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y,
    w: 156,
    h: 44,
    kind: 'broker',
    lines: lines(words, id),
    ...fields,
  });
  const left = 12;
  const right = 192;
  const boxes: Box[] = [
    box('upstream', left, 0, {kind: 'service'}),
    box('stdin', left, 72),
    box('dispatch', left, 144, {h: 58}),
    box('consumers', left, 230, {code: true, href: '/components/subscriptions/lines/'}),
    box('publisher', right, 0, {code: true, href: '/components/publishing/stdio-publish/'}),
    box('stdout', right, 72),
    box('line', right, 144, {h: 58, kind: 'step', code: true}),
    box('downstream', right, 230, {kind: 'service'}),
    {
      id: 'process',
      x: 0,
      y: 302,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'process'),
      href: '/components/broker/shutdown/',
    },
  ];
  const links: Link[] = [
    {from: 'upstream', to: 'stdin', label: word(words, 'lines')},
    {from: 'stdin', to: 'dispatch'},
    {from: 'dispatch', to: 'consumers'},
    {from: 'publisher', to: 'stdout'},
    {from: 'stdout', to: 'line'},
    {from: 'line', to: 'downstream'},
    {
      from: 'stdout',
      to: 'dispatch',
      exit: 'left',
      enter: 'right',
      enterAt: 0.3,
      dashed: true,
    },
  ];
  return {boxes, links};
}
