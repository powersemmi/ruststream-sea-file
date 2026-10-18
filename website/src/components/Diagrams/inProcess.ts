import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * One connected broker, two transports: `connect` links it to the client, `connect_in_process`
 * to a file or a pipe held in memory, which the test harness drives and counts. Under each, what
 * that transport covers.
 */
export function inProcess(words: Words): {boxes: Box[]; links: Link[]} {
  const half = (id: string, x: number, y: number, h: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y,
    w: 172,
    h,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    {
      id: 'connected',
      x: 12,
      y: 0,
      w: 336,
      h: 44,
      kind: 'broker',
      code: true,
      lines: lines(words, 'connected'),
    },
    half('live', 0, 92, 58, {kind: 'broker', href: '/components/testing/live/'}),
    half('memory', 188, 92, 58, {kind: 'core', href: '/components/testing/'}),
    half('coordinator', 188, 176, 58, {kind: 'core'}),
    half('liveOnly', 0, 260, 104, {kind: 'note'}),
    half('emulated', 188, 260, 104, {kind: 'good'}),
    {id: 'production', x: 0, y: 388, w: 360, h: 44, kind: 'note', lines: lines(words, 'production')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'connected', to: 'live', exitAt: along(86), label: word(words, 'connect')},
    {from: 'connected', to: 'memory', exitAt: along(274), label: word(words, 'inProcess')},
    {from: 'memory', to: 'coordinator'},
  ];
  return {boxes, links};
}
