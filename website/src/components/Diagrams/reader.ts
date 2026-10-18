import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * How the file client reads: a task follows the file like `tail -f`, a message source tells the
 * messages from the beacons, and a consumer task hands the messages of its keys to the
 * subscription. Beside it, the way a seek finds its target: survey the beacons, rewind to one,
 * read forward.
 */
export function reader(words: Words): {boxes: Box[]; links: Link[]} {
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
    box('file', left, 0, {w: 336, h: 30, kind: 'step'}),
    box('source', left, 66),
    box('messages', left, 138),
    box('consumer', left, 210, {href: '/components/subscriptions/file-stream/'}),
    box('subscription', left, 282, {kind: 'core', href: '/architecture/seeking/'}),
    box('survey', right, 138, {kind: 'core'}),
    box('rewind', right, 210, {kind: 'core'}),
    box('scan', right, 282, {kind: 'core'}),
    {
      id: 'replay',
      x: 0,
      y: 354,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'replay'),
      href: '/components/subscriptions/replay/',
    },
  ];
  const links: Link[] = [
    {from: 'file', to: 'source', exitAt: (left + 78 - left) / 336, label: word(words, 'bytes')},
    {from: 'source', to: 'messages'},
    {from: 'messages', to: 'consumer'},
    {from: 'consumer', to: 'subscription', label: word(words, 'next')},
    {from: 'messages', to: 'survey', dashed: true, label: word(words, 'seek')},
    {from: 'survey', to: 'rewind'},
    {from: 'rewind', to: 'scan'},
  ];
  return {boxes, links};
}
