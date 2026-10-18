import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate with a column
 * per transport, the two sea-streamer clients, and at the bottom what they speak to: a file on
 * disk and the process's own pipes.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 236, {kind: 'group'}),
    box('fileSubscriber', left, 226, column, 44, {
      kind: 'broker',
      code: true,
      href: '/architecture/transports/reader/',
    }),
    box('stdioSubscriber', right, 226, column, 44, {
      kind: 'broker',
      code: true,
      href: '/architecture/transports/pipe/',
    }),
    box('filePublisher', left, 280, column, 30, {
      kind: 'broker',
      code: true,
      href: '/architecture/transports/writer/',
    }),
    box('stdioPublisher', right, 280, column, 30, {
      kind: 'broker',
      code: true,
      href: '/components/publishing/stdio-publish/',
    }),
    box('fileBroker', left, 320, column, 44, {
      kind: 'broker',
      code: true,
      href: '/components/broker/file-broker/',
    }),
    box('stdioBroker', right, 320, column, 44, {
      kind: 'broker',
      code: true,
      href: '/components/broker/stdio-broker/',
    }),
    box('envelope', left, 374, 304, 44, {kind: 'core', href: '/architecture/envelope/'}),
    box('client', 0, 460, 360, 72, {kind: 'group'}),
    box('fileClient', left, 486, column, 36, {
      kind: 'broker',
      code: true,
      href: '/architecture/transports/',
    }),
    box('stdioClient', right, 486, column, 36, {
      kind: 'broker',
      code: true,
      href: '/architecture/transports/pipe/',
    }),
    box('disk', left, 556, column, 30, {kind: 'broker'}),
    box('pipes', right, 556, column, 30, {kind: 'broker'}),
    box('inProcess', 0, 610, 360, 44, {kind: 'note', href: '/architecture/in-process/'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'fileSubscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'runtime', to: 'stdioSubscriber', exitAt: centre(right)},
    {from: 'fileSubscriber', to: 'fileClient', exit: 'left', enter: 'left'},
    {from: 'stdioSubscriber', to: 'stdioClient', exit: 'right', enter: 'right'},
    {from: 'fileClient', to: 'disk'},
    {from: 'stdioClient', to: 'pipes'},
  ];
  return {boxes, links};
}
