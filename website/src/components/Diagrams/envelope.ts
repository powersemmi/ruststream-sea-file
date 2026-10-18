import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * How a publish writes its message: a payload that needs nothing around it goes out as it is;
 * otherwise it is enveloped, as `name: value` lines when every header survives them, else with
 * every name and value length-prefixed. Both envelopes are a prefix and base64, so a line of the
 * pipe can carry them.
 */
export function envelope(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'message', x: 12, y: 0, w: 336, h: 44, kind: 'service', lines: lines(words, 'message')},
    {
      id: 'check',
      x: 12,
      y: 72,
      w: 336,
      h: 58,
      kind: 'core',
      lines: lines(words, 'check'),
      href: '/components/headers/payloads/',
    },
    {id: 'plain', x: 0, y: 166, w: 164, h: 44, kind: 'good', lines: lines(words, 'plain')},
    {id: 'lines', x: 196, y: 166, w: 164, h: 44, kind: 'core', lines: lines(words, 'lines')},
    {
      id: 'rs1',
      x: 0,
      y: 246,
      w: 172,
      h: 74,
      kind: 'broker',
      code: true,
      lines: lines(words, 'rs1'),
      href: '/components/headers/',
    },
    {
      id: 'rs2',
      x: 188,
      y: 246,
      w: 172,
      h: 89,
      kind: 'broker',
      code: true,
      lines: lines(words, 'rs2'),
      href: '/components/headers/',
    },
    {id: 'decode', x: 0, y: 359, w: 360, h: 44, kind: 'note', lines: lines(words, 'decode')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'message', to: 'check'},
    {from: 'check', to: 'plain', exitAt: along(82), label: word(words, 'no')},
    {from: 'check', to: 'lines', exitAt: along(278), label: word(words, 'yes')},
    {from: 'lines', to: 'rs1', exitAt: 0.3, label: word(words, 'yes')},
    {from: 'lines', to: 'rs2', exitAt: 0.7, label: word(words, 'no')},
  ];
  return {boxes, links};
}
