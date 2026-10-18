import type {Box, BoxKind, Link} from './flow';
import {lines, type Words} from './words';

// The regions of the file the drawing shows, top to bottom as they follow each other on disk, and
// what each one is. A message of a key interleaves with the others, and a beacon may split one.
export const REGIONS: ReadonlyArray<{key: string; kind: BoxKind}> = [
  {key: 'header', kind: 'broker'},
  {key: 'orders1', kind: 'step'},
  {key: 'receipts1', kind: 'step'},
  {key: 'orders2', kind: 'step'},
  {key: 'beacon', kind: 'core'},
  {key: 'continued', kind: 'step'},
  {key: 'eos', kind: 'note'},
];

// The fields of one message, in the order they are written.
export const FIELDS = ['key', 'meta', 'size', 'payload', 'checksum', 'end'] as const;

const ROW = 44;
const GAP = 10;
const COLUMN = 152;

const HREFS: Record<string, string> = {
  header: '/components/broker/file-broker/',
  beacon: '/components/broker/beacon-interval/',
  eos: '/components/broker/end-with-eos/',
};

/*
 * A stream file as it lies on disk: a header, the messages of every stream key one after another,
 * a beacon at every multiple of the beacon interval (which may split a message in two), and an
 * optional end-of-stream message. Beside it, one message taken apart into its fields.
 */
export function streamFile(words: Words): {boxes: Box[]; links: Link[]} {
  const regions: Box[] = REGIONS.map(({key, kind}, index) => ({
    id: key,
    x: 12,
    y: 26 + index * (ROW + GAP),
    w: COLUMN,
    h: ROW,
    kind,
    lines: lines(words, key),
    href: HREFS[key],
  }));
  const fileHeight = 26 + REGIONS.length * (ROW + GAP) - GAP + 12;
  const fields: Box[] = FIELDS.map((key, index) => ({
    id: key,
    x: 196,
    y: 26 + index * (ROW + GAP),
    w: COLUMN,
    h: ROW,
    kind: key === 'payload' ? 'service' : 'step',
    code: key === 'end',
    lines: lines(words, key),
  }));
  const messageHeight = 26 + FIELDS.length * (ROW + GAP) - GAP + 12;
  const first = regions[1]!;
  const boxes: Box[] = [
    {id: 'file', x: 0, y: 0, w: 176, h: fileHeight, kind: 'group', lines: lines(words, 'file')},
    ...regions,
    {
      id: 'message',
      x: 184,
      y: 0,
      w: 176,
      h: messageHeight,
      kind: 'group',
      lines: lines(words, 'message'),
    },
    ...fields,
    {
      id: 'keys',
      x: 0,
      y: fileHeight + 20,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'keys'),
      href: '/components/subscriptions/file-stream/',
    },
  ];
  const links: Link[] = [
    {
      from: first.id,
      to: 'message',
      exit: 'right',
      enter: 'left',
      // Level with the message it takes apart, so the arrow runs straight across.
      enterAt: (first.y + ROW / 2) / messageHeight,
    },
  ];
  return {boxes, links};
}
