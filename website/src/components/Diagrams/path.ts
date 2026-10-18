import type {Box, Lane, Link} from './flow';
import {chain, type Step} from './chain';
import {word, type Words} from './words';

const WIDTH = 172;
// The two lanes, side by side: a stream file on the left, a pipe on the right.
const FILE_X = 0;
const PIPE_X = 188;
// Room above the first boxes for the lanes' headers.
const TOP = 28;

const FILE: Step[] = [
  {key: 'fRead', kind: 'broker', href: '/architecture/transports/reader/'},
  {key: 'fDriver', kind: 'broker', href: '/architecture/seeking/'},
  {key: 'fChannel', kind: 'broker', rows: 1},
  {key: 'fStream', kind: 'broker'},
  {key: 'fBatch', kind: 'core', href: '/components/subscriptions/batches/'},
  {key: 'fDecode', kind: 'core'},
  {key: 'fHandler', kind: 'service', href: '/components/subscriptions/'},
  {key: 'fPublish', kind: 'broker', href: '/architecture/envelope/'},
  {key: 'fAppend', kind: 'broker', href: '/architecture/transports/writer/'},
];

const PIPE: Step[] = [
  {key: 'pRead', kind: 'broker', href: '/architecture/transports/pipe/'},
  {key: 'pTask', kind: 'broker'},
  {key: 'pChannel', kind: 'broker', rows: 1},
  {key: 'pStream', kind: 'broker'},
  {key: 'pBatch', kind: 'core', href: '/components/subscriptions/batches/'},
  {key: 'pDecode', kind: 'core'},
  {key: 'pHandler', kind: 'service', href: '/components/subscriptions/'},
  {key: 'pPublish', kind: 'broker', href: '/architecture/envelope/'},
  {key: 'pWrite', kind: 'broker'},
];

/**
 * The path of one message that is answered, on a stream file and on a pipe: from the client's
 * reader to the handler, and from the handler's reply to the transport.
 */
export function path(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const file = chain(words, FILE, {x: FILE_X, w: WIDTH, top: TOP, gap: 22});
  const pipe = chain(words, PIPE, {x: PIPE_X, w: WIDTH, top: TOP, gap: 22});
  return {
    boxes: [...file.boxes, ...pipe.boxes],
    links: [...file.links, ...pipe.links],
    lanes: [
      {x: FILE_X + WIDTH / 2, label: word(words, 'fileLane')},
      {x: PIPE_X + WIDTH / 2, label: word(words, 'pipeLane')},
    ],
  };
}
