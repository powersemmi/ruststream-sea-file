import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/** One step on the right of a ladder: what the transport does, and how tall its box is. */
interface Step {
  key: string;
  y: number;
  h?: number;
  href?: string;
  kind?: Box['kind'];
}

/*
 * A broker's three states as types on the left, and on the right what each transition does on
 * the transport. `states` names the boxes of the left column: the broker as built, connected and
 * closed, at the heights of the steps they own.
 */
function ladder(
  words: Words,
  states: {key: string; y: number; href?: string}[],
  steps: Step[],
  note: {href?: string},
): {boxes: Box[]; links: Link[]} {
  const bottom = Math.max(...steps.map((step) => step.y + (step.h ?? 58))) + 12;
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: bottom, kind: 'group', lines: lines(words, 'states')},
    {id: 'transport', x: 184, y: 0, w: 176, h: bottom, kind: 'group', lines: lines(words, 'transport')},
    ...states.map(
      ({key, y, href}): Box => ({
        id: key,
        x: 12,
        y,
        w: 156,
        h: 58,
        kind: 'broker',
        code: true,
        lines: lines(words, key),
        href,
      }),
    ),
    ...steps.map(
      ({key, y, h = 58, href, kind = 'core'}): Box => ({
        id: key,
        x: 192,
        y,
        w: 156,
        h,
        kind,
        lines: lines(words, key),
        href,
      }),
    ),
    {
      id: 'afterShutdown',
      x: 0,
      y: bottom + 22,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
      href: note.href,
    },
  ];
  const links: Link[] = [
    {from: states[0]!.key, to: states[1]!.key, label: word(words, 'connect')},
    {from: states[1]!.key, to: states[2]!.key, label: word(words, 'close')},
    ...steps.slice(1).map((step, index) => ({from: steps[index]!.key, to: step.key})),
  ];
  return {boxes, links};
}

/*
 * The file broker: `new` records the path, connect opens the file and its producer, a
 * subscription opens a reader, a publish appends and flushes, and shutdown ends the writer.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  return ladder(
    words,
    [
      {key: 'unconnected', y: 28, href: '/components/broker/file-broker/'},
      {key: 'connected', y: 182},
      {key: 'closed', y: 368, href: '/components/broker/shutdown/'},
    ],
    [
      {key: 'config', y: 28},
      {key: 'open', y: 98, h: 72, href: '/components/broker/existing-only/'},
      {key: 'subscriptions', y: 182, h: 72},
      {key: 'publishes', y: 278, h: 58},
      {key: 'shutdown', y: 368, h: 88, href: '/components/broker/end-with-eos/'},
    ],
    {href: '/components/broker/shutdown/'},
  );
}

/*
 * The stdio broker: connect records loopback and touches no pipe, the first subscription and the
 * first publish start the client's threads, and shutdown ends every stdio stream of the process.
 */
export function stdioLifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  return ladder(
    words,
    [
      {key: 'unconnected', y: 28, href: '/components/broker/stdio-broker/'},
      {key: 'connected', y: 98},
      {key: 'closed', y: 254, href: '/components/broker/shutdown/'},
    ],
    [
      {key: 'config', y: 28, href: '/components/broker/loopback/'},
      {key: 'reading', y: 98},
      {key: 'writing', y: 176},
      {key: 'shutdown', y: 254, kind: 'bad', href: '/components/broker/shutdown/'},
    ],
    {},
  );
}
