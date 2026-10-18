import {expect, it} from 'vitest';
import {from, hrefs} from './testing';

it('links the outcomes and the retries to their components', () => {
  expect(hrefs('outcomes')).toEqual({
    ack: '/components/settlement/',
    later: '/components/settlement/retry-after/',
    resume: '/components/positions/start-at/',
  });
  expect(hrefs('retries')).toEqual({
    cap: '/components/settlement/retries/',
    deadLetter: '/components/settlement/dead-letters/',
    file: '/components/settlement/retry-after/',
    pipe: '/components/settlement/out-retry/',
  });
});

it('never brings a message back from an acknowledgement or a requeue', () => {
  expect(from('outcomes', 'outcome')).toEqual(['ack', 'retry', 'later']);
  expect([...from('outcomes', 'ack'), ...from('outcomes', 'retry')]).toEqual([]);
});

it('decides on the attempt cap before a copy goes anywhere', () => {
  expect(from('retries', 'outcome')).toEqual(['cap']);
  expect(from('retries', 'cap')).toEqual(['copy', 'deadLetter', 'dropped']);
  // Only a copy below the cap goes on, to the file or down the pipe; the others end there.
  expect(from('retries', 'copy')).toEqual(['file', 'pipe']);
  expect([...from('retries', 'deadLetter'), ...from('retries', 'dropped')]).toEqual([]);
});
