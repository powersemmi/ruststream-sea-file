import {expect, it} from 'vitest';
import {from, hrefs} from './testing';

it('links a seek to the components that start it and name its target', () => {
  expect(hrefs('seeking')).toEqual({
    handler: '/components/positions/seek/',
    map: '/components/positions/',
  });
});

it('either moves the reader or is refused, and only a move discards what was queued', () => {
  expect(from('seeking', 'clientSeek')).toEqual(['moved', 'refused']);
  expect(from('seeking', 'moved')).toEqual(['dropped']);
  expect(from('seeking', 'dropped')).toEqual(['next']);
  expect(from('seeking', 'refused')).toEqual([]);
});
