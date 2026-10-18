import {expect, it} from 'vitest';
import {from, hrefs} from './testing';

it('links each transport of a connected broker to the testing page that covers it', () => {
  expect(hrefs('inProcess')).toEqual({
    live: '/components/testing/live/',
    memory: '/components/testing/',
  });
  expect(from('inProcess', 'connected')).toEqual(['live', 'memory']);
});
