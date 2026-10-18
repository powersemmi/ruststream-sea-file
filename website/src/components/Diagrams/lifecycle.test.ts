import {expect, it} from 'vitest';
import {hrefs} from './testing';

it('links each lifecycle to the settings and the shutdown of its broker', () => {
  expect(hrefs('lifecycle')).toEqual({
    unconnected: '/components/broker/file-broker/',
    closed: '/components/broker/shutdown/',
    open: '/components/broker/existing-only/',
    shutdown: '/components/broker/end-with-eos/',
    afterShutdown: '/components/broker/shutdown/',
  });
  expect(hrefs('stdioLifecycle')).toEqual({
    unconnected: '/components/broker/stdio-broker/',
    closed: '/components/broker/shutdown/',
    config: '/components/broker/loopback/',
    shutdown: '/components/broker/shutdown/',
  });
});
