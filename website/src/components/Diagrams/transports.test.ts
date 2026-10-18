import {describe, expect, it} from 'vitest';
import {layoutOf} from './layouts';
import {hrefs, words} from './testing';

describe('the transports', () => {
  it('link the parts of a stream file to the settings that shape them', () => {
    expect(hrefs('streamFile')).toEqual({
      header: '/components/broker/file-broker/',
      beacon: '/components/broker/beacon-interval/',
      eos: '/components/broker/end-with-eos/',
    });
  });

  it('link the reader, the writer and the pipe to the components they serve', () => {
    expect(hrefs('reader')).toEqual({
      consumer: '/components/subscriptions/file-stream/',
      subscription: '/architecture/seeking/',
      replay: '/components/subscriptions/replay/',
    });
    expect(hrefs('writer')).toEqual({
      publisher: '/components/publishing/file-publish/',
      sync: '/components/broker/shutdown/',
    });
    expect(hrefs('pipe')).toEqual({
      consumers: '/components/subscriptions/lines/',
      publisher: '/components/publishing/stdio-publish/',
      process: '/components/broker/shutdown/',
    });
  });

  it('hand what the stdout thread writes back to the dispatch only on a dashed loopback', () => {
    const loopback = layoutOf('pipe', words).links.filter(
      (link) => link.from === 'stdout' && link.to === 'dispatch',
    );
    expect(loopback).toHaveLength(1);
    expect(loopback[0]!.dashed).toBe(true);
  });
});
