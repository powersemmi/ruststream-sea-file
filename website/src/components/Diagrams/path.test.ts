import {expect, it} from 'vitest';
import {layoutOf} from './layouts';
import {hrefs, words} from './testing';

it('links the path of a message on both transports', () => {
  expect(hrefs('path')).toEqual({
    fRead: '/architecture/transports/reader/',
    fDriver: '/architecture/seeking/',
    fBatch: '/components/subscriptions/batches/',
    fHandler: '/components/subscriptions/',
    fPublish: '/architecture/envelope/',
    fAppend: '/architecture/transports/writer/',
    pRead: '/architecture/transports/pipe/',
    pBatch: '/components/subscriptions/batches/',
    pHandler: '/components/subscriptions/',
    pPublish: '/architecture/envelope/',
  });
});

it('draws every step of a lane under that lane, so the text version lists it there', () => {
  const {boxes, lanes} = layoutOf('path', words);
  const centres = new Set(lanes!.map((lane) => lane.x));
  expect(boxes.every((box) => centres.has(box.x + box.w / 2))).toBe(true);
});
