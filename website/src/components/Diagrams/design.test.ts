import {expect, it} from 'vitest';
import {hrefs} from './testing';

it('links the design of the crate to its brokers and to the pages of its parts', () => {
  expect(hrefs('design')).toEqual({
    fileSubscriber: '/architecture/transports/reader/',
    stdioSubscriber: '/architecture/transports/pipe/',
    filePublisher: '/architecture/transports/writer/',
    stdioPublisher: '/components/publishing/stdio-publish/',
    fileBroker: '/components/broker/file-broker/',
    stdioBroker: '/components/broker/stdio-broker/',
    envelope: '/architecture/envelope/',
    fileClient: '/architecture/transports/',
    stdioClient: '/architecture/transports/pipe/',
    inProcess: '/architecture/in-process/',
  });
});
