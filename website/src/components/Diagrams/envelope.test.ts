import {expect, it} from 'vitest';
import {from, hrefs} from './testing';

it('links the envelope to the headers and the payloads a pipe carries', () => {
  expect(hrefs('envelope')).toEqual({
    check: '/components/headers/payloads/',
    rs1: '/components/headers/',
    rs2: '/components/headers/',
  });
});

it('writes a payload as it is unless it must be enveloped, and picks the form by the headers', () => {
  expect(from('envelope', 'check')).toEqual(['plain', 'lines']);
  expect(from('envelope', 'lines')).toEqual(['rs1', 'rs2']);
  expect([...from('envelope', 'plain'), ...from('envelope', 'rs1')]).toEqual([]);
});
