import { describe, expect, it, vi } from 'vitest'
import { subscribeRun } from '../src/stream'
import { mockRun } from './fixtures'

class MockSource {
  onopen: ((event: Event) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  listeners = new Map<string, (event: Event) => void>()
  closed = false
  addEventListener(type: string, callback: EventListenerOrEventListenerObject) {
    this.listeners.set(type, callback as (event: Event) => void)
  }
  close() { this.closed = true }
  open() { this.onopen?.(new Event('open')) }
  error() { this.onerror?.(new Event('error')) }
  update(sequence: number) { this.listeners.get('update')?.({ lastEventId: String(sequence), data: JSON.stringify({ sequence }) } as MessageEvent) }
}

async function settle() { await new Promise(resolve => setTimeout(resolve, 0)) }

describe('run event stream', () => {
  it('uses snapshot cursor, deduplicates replay, and refetches after reconnect', async () => {
    const source = new MockSource()
    const urls: string[] = []
    const load = vi.fn().mockResolvedValue({ ...mockRun, sequence: 4 })
    const onSnapshot = vi.fn()
    const onConnection = vi.fn()
    const stop = subscribeRun(mockRun.id, mockRun, { onSnapshot, onConnection }, { load, source: url => { urls.push(url); return source } })
    expect(urls).toEqual(['/api/runs/fixture-run/events?after=4'])
    source.open(); await settle()
    expect(load).toHaveBeenCalledTimes(1)
    expect(onConnection).toHaveBeenLastCalledWith('connected')
    source.update(4); await settle()
    expect(load).toHaveBeenCalledTimes(1)
    source.update(5); await settle()
    expect(load).toHaveBeenCalledTimes(2)
    source.update(5); await settle()
    expect(load).toHaveBeenCalledTimes(2)
    source.error()
    expect(onConnection).toHaveBeenLastCalledWith('disconnected')
    source.open(); await settle()
    expect(load).toHaveBeenCalledTimes(3)
    expect(onSnapshot).toHaveBeenCalledTimes(3)
    expect(onConnection).toHaveBeenLastCalledWith('connected')
    stop()
    expect(source.closed).toBe(true)
  })

  it('keeps the previous snapshot and marks the stream disconnected after a failed refresh', async () => {
    const source = new MockSource()
    const onSnapshot = vi.fn()
    const onConnection = vi.fn()
    const stop = subscribeRun(mockRun.id, mockRun, { onSnapshot, onConnection }, { load: vi.fn().mockRejectedValue(new Error('offline')), source: () => source })
    source.open(); await settle()
    expect(onSnapshot).not.toHaveBeenCalled()
    expect(onConnection).toHaveBeenLastCalledWith('disconnected')
    stop()
  })
})
