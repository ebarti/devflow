import { api } from './api'
import type { RunDetail } from './model'

type Connection = 'connecting' | 'connected' | 'disconnected'
interface Source {
  addEventListener(type: string, listener: (event: Event) => void): void
  close(): void
  onopen: ((event: Event) => void) | null
  onerror: ((event: Event) => void) | null
}

/** Snapshot-first stream. Native EventSource retries with Last-Event-ID; cursor replay is deduplicated here. */
export function subscribeRun(
  runId: string,
  initial: RunDetail,
  callbacks: { onSnapshot: (run: RunDetail) => void; onConnection: (state: Connection) => void },
  dependencies: { load?: (id: string) => Promise<RunDetail>; source?: (url: string) => Source } = {},
): () => void {
  let lastSequence = Math.max(initial.sequence ?? 0, ...(initial.events ?? []).map(event => event.sequence))
  let closed = false
  let open = false
  let refreshing = false
  let refreshRequested = false
  const load = dependencies.load ?? api.getRun
  const source = (dependencies.source ?? (url => new EventSource(url)))(api.eventUrl(runId, lastSequence))

  async function refresh() {
    if (closed) return
    if (refreshing) { refreshRequested = true; return }
    refreshing = true
    do {
      refreshRequested = false
      try {
        const snapshot = await load(runId)
        if (closed) return
        lastSequence = Math.max(lastSequence, snapshot.sequence ?? 0, ...(snapshot.events ?? []).map(event => event.sequence))
        callbacks.onSnapshot(snapshot)
        callbacks.onConnection(open ? 'connected' : 'disconnected')
      } catch {
        if (!closed) callbacks.onConnection('disconnected')
      }
    } while (refreshRequested && !closed)
    refreshing = false
  }

  function update(event: Event) {
    const message = event as MessageEvent
    let sequence = Number(message.lastEventId)
    if (!Number.isSafeInteger(sequence) || sequence < 1) {
      try { sequence = Number((JSON.parse(message.data) as { sequence?: number }).sequence) }
      catch { sequence = NaN }
    }
    if (Number.isSafeInteger(sequence) && sequence > lastSequence) {
      lastSequence = sequence
      void refresh()
    }
  }

  callbacks.onConnection('connecting')
  source.addEventListener('update', update)
  source.addEventListener('reset', () => { void refresh() })
  source.onopen = () => { open = true; void refresh() }
  source.onerror = () => { open = false; callbacks.onConnection('disconnected') }

  return () => {
    closed = true
    source.close()
  }
}
