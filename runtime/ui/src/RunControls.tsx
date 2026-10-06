import { useRef, useState } from 'react'
import { api, ApiError } from './api'
import { time } from './format'
import type { RunDetail } from './model'

export function RunControls({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const [message, setMessage] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [receipt, setReceipt] = useState('')
  const [commandId, setCommandId] = useState(() => crypto.randomUUID())
  const intent = useRef<{ kind: 'archive' | 'steer'; command_id: string; expected_revision: number; message?: string; archived?: boolean } | null>(null)
  const [uncertain, setUncertain] = useState(false)
  async function submit(kind: 'archive' | 'steer') {
    if (busy || run.projection_revision == null) return
    setBusy(true); setError(''); setReceipt('')
    try {
      intent.current ??= { kind, command_id: commandId, expected_revision: run.projection_revision, ...(kind === 'archive' ? { archived: !run.archived } : { message: message.trim() }) }
      const frozen = intent.current
      if (frozen.kind !== kind) return
      const common = { command_id: frozen.command_id, expected_revision: frozen.expected_revision }
      if (kind === 'archive') await api.archive(run.id, { ...common, archived: frozen.archived! })
      else await api.steer(run.id, { ...common, message: frozen.message! })
      const wasArchive = frozen.archived
      intent.current = null; setUncertain(false)
      setCommandId(crypto.randomUUID()); setMessage('')
      setReceipt(kind === 'archive' ? wasArchive ? 'Task archived. Evidence and statistics are retained.' : 'Task restored.' : 'Instructions queued for the next role launch. The current role continues.')
      await onRefresh()
    } catch (cause) {
      const unknown = !(cause instanceof ApiError) || cause.status === 0 || cause.status >= 500
      setUncertain(unknown)
      if (!unknown) { intent.current = null; setCommandId(crypto.randomUUID()) }
      setError(unknown ? 'The command outcome is unknown. A retry keeps the exact original request.' : cause.message)
      await onRefresh()
    } finally { setBusy(false) }
  }
  const stopped = Boolean(run.outcome) || ['terminal', 'blocked', 'cancelled'].includes(run.execution_state ?? '')
  return <section className="section lower-section" aria-labelledby="steering-title"><h2 id="steering-title">Steer work</h2>
    <p className="subtle">Send instructions for the next role launch within the accepted scope. The active role continues; permissions, required checks and the delivery endpoint stay fixed. Use the plan-response controls above when a plan decision is pending.</p>
    {run.can_steer || uncertain && intent.current?.kind === 'steer' ? <div className="steering-form"><label htmlFor="steering-message">Instructions for the next phase</label><textarea id="steering-message" rows={3} maxLength={4000} value={message} disabled={busy || uncertain} onChange={e => { setMessage(e.target.value); setCommandId(crypto.randomUUID()) }} /><button className="primary-button" disabled={busy || !message.trim() || run.projection_revision == null} onClick={() => void submit('steer')}>{busy ? 'Sending…' : uncertain ? 'Retry same instructions' : 'Send instructions'}</button></div> : <p className="subtle">{stopped ? 'This task has stopped; its steering history remains available.' : 'Final QA has started or steering is unavailable. There is no guaranteed next role; cancellation remains available below while work is active.'}</p>}
    {run.steering?.length ? <ol className="steering-history">{run.steering.map(note => <li key={note.id}><p>{note.message}</p><small>{time(note.created_at)} · {note.included_in.length ? `Included in ${[...new Set(note.included_in.map(item => item.role))].join(', ')} launch input` : 'Queued · not yet included in a role launch'}</small></li>)}</ol> : null}
    {stopped ? <button className="outline-button" disabled={busy || run.projection_revision == null || uncertain && intent.current?.kind !== 'archive'} onClick={() => void submit('archive')}>{uncertain && intent.current?.kind === 'archive' ? 'Retry same archive command' : run.archived ? 'Restore task' : 'Archive task'}</button> : null}
    {receipt ? <p role="status">{receipt}</p> : null}{error ? <p className="inline-alert" role="alert">{error}</p> : null}
  </section>
}
