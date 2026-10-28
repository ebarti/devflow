import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from './api'
import type { PauseControl, PauseIntent } from './api'
import { loadIntent, saveIntent } from './operationIntent'

const labels: Record<PauseControl['observed'], string> = {
  running: 'Running',
  pause_requested: 'Pause requested — queued start is being fenced',
  pausing: 'Pausing — preserving the queued run',
  paused: 'Paused',
  resuming: 'Resume requested — preparing the queued run',
  complete: 'Complete',
}

function retained(key: string): PauseIntent | null {
  const value = loadIntent<PauseIntent>(key)
  if (!value || !Number.isSafeInteger(value.expected_revision) || value.expected_revision < 0) return null
  if (value.action === 'pause' && value.epoch === undefined) return value
  if (value.action === 'resume' && Number.isSafeInteger(value.epoch) && value.epoch! > 0) return value
  return null
}

/** The request remains frozen until acceptance or a definite refusal is observed. */
export function PauseControls({ runId }: { runId: string }) {
  const key = `devflow.pause:${runId}`
  const [intent, setIntent] = useState<PauseIntent | null>(() => retained(key))
  const [status, setStatus] = useState<PauseControl | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  const refresh = useCallback(async () => {
    try {
      setStatus(await api.pauseControl(runId))
    } catch {
      setStatus(null)
    }
  }, [runId])

  useEffect(() => {
    void refresh()
    const timer = window.setInterval(() => void refresh(), 5000)
    return () => window.clearInterval(timer)
  }, [refresh])

  async function submit(action: 'pause' | 'resume') {
    if (busy) return
    const next = intent ?? (status ? {
      action,
      command_id: crypto.randomUUID(),
      expected_revision: status.revision,
      ...(action === 'resume' ? { epoch: status.epoch } : {}),
    } : null)
    if (!next || (!intent && (action === 'pause'
      ? status?.observed !== 'running' || status.pause_eligible !== true
      : status?.observed !== 'paused'))) return
    try {
      sessionStorage.setItem(key, JSON.stringify(next))
      if (sessionStorage.getItem(key) !== JSON.stringify(next)) throw new Error('intent not retained')
    } catch {
      setError('The request could not be retained in this tab, so it was not sent.')
      return
    }
    setIntent(next)
    setBusy(true)
    setError('')
    try {
      await api.pauseCommand(runId, next)
      saveIntent(key, null)
      setIntent(null)
      await refresh()
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 409) {
        saveIntent(key, null)
        setIntent(null)
        setError(`Request refused: ${cause.message}`)
        await refresh()
      } else {
        setError('The request outcome is unknown. Retry the same request or inspect its recorded state.')
      }
    } finally {
      setBusy(false)
    }
  }

  return <section className="section" aria-labelledby="pause-control-heading">
    <h2 id="pause-control-heading">Pause and resume</h2>
    <p>Only queued runs whose workflow has not started can be paused. The service checks eligibility again when requested.</p>
    {status ? <>
      <p role="status">{labels[status.observed] ?? 'Pause state unknown'} · Epoch {status.epoch} · Revision {status.revision}</p>
      {status.observed === 'pause_requested' || status.observed === 'pausing' ? <p>The pause is not complete yet.</p> : null}
      {status.notification_pending ? <p>Workflow notification is pending or its outcome is unknown.</p> : null}
      {status.blockers.length ? <div><p>Resume blockers recorded:</p><ul>{status.blockers.map(blocker => <li key={blocker}>{blocker}</li>)}</ul></div> : null}
      {status.observed === 'paused' ? <p>Resume checks current authority and the same checkpoint when requested.</p> : null}
    </> : <p role="status">Pause state is unavailable. Refresh to inspect the recorded state.</p>}
    {intent ? <>
      <p role="status">A {intent.action} request is retained with its original command ID, revision{intent.epoch ? ' and epoch' : ''}.</p>
      <button type="button" disabled={busy} onClick={() => void submit(intent.action)}>{busy ? 'Requesting…' : `Retry same ${intent.action} request`}</button>
    </> : <>
      {status?.observed === 'running' && status.pause_eligible === true ? <button type="button" disabled={busy} onClick={() => void submit('pause')}>{busy ? 'Requesting…' : 'Request pause'}</button> : null}
      {status?.observed === 'paused' ? <button type="button" disabled={busy || status.blockers.length > 0} onClick={() => void submit('resume')}>{busy ? 'Requesting…' : 'Request resume'}</button> : null}
    </>}
    {error ? <p className="form-error" role="alert">{error}</p> : null}
    {!status ? <button type="button" onClick={() => void refresh()}>Refresh pause state</button> : null}
  </section>
}
