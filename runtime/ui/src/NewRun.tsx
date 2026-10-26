import { useEffect, useMemo, useState } from 'react'
import { api, type DurableOperation } from './api'
import type { NewRunRequest, ServiceInfo } from './model'

type Fields = Omit<NewRunRequest, 'command_id' | 'authorized_endpoint'>
const pendingSubmitKey = 'devflow.pending-submit-operation'
type PendingSubmit = { operation_id: string; request?: NewRunRequest }

function readPendingSubmit(): PendingSubmit | null {
  try {
    const raw = sessionStorage.getItem(pendingSubmitKey)
    if (!raw) return null
    try {
      const saved = JSON.parse(raw) as PendingSubmit
      if (typeof saved?.operation_id === 'string' && saved.request && typeof saved.request.command_id === 'string' && typeof saved.request.run_id === 'string') return saved
    } catch { /* Older sessions retained only the operation ID. */ }
    return raw.startsWith('submit:') ? { operation_id: raw } : null
  } catch { return null }
}

function rememberPendingSubmit(value: PendingSubmit | null) {
  try { if (value) sessionStorage.setItem(pendingSubmitKey, JSON.stringify(value)); else sessionStorage.removeItem(pendingSubmitKey) } catch { /* The live view still retains its intent. */ }
}

function freshFields(): Fields {
  return {
    run_id: `run-${crypto.randomUUID()}`,
    work_id: '', issue_url: '', repository_key: '', goal: '',
    accepted_plan: '', publication_summary: '',
    base_ref: '', branch: '',
  }
}

export function NewRun({ service, onCreated, onBack }: { service: ServiceInfo | null; onCreated: (runId: string) => void; onBack: () => void }) {
  const [saved] = useState<PendingSubmit | null>(readPendingSubmit)
  const [fields, setFields] = useState<Fields>(() => {
    if (!saved?.request) return freshFields()
    const { command_id: _command, authorized_endpoint: _endpoint, ...retained } = saved.request
    return retained
  })
  const [commandId, setCommandId] = useState(() => saved?.request?.command_id ?? crypto.randomUUID())
  const [intent, setIntent] = useState<NewRunRequest | null>(saved?.request ?? null)
  const [storedId, setStoredId] = useState<string | null>(saved?.operation_id ?? null)
  const [storedOperation, setStoredOperation] = useState<DurableOperation | null>(null)
  const [storedError, setStoredError] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(saved?.request ? 'A previously submitted request is retained. Check its operation before starting another.' : '')
  const access = service?.repository_access
  const repositories = (service?.repositories ?? []).filter(repo => !access || access.repositories.some(item => item.name.toLowerCase() === repo.github_repo?.toLowerCase() && item.allowed))
  const intakeEnabled = service?.policy?.intake_enabled === true
  const selected = useMemo(() => repositories.find(repo => repo.key === fields.repository_key), [repositories, fields.repository_key])

  async function inspectStored() {
    if (!storedId) return
    try { setStoredOperation(await api.getOperation(storedId)); setStoredError('') }
    catch (cause) { setStoredError(cause instanceof Error ? cause.message : 'Operation status is unavailable.') }
  }

  useEffect(() => {
    if (storedId && !intent) void inspectStored()
  // A saved handle is inspected on mount; the button below explicitly refreshes it.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [storedId, intent])

  function clearStored() {
    rememberPendingSubmit(null); setStoredId(null); setStoredOperation(null); setStoredError('')
  }

  function set<K extends keyof Fields>(key: K, value: Fields[K]) {
    setFields(current => ({ ...current, [key]: value }))
    setCommandId(crypto.randomUUID())
    setError('')
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (busy || (!selected && !intent)) return
    setBusy(true); setError('')
    try {
      const { accepted_plan, publication_summary, ...requestFields } = fields
      const request = intent ?? {
        ...requestFields,
        ...(publication_summary?.trim() ? { publication_summary } : {}),
        ...(!intakeEnabled ? { accepted_plan } : {}),
        command_id: commandId, authorized_endpoint: 'published_unmerged' as const,
        base_ref: fields.base_ref || selected?.base_ref || '',
      }
      setIntent(request)
      const operationId = storedId ?? `submit:${request.command_id}`
      rememberPendingSubmit({ operation_id: operationId, request }); setStoredId(operationId)
      const operation = await api.createOperation({
        operation_id: operationId, run_id: null, kind: 'submit', request,
      })
      if (operation.operation_id !== operationId) {
        rememberPendingSubmit({ operation_id: operation.operation_id, request })
        setStoredId(operation.operation_id)
      }
      if (operation.state === 'succeeded') {
        const result = operation.result as { run_id?: unknown } | null
        if (!result || typeof result.run_id !== 'string' || !result.run_id) {
          throw new Error('The completed operation has no run identity.')
        }
        clearStored()
        onCreated(result.run_id)
      } else if (operation.state === 'refused' || operation.state === 'failed') {
        setIntent(null); clearStored(); setCommandId(crypto.randomUUID())
        setError(operation.error || 'The run was not accepted.')
      } else {
        setError(`Operation ${operation.operation_id} is ${operation.state}. Retry this request or inspect its status.`)
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The run could not be submitted.')
    } finally { setBusy(false) }
  }

  const restored = storedId && !intent
  const restoredRunId = storedOperation?.state === 'succeeded' && storedOperation.result && typeof storedOperation.result === 'object' ? (storedOperation.result as { run_id?: unknown }).run_id : null

  return <div className="form-page">
    {restored ? <section aria-label="Previously submitted operation">
      <h2>Previously submitted operation</h2>
      <p>Operation {storedId}: {storedOperation?.state ?? 'status unavailable'}.</p>
      {storedError ? <p role="status">{storedError}</p> : null}
      <button type="button" onClick={() => void inspectStored()}>Refresh operation status</button>
      {storedOperation?.state === 'unknown' ? <button type="button" onClick={() => void api.reconcileOperation(storedId).then(setStoredOperation).catch(cause => setStoredError(String(cause)))}>Reconcile original operation</button> : null}
      {typeof restoredRunId === 'string' && restoredRunId ? <button type="button" onClick={() => { clearStored(); onCreated(restoredRunId) }}>Open submitted run</button> : null}
      {storedOperation?.state === 'refused' || storedOperation?.state === 'failed' ? <button type="button" onClick={clearStored}>Review a new request</button> : null}
    </section> : null}
    <div className="form-page__heading"><h1>New run</h1><p>{intakeEnabled ? 'Devflow investigates your goal, asks for needed clarification, and records a plan before continuing implementation automatically.' : 'Submit a goal with its already accepted plan.'}</p></div>
    {repositories.length || intent ? <form onSubmit={event => void submit(event)} className="run-form">
      <div className="field-grid">
        <label>Run ID<input disabled={busy || intent !== null || !!restored} value={fields.run_id} onChange={event => set('run_id', event.target.value)} pattern="[A-Za-z0-9][A-Za-z0-9._-]*" maxLength={128} required autoComplete="off" /></label>
        <label>Work ID<input disabled={busy || intent !== null || !!restored} value={fields.work_id} onChange={event => set('work_id', event.target.value)} required autoComplete="off" /></label>
        <label className="field-wide">GitHub issue URL<input type="url" disabled={busy || intent !== null || !!restored} value={fields.issue_url} onChange={event => set('issue_url', event.target.value)} placeholder="https://github.com/owner/repo/issues/123" required autoComplete="url" /></label>
        <label>Repository<select disabled={busy || intent !== null || !!restored} value={fields.repository_key} onChange={event => {
          const repo = repositories.find(item => item.key === event.target.value)
          setFields(current => ({ ...current, repository_key: event.target.value, base_ref: repo?.base_ref ?? '', recovery_key: undefined }))
          setCommandId(crypto.randomUUID()); setError('')
        }} required><option value="">Select a repository</option>{intent && !selected ? <option value={fields.repository_key}>Previously selected repository</option> : null}{repositories.map(repo => <option key={repo.key} value={repo.key}>{repo.label || repo.key}</option>)}</select></label>
        <label>Base ref<input disabled={busy || intent !== null || !!restored} value={fields.base_ref} readOnly aria-readonly="true" placeholder="Select a repository" /></label>
        <label className="field-wide">Branch<input disabled={busy || intent !== null || !!restored} value={fields.branch} onChange={event => set('branch', event.target.value)} required autoComplete="off" placeholder="feat/issue-description" /></label>
        {selected?.recovery_keys?.length ? <label className="field-wide">Approved recovery source<select disabled={busy || intent !== null || !!restored} value={fields.recovery_key ?? ''} onChange={event => set('recovery_key', event.target.value || undefined)}><option value="">None</option>{selected.recovery_keys.map(key => <option key={key} value={key}>{key}</option>)}</select></label> : null}
        <label className="field-wide">Goal<textarea disabled={busy || intent !== null || !!restored} value={fields.goal} onChange={event => set('goal', event.target.value)} rows={5} required /></label>
        <label className="field-wide">Publication summary<input disabled={busy || intent !== null || !!restored} value={fields.publication_summary ?? ''} onChange={event => set('publication_summary', event.target.value)} maxLength={120} placeholder="docs: investigate source completeness" aria-describedby="publication-summary-help" /></label>
        <small id="publication-summary-help" className="field-wide">Describe the change for the PR title and commit. Required for detailed goals; use a type such as docs: or fix:.</small>
        {!intakeEnabled ? <label className="field-wide">Accepted plan<textarea disabled={busy || intent !== null || !!restored} value={fields.accepted_plan ?? ''} onChange={event => set('accepted_plan', event.target.value)} rows={7} required /></label> : null}
      </div>
      <div className="submission-scope"><strong>Authorized endpoint</strong><span>Published, unmerged pull request</span><small>Repository, base, recovery sources, role models and checks are controlled by the service policy.</small></div>
      {error ? <p className="form-error" role="alert">{error}</p> : null}
      <div className="button-row"><button className="primary-button" type="submit" disabled={busy || !!restored}>{busy ? 'Submitting…' : intent ? 'Check submitted operation' : intakeEnabled ? 'Start investigation' : 'Start run'}</button><button className="text-button" type="button" onClick={onBack}>Back to runs</button></div>
    </form> : <div className="empty-state"><h2>No repositories available</h2><p>The local service has not provided an allowlisted repository. Refresh Settings after the service is configured.</p><button className="text-button" onClick={onBack}>Back to runs</button></div>}
  </div>
}
