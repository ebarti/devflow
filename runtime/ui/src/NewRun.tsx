import { useMemo, useState } from 'react'
import { api } from './api'
import type { NewRunRequest, ServiceInfo } from './model'

type Fields = Omit<NewRunRequest, 'command_id' | 'authorized_endpoint'>

function freshFields(): Fields {
  return {
    run_id: `run-${crypto.randomUUID()}`,
    work_id: '', issue_url: '', repository_key: '', goal: '',
    accepted_plan: '', publication_summary: '',
    base_ref: '', branch: '',
  }
}

export function NewRun({ service, onCreated, onBack }: { service: ServiceInfo | null; onCreated: (runId: string) => void; onBack: () => void }) {
  const [fields, setFields] = useState<Fields>(freshFields)
  const [commandId, setCommandId] = useState(() => crypto.randomUUID())
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const repositories = service?.repositories ?? []
  const intakeEnabled = service?.policy?.intake_enabled === true
  const selected = useMemo(() => repositories.find(repo => repo.key === fields.repository_key), [repositories, fields.repository_key])

  function set<K extends keyof Fields>(key: K, value: Fields[K]) {
    setFields(current => ({ ...current, [key]: value }))
    setCommandId(crypto.randomUUID())
    setError('')
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!selected || busy) return
    setBusy(true); setError('')
    try {
      const { accepted_plan, publication_summary, ...requestFields } = fields
      const result = await api.newRun({
        ...requestFields,
        ...(publication_summary?.trim() ? { publication_summary } : {}),
        ...(!intakeEnabled ? { accepted_plan } : {}),
        command_id: commandId, authorized_endpoint: 'published_unmerged',
        base_ref: fields.base_ref || selected.base_ref || '',
      })
      onCreated(result.run_id)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The run could not be submitted.')
    } finally { setBusy(false) }
  }

  return <div className="form-page">
    <div className="form-page__heading"><h1>New run</h1><p>{intakeEnabled ? 'Devflow investigates your goal, asks for needed clarification, and records a plan before continuing implementation automatically.' : 'Submit a goal with its already accepted plan.'}</p></div>
    {repositories.length ? <form onSubmit={event => void submit(event)} className="run-form">
      <div className="field-grid">
        <label>Run ID<input value={fields.run_id} onChange={event => set('run_id', event.target.value)} pattern="[A-Za-z0-9][A-Za-z0-9._-]*" maxLength={128} required autoComplete="off" /></label>
        <label>Work ID<input value={fields.work_id} onChange={event => set('work_id', event.target.value)} required autoComplete="off" /></label>
        <label className="field-wide">GitHub issue URL<input type="url" value={fields.issue_url} onChange={event => set('issue_url', event.target.value)} placeholder="https://github.com/owner/repo/issues/123" required autoComplete="url" /></label>
        <label>Repository<select value={fields.repository_key} onChange={event => {
          const repo = repositories.find(item => item.key === event.target.value)
          setFields(current => ({ ...current, repository_key: event.target.value, base_ref: repo?.base_ref ?? '', recovery_key: undefined }))
          setCommandId(crypto.randomUUID()); setError('')
        }} required><option value="">Select a repository</option>{repositories.map(repo => <option key={repo.key} value={repo.key}>{repo.label || repo.key}</option>)}</select></label>
        <label>Base ref<input value={fields.base_ref} readOnly aria-readonly="true" placeholder="Select a repository" /></label>
        <label className="field-wide">Branch<input value={fields.branch} onChange={event => set('branch', event.target.value)} required autoComplete="off" placeholder="feat/issue-description" /></label>
        {selected?.recovery_keys?.length ? <label className="field-wide">Approved recovery source<select value={fields.recovery_key ?? ''} onChange={event => set('recovery_key', event.target.value || undefined)}><option value="">None</option>{selected.recovery_keys.map(key => <option key={key} value={key}>{key}</option>)}</select></label> : null}
        <label className="field-wide">Goal<textarea value={fields.goal} onChange={event => set('goal', event.target.value)} rows={5} required /></label>
        <label className="field-wide">Publication summary<input value={fields.publication_summary ?? ''} onChange={event => set('publication_summary', event.target.value)} maxLength={120} placeholder="docs: investigate source completeness" aria-describedby="publication-summary-help" /></label>
        <small id="publication-summary-help" className="field-wide">Describe the change for the PR title and commit. Required for detailed goals; use a type such as docs: or fix:.</small>
        {!intakeEnabled ? <label className="field-wide">Accepted plan<textarea value={fields.accepted_plan ?? ''} onChange={event => set('accepted_plan', event.target.value)} rows={7} required /></label> : null}
      </div>
      <div className="submission-scope"><strong>Authorized endpoint</strong><span>Published, unmerged pull request</span><small>Repository, base, recovery sources, role models and checks are controlled by the service policy.</small></div>
      {error ? <p className="form-error" role="alert">{error}</p> : null}
      <div className="button-row"><button className="primary-button" type="submit" disabled={busy}>{busy ? 'Submitting…' : intakeEnabled ? 'Start investigation' : 'Start run'}</button><button className="text-button" type="button" onClick={onBack}>Back to runs</button></div>
    </form> : <div className="empty-state"><h2>No repositories available</h2><p>The local service has not provided an allowlisted repository. Refresh Settings after the service is configured.</p><button className="text-button" onClick={onBack}>Back to runs</button></div>}
  </div>
}
