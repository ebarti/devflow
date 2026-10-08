import { useEffect, useState } from 'react'
import { api } from './api'
import { display, titleCase } from './format'
import type { RepositoryAccess, ServiceInfo } from './model'

function RepositoryPermissions({ access, onSaved }: { access: RepositoryAccess; onSaved: (access: RepositoryAccess) => void }) {
  const [draft, setDraft] = useState(access)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [saved, setSaved] = useState(false)
  const dirty = draft.repositories.some((item, index) => item.allowed !== access.repositories[index]?.allowed)
  useEffect(() => { setDraft(access); setError('') }, [access])

  async function save() {
    setBusy(true); setError(''); setSaved(false)
    try {
      const updated = await api.saveRepositoryAccess({
        expected_revision: draft.revision,
        allowed_repositories: draft.repositories.filter(item => item.allowed).map(item => item.name),
      })
      setDraft(updated); onSaved(updated); setSaved(true)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Repository access could not be saved.')
    } finally { setBusy(false) }
  }

  return <>
    <p className="settings-help">Choose which repositories Devflow can start new runs in. Existing runs keep their permissions.</p>
    <ul className="settings-repos">{draft.repositories.map(repo => <li key={repo.name}>
      <label><input type="checkbox" checked={repo.allowed} disabled={busy} onChange={event => {
        setDraft(current => ({ ...current, repositories: current.repositories.map(item => item.name === repo.name ? { ...item, allowed: event.target.checked } : item) }))
        setSaved(false)
      }} />{repo.name}</label>
    </li>)}</ul>
    {error ? <p className="form-error" role="alert">{error}</p> : null}
    <div className="button-row"><button type="button" className="primary-button" disabled={busy || !dirty} onClick={() => void save()}>{busy ? 'Saving…' : 'Save repository access'}</button>{saved ? <span role="status">Repository access saved.</span> : null}</div>
  </>
}

export function Settings({ service, loading, error, onRefresh, onRepositoryAccessSaved }: { service: ServiceInfo | null; loading: boolean; error: string; onRefresh: () => void; onRepositoryAccessSaved: (access: RepositoryAccess) => void }) {
  return <div className="settings-page">
    <div className="form-page__heading"><h1>Settings</h1><p>Repository access and local service information.</p></div>
    {error ? <div className="connection-banner connection-banner--bad" role="alert">{error} <button onClick={onRefresh}>Retry</button></div> : null}
    {loading && !service ? <p className="empty-section">Loading service information…</p> : null}
    {service ? <>
      <section className="section settings-section"><h2>Local service</h2><dl className="settings-list">
        <div><dt>Status</dt><dd>{titleCase(service.status)}</dd></div>
        <div><dt>Version</dt><dd>{display(service.version)}</dd></div>
        <div><dt>Temporal</dt><dd>{typeof service.temporal === 'string' ? titleCase(service.temporal) : titleCase(service.temporal?.status)}</dd></div>
        <div><dt>Parallel agents</dt><dd>{display(service.capacity?.active)} running · maximum {display(service.capacity?.limit)}<p className="settings-help">Maximum agents working at the same time. Additional agents wait for a slot.</p></dd></div>
      </dl></section>
      <section className="section settings-section"><h2>Allowed repositories</h2>{service.repository_access ? <RepositoryPermissions access={service.repository_access} onSaved={onRepositoryAccessSaved} /> : <p className="empty-section">Repository access settings are unavailable. Refresh after updating the local service.</p>}</section>
      <section className="section settings-section"><h2>Agent models</h2>{service.policy?.roles && Object.keys(service.policy.roles).length ? <div className="table-scroll"><table><thead><tr><th>Role</th><th>Model</th><th>Effort</th></tr></thead><tbody>{Object.entries(service.policy.roles).map(([role, config]) => <tr key={role}><td>{titleCase(role)}</td><td>{display(config.model)}</td><td>{display(config.effort)}</td></tr>)}</tbody></table></div> : <p className="empty-section">Agent models are unavailable.</p>}</section>
    </> : null}
    <button type="button" className="outline-button" onClick={onRefresh}>Refresh service information</button>
  </div>
}
