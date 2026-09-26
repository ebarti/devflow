import { display, titleCase } from './format'
import type { ServiceInfo } from './model'

export function Settings({ service, loading, error, onRefresh }: { service: ServiceInfo | null; loading: boolean; error: string; onRefresh: () => void }) {
  return <div className="settings-page">
    <div className="form-page__heading"><h1>Settings</h1><p>Service information and admission policy.</p></div>
    {error ? <div className="connection-banner connection-banner--bad" role="alert">{error} <button onClick={onRefresh}>Retry</button></div> : null}
    {loading && !service ? <p className="empty-section">Loading service information…</p> : null}
    {service ? <>
      <section className="section settings-section"><h2>Local service</h2><dl className="settings-list">
        <div><dt>Status</dt><dd>{titleCase(service.status)}</dd></div>
        <div><dt>Version</dt><dd>{display(service.version)}</dd></div>
        <div><dt>Temporal</dt><dd>{typeof service.temporal === 'string' ? titleCase(service.temporal) : titleCase(service.temporal?.status)}</dd></div>
        <div><dt>Worker capacity</dt><dd>{display(service.capacity?.active)} active · {display(service.capacity?.queued)} queued · limit {display(service.capacity?.limit)}</dd></div>
      </dl></section>
      <section className="section settings-section"><h2>Allowed repositories</h2>{service.repositories?.length ? <ul className="settings-repos">{service.repositories.map(repo => <li key={repo.key}><strong>{display(repo.label, repo.key)}</strong><span>{repo.key}</span><span>Base {display(repo.base_ref)} · {display(repo.base_sha, 'SHA unobserved')}</span></li>)}</ul> : <p className="empty-section">No repositories are configured.</p>}</section>
      <section className="section settings-section"><h2>Role policy</h2>{service.policy?.roles && Object.keys(service.policy.roles).length ? <div className="table-scroll"><table><thead><tr><th>Role</th><th>Model</th><th>Effort</th></tr></thead><tbody>{Object.entries(service.policy.roles).map(([role, config]) => <tr key={role}><td>{titleCase(role)}</td><td>{display(config.model)}</td><td>{display(config.effort)}</td></tr>)}</tbody></table></div> : <p className="empty-section">Role policy is unavailable.</p>}</section>
    </> : null}
    <button type="button" className="outline-button" onClick={onRefresh}>Refresh service information</button>
  </div>
}
