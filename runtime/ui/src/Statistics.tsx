import { useEffect, useState } from 'react'
import { api } from './api'
import type { Cohort, Statistics as StatisticsData } from './model'

function label(cohort: Cohort) {
  const runtime = cohort.release || (cohort.revision ? `${cohort.revision.slice(0, 8)}${cohort.local_digest ? ` · local ${cohort.local_digest.slice(0, 8)}` : ' · unreleased'}` : 'Unknown historical revision')
  return `${runtime} · ${cohort.provider === 'fake' ? 'simulation' : cohort.provider || 'provider unknown'}`
}
const number = (n: number | null) => n == null ? 'Unknown' : n.toLocaleString()

export function Statistics() {
  const [data, setData] = useState<StatisticsData | null>(null)
  const [error, setError] = useState('')
  const [refresh, setRefresh] = useState(0)
  useEffect(() => {
    let alive = true
    void api.getStatistics().then(next => { if (alive) { setData(next); setError('') } }).catch(cause => { if (alive) setError(cause instanceof Error ? cause.message : 'Could not load statistics.') })
    return () => { alive = false }
  }, [refresh])
  return <section className="statistics-page" aria-labelledby="statistics-title">
    <div className="board-toolbar"><div><h2 id="statistics-title">Delivery efficiency</h2><p>Compare observed outcomes across tagged releases and local revisions.</p></div><button className="outline-button" onClick={() => setRefresh(n => n + 1)}>Refresh statistics</button></div>
    {error ? <p className="inline-alert" role="alert">{error}{data ? ' · showing the last observed sample' : ''}</p> : null}
    {!data && !error ? <p>Loading statistics…</p> : null}
    {data ? <><div className="metric-grid"><div><span>Recorded runs</span><strong>{data.total_runs}</strong><small>Includes archived tasks and failures</small></div><div><span>Delivered</span><strong>{data.cohorts.reduce((n, c) => n + c.delivered, 0)}</strong><small>Reached the authorized endpoint</small></div><div><span>Delivered without repairs</span><strong>{data.cohorts.reduce((n, c) => n + c.first_pass_delivered, 0)}</strong><small>No recovery or superseded predecessor</small></div><div><span>Blocked</span><strong>{data.cohorts.reduce((n, c) => n + c.blocked, 0)}</strong><small>Retained in the success-rate denominator</small></div></div>
    <h3>Release and revision comparison</h3><div className="table-scroll"><table className="cohort-table"><thead><tr><th>Runtime cohort</th><th>Runs / terminal</th><th>Delivered / first pass</th><th>Success rate</th><th>Repairs</th><th>Median elapsed</th><th>Observed tokens</th><th>Observed cost</th></tr></thead><tbody>{data.cohorts.map((c, i) => <tr key={`${c.revision}:${c.local_digest}:${i}`}><td><strong>{label(c)}</strong><small>{c.revision ? c.revision.slice(0, 12) : 'Identity not recorded at admission'}</small></td><td>{c.runs} / {c.terminal}<small>{c.active} active · {c.blocked} blocked · {c.cancelled} cancelled</small></td><td>{c.delivered} / {c.first_pass_delivered}</td><td>{c.success_rate == null ? 'No terminal sample' : `${(c.success_rate * 100).toFixed(1)}%`}</td><td>{c.repairs}</td><td>{c.median_duration_seconds == null ? 'Unknown' : `${(c.median_duration_seconds / 60).toFixed(1)} min`}<small>{c.duration_observations} observations</small></td><td>{number(c.observed_tokens)}<small>{c.token_observations}/{c.attempts} attempts observed</small></td><td>{c.observed_cost_usd == null ? 'Unknown' : `$${c.observed_cost_usd.toFixed(2)}`}<small>{c.cost_observations}/{c.attempts} attempts observed</small></td></tr>)}</tbody></table></div>
    {!data.cohorts.length ? <p className="empty-section">No recorded runs yet.</p> : null}
    <h3>Role usage</h3>{data.cohorts.map((c, i) => <div key={i} className="role-usage"><h4>{label(c)}</h4>{Object.keys(c.roles).length ? <dl>{Object.entries(c.roles).map(([role, usage]) => <div key={role}><dt>{role}</dt><dd>{usage.attempts} attempts · {usage.token_observations ? number(usage.observed_tokens) : 'Unknown'} tokens · {usage.token_observations}/{usage.attempts} observed</dd></div>)}</dl> : <p className="subtle">No role attempts recorded.</p>}</div>)}
    <p className="statistics-definitions">{data.definitions}</p></> : null}
  </section>
}
