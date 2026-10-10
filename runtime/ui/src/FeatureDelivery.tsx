import { useRef, useState } from 'react'
import { api, ApiError, safeWebUrl } from './api'
import { titleCase } from './format'
import type { RunDetail } from './model'

export function FeatureDelivery({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [revisionReason, setRevisionReason] = useState('')
  const [requestedRevision, setRequestedRevision] = useState(false)
  const revisionIntent = useRef<{ command_id: string; expected_revision: number; reason: string } | null>(null)
  const [successor, setSuccessor] = useState<string | null>(null)
  const intent = useRef<{ command_id: string; expected_revision: number } | null>(null)
  const delivery = run.feature_delivery
  if (!delivery) return null
  const planUrl = safeWebUrl(delivery.github_plan_url)
  const budget = delivery.repair_budget
  const plan = run.feature_plan

  async function continueFeature() {
    if (busy || run.projection_revision == null) return
    intent.current ??= { command_id: crypto.randomUUID(), expected_revision: run.projection_revision }
    setBusy(true)
    setError(null)
    try {
      const result = await api.continueFeature(run.id, intent.current)
      setSuccessor(result.run_id)
      await onRefresh()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not continue this feature.')
      if (cause instanceof ApiError && cause.status === 409) {
        intent.current = null
        await onRefresh()
      }
    } finally { setBusy(false) }
  }

  async function reviseFeaturePlan() {
    if (busy || !plan || (!revisionIntent.current && (!plan.can_revise || plan.expected_revision == null || !revisionReason.trim()))) return
    revisionIntent.current ??= { command_id: crypto.randomUUID(), expected_revision: plan.expected_revision!, reason: revisionReason }
    setBusy(true)
    setError(null)
    try {
      const result = await api.reviseFeaturePlan(run.id, revisionIntent.current)
      setRequestedRevision(true)
      setSuccessor(result.run_id)
      await onRefresh()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not request a plan revision.')
      if (cause instanceof ApiError && cause.status === 409) {
        revisionIntent.current = null
        await onRefresh()
      }
    } finally { setBusy(false) }
  }

  return <section className="section" aria-labelledby="feature-delivery-heading">
    <h2 id="feature-delivery-heading">Feature delivery</h2>
    {planUrl ? <p><a href={planUrl} target="_blank" rel="noopener noreferrer">Accepted plan and stack on GitHub</a></p> : <p>The delivery plan is being prepared from the GitHub issue.</p>}
    <p>Product repairs: {budget.used} of {budget.maximum}. The allowance carries across continuations and plan corrections.</p>
    {plan ? <>
      {plan.plan_identity.comment_id != null && plan.plan_identity.plan_digest ? <p>GitHub plan revision {plan.plan_identity.plan_revision} · {titleCase(plan.phase ?? 'accepted')}</p> : <p>Plan state: {titleCase(plan.phase ?? 'planning')}.</p>}
      {plan.reason ? <p>{plan.reason}</p> : null}
      {plan.affected_chunks.length > 0 ? <p>Affected chunks: {plan.affected_chunks.join(', ')}.</p> : null}
      {Object.entries(plan.child_plan_links).length > 0 ? <ul>{Object.entries(plan.child_plan_links).map(([stream, link]) => {
        const url = safeWebUrl(link.url)
        return url ? <li key={stream}><a href={url} target="_blank" rel="noopener noreferrer">{stream} workstream plan</a></li> : null
      })}</ul> : null}
      <details><summary>Execution scope and expected files</summary>
        <p>Allowed roots: {plan.authority.allowed_roots.join(', ') || 'None'}. Allowed files: {plan.authority.allowed_files.join(', ') || 'None'}.</p>
        <p>Protected paths: {plan.authority.protected_paths.join(', ') || 'None'}.</p>
        <ul>{Object.entries(plan.expected_paths).map(([chunk, paths]) => <li key={chunk}>{chunk}: {paths.join(', ')}</li>)}</ul>
      </details>
      {plan.evidence.length > 0 ? <details><summary>Revision evidence ({plan.evidence.length})</summary><ul>{plan.evidence.map(item => <li key={item.path}><code>{item.path}</code></li>)}</ul></details> : null}
    </> : null}
    {budget.learning_required ? <p className="inline-alert">Learning required · retain this delivery for a later review of the Devflow prompts.</p> : null}
    {delivery.workers.length > 0 ? <div className="table-scroll"><table className="feature-workers-table">
      <thead><tr><th>Chunk</th><th>Stage</th><th>Status</th><th>Child issue</th><th>Pull request</th></tr></thead>
      <tbody>{delivery.workers.map(worker => {
        const prUrl = safeWebUrl(worker.pull_request?.url)
        const childUrl = safeWebUrl(worker.issue_url)
        return <tr key={worker.run_id}><td><a href={`/runs/${encodeURIComponent(worker.run_id)}`}>{worker.chunk_id}</a></td><td>{['build', 'chunk'].includes(worker.kind) ? 'Implementation' : 'Integration and verification'}</td><td>{titleCase(worker.phase)}</td><td>{childUrl ? <a href={childUrl} target="_blank" rel="noopener noreferrer">{worker.workstream_id ?? 'Workstream'}</a> : 'Pending'}</td><td>{prUrl ? <a href={prUrl} target="_blank" rel="noopener noreferrer">#{worker.pull_request?.number}</a> : 'Pending'}</td></tr>
      })}</tbody>
    </table></div> : null}
    {delivery.can_continue && ['blocked', 'cancelled'].includes(run.outcome ?? '') && !successor ? <button className="primary-button" type="button" disabled={busy || revisionIntent.current != null || run.projection_revision == null} onClick={() => void continueFeature()}>{busy ? 'Continuing…' : 'Continue this feature'}</button> : null}
    {plan && (plan.can_revise || revisionIntent.current != null) && !successor ? <form className="steering-form feature-revision-form" onSubmit={event => { event.preventDefault(); void reviseFeaturePlan() }}>
      <label htmlFor="revision-reason">Planning defect</label>
      <textarea id="revision-reason" rows={3} maxLength={4000} value={revisionReason} disabled={busy || revisionIntent.current != null} onChange={event => setRevisionReason(event.target.value)} placeholder="Describe the flawed assumption, dependency, or verification gate." />
      <button className="primary-button" type="submit" disabled={busy || intent.current != null || plan.expected_revision == null || !revisionReason.trim()}>{busy ? 'Requesting…' : revisionIntent.current ? 'Retry plan revision request' : 'Request plan revision'}</button>
    </form> : null}
    {plan && !plan.can_revise && plan.reason_ineligible ? <p>Plan revision unavailable: {plan.reason_ineligible}.</p> : null}
    {successor ? <p role="status">{requestedRevision ? 'Plan revision requested.' : 'Continuation started.'} <a href={`/runs/${encodeURIComponent(successor)}`}>Open the coordinating run</a>.</p> : null}
    {error ? <p role="alert">{error}</p> : null}
  </section>
}
