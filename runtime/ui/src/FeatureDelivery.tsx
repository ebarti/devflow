import { useRef, useState } from 'react'
import { api, ApiError, safeWebUrl } from './api'
import { titleCase } from './format'
import type { RunDetail } from './model'

export function FeatureDelivery({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [successor, setSuccessor] = useState<string | null>(null)
  const intent = useRef<{ command_id: string; expected_revision: number } | null>(null)
  const delivery = run.feature_delivery
  if (!delivery) return null
  const planUrl = safeWebUrl(delivery.github_plan_url)
  const budget = delivery.repair_budget

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

  return <section className="section" aria-labelledby="feature-delivery-heading">
    <h2 id="feature-delivery-heading">Feature delivery</h2>
    {planUrl ? <p><a href={planUrl} target="_blank" rel="noopener noreferrer">Accepted plan and stack on GitHub</a></p> : <p>The delivery plan is being prepared from the GitHub issue.</p>}
    <p>Product repairs: {budget.used} of {budget.maximum}. The allowance carries across continuations.</p>
    {budget.learning_required ? <p className="inline-alert">Learning required · retain this delivery for a later review of the Devflow prompts.</p> : null}
    {delivery.workers.length > 0 ? <div className="table-scroll"><table>
      <thead><tr><th>Chunk</th><th>Stage</th><th>Status</th><th>Pull request</th></tr></thead>
      <tbody>{delivery.workers.map(worker => {
        const prUrl = safeWebUrl(worker.pull_request?.url)
        return <tr key={worker.run_id}><td><a href={`/runs/${encodeURIComponent(worker.run_id)}`}>{worker.chunk_id}</a></td><td>{worker.kind === 'build' ? 'Implementation' : 'Integration and verification'}</td><td>{titleCase(worker.phase)}</td><td>{prUrl ? <a href={prUrl} target="_blank" rel="noopener noreferrer">#{worker.pull_request?.number}</a> : 'Pending'}</td></tr>
      })}</tbody>
    </table></div> : null}
    {delivery.can_continue && ['blocked', 'cancelled'].includes(run.outcome ?? '') && !successor ? <button className="primary-button" type="button" disabled={busy || run.projection_revision == null} onClick={() => void continueFeature()}>{busy ? 'Continuing…' : 'Continue this feature'}</button> : null}
    {successor ? <p role="status">Continuation started. <a href={`/runs/${encodeURIComponent(successor)}`}>Open the coordinating run</a>.</p> : null}
    {error ? <p role="alert">{error}</p> : null}
  </section>
}
