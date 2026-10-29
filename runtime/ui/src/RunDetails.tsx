import { useRef, useState } from 'react'
import { api, ApiError, safeWebUrl } from './api'
import { CheckIcon, ExternalIcon } from './icons'
import { display, time, timelineTime, titleCase, tokens, tone } from './format'
import type { ActivityEvent, CheckState, Decision, IntakePlan, PhaseGate, RoleState, RunDetail } from './model'
import { RunControls } from './RunControls'
import { DurableOperations } from './DurableOperations'
import { loadIntent, saveIntent } from './operationIntent'
import { FeatureDelivery } from './FeatureDelivery'
import { runStatus } from './runStatus'

function State({ value }: { value: string | null | undefined }) {
  return <span className={`state state--${tone(value)}`}><span className="state__mark" aria-hidden="true">{tone(value) === 'good' ? <CheckIcon /> : null}</span>{titleCase(value)}</span>
}

function Workstreams({ plan }: { plan: IntakePlan }) {
  if (!plan.workstreams?.length) return null
  return <div><h3>Workstreams and complete chunks</h3>{plan.workstreams.map(stream => <div key={stream.id}>
    <h4>{stream.title}{stream.issue_number ? ` · Issue #${stream.issue_number}` : ' · New sub-issue'}</h4>
    <ol>{stream.chunks.map(chunk => <li key={chunk.id}><strong>{chunk.title}</strong><p>{chunk.acceptance.join('; ')}</p><small>{chunk.depends_on.length ? `Requires: ${chunk.depends_on.join(', ')}` : 'Can start independently'}</small></li>)}</ol>
  </div>)}</div>
}

function PhaseStrip({ gates }: { gates: PhaseGate[] | null | undefined }) {
  if (!gates?.length) return <section className="phase-strip phase-strip--empty" aria-label="Workflow phase gates"><p className="empty-section">No phase gate observations are available.</p></section>
  let leadingCompleted = 0
  for (const gate of gates) {
    if (tone(gate.state) !== 'good') break
    leadingCompleted += 1
  }
  const completeSegments = Math.max(0, leadingCompleted - 1)
  return <section className="phase-strip" aria-label="Workflow phase gates">
    {completeSegments > 0 ? <span className="phase-strip__progress" style={{ width: `calc((100% - 2 * var(--phase-end)) * ${completeSegments / (gates.length - 1)})` }} aria-hidden="true" /> : null}
    {gates.map((gate, index) => {
      const state = tone(gate.state)
      return <div key={`${gate.id}-${index}`} className={`phase phase--${state}`} title={gate.detail ?? undefined}>
        <span className="phase__circle" aria-hidden="true">{state === 'good' ? <CheckIcon /> : null}</span>
        <span className="phase__label">{gate.label || titleCase(gate.id)}</span>
        <span className="sr-only">: {titleCase(gate.state)}</span>
      </div>
    })}
  </section>
}

function Facts({ run }: { run: RunDetail }) {
  const url = safeWebUrl(run.pull_request?.url)
  const tracker = run.tracker
  const trackerLabel = tracker?.conflict ? 'Conflict' : tracker?.state === 'unconfigured' ? 'Unconfigured' : tracker?.pending || tracker?.state === 'pending' ? 'Readback pending' : tracker?.state === 'consistent' ? 'Readback confirmed' : 'Unresolved'
  const trackerTone = tracker?.conflict ? 'bad' : tracker?.state === 'consistent' ? 'neutral' : 'waiting'
  return <dl className="facts" aria-label="Run facts">
    {run.feature_delivery ? <div><dt>Feature stack</dt><dd>{run.pull_request?.stack_id ? `Stack #${run.pull_request.stack_id} · ` : ''}{run.pull_request?.pull_requests?.length ?? 0} published PRs</dd></div> : <><div><dt>Candidate</dt><dd className="mono" title={run.candidate?.head ?? undefined}>{display(run.candidate?.head, 'Not available')}</dd></div>
    <div><dt>{run.feature ? 'This run’s PR' : 'Pull request'}</dt><dd>{url ? <a href={url} target="_blank" rel="noopener noreferrer">{run.pull_request?.number ? `#${run.pull_request.number}` : 'Open pull request'} <ExternalIcon /></a> : display(run.pull_request?.number, 'Not published')}</dd></div></>}
    <div><dt>Authorized endpoint</dt><dd><span className="outline-pill">{titleCase(run.authorized_endpoint)}</span></dd></div>
    {run.feature ? <>
      <div><dt>Feature status</dt><dd>{run.feature.status}</dd></div>
      <div><dt>Project sync</dt><dd>{run.feature.mirror?.state === 'consistent' ? `Confirmed ${time(run.feature.mirror.checked_at)}` : 'Pending'}{run.feature.mirror?.last_error ? `: ${run.feature.mirror.last_error}` : ''}</dd></div>
      {run.feature.issue_observation ? <div><dt>GitHub issue</dt><dd>{titleCase(run.feature.issue_observation.observation?.state)}{run.feature.issue_observation.observation?.state_reason ? ` · ${titleCase(run.feature.issue_observation.observation.state_reason)}` : ''} · checked {time(run.feature.issue_observation.checked_at)}{run.feature.issue_observation.error ? ` · ${run.feature.issue_observation.error}` : ''}</dd></div> : null}
      <div><dt>Attempt status</dt><dd>{runStatus(run)}</dd></div>
      <div><dt>Run phase</dt><dd>{titleCase(run.phase)}</dd></div>
      {run.feature.pull_requests.map(pr => {
        const currentUrl = safeWebUrl(pr.url)
        return <div key={pr.url}><dt>Current feature PR</dt><dd>{currentUrl ? <a href={currentUrl} target="_blank" rel="noopener noreferrer">#{currentUrl.split('/').pop()} <ExternalIcon /></a> : 'PR'} · {pr.observation?.state ?? 'Not yet observed'} · checked {time(pr.checked_at)}{pr.error ? ` · ${pr.error}` : ''}</dd></div>
      })}
    </> : <div><dt>Tracker</dt><dd><span className={`soft-pill soft-pill--${trackerTone}`}>{trackerLabel}</span></dd></div>}
  </dl>
}

function RoleTable({ roles }: { roles: RoleState[] | null | undefined }) {
  return <section className="section role-section" aria-labelledby="roles-heading">
    <h2 id="roles-heading">Roles</h2>
    <div className="table-scroll"><table>
      <thead><tr><th>Role</th><th>State</th><th>Session</th><th>Tokens</th></tr></thead>
      <tbody>{roles?.length ? roles.map((role, index) => <tr key={`${role.role}-${role.attempt_id ?? index}`}>
        <td>{titleCase(role.role)}</td>
        <td><State value={role.state} /></td>
        <td className="mono">{display(role.session_id, 'Unknown')}</td>
        <td>{tokens(role.usage?.total_tokens)}</td>
      </tr>) : <tr><td className="empty-row" colSpan={4}>No role attempts have been recorded.</td></tr>}</tbody>
    </table></div>
  </section>
}

function activityLabel(event: ActivityEvent): string {
  if (event.type === 'published') return 'Pull request published on GitHub'
  if (event.type === 'candidate_ready') return 'Before-PR checks passed; publishing pull request'
  return display(event.message, titleCase(event.type))
}

function Activity({ events }: { events: ActivityEvent[] | null | undefined }) {
  const ordered = [...(events ?? [])].sort((a, b) => b.sequence - a.sequence)
  return <section className="section activity-section" aria-labelledby="activity-heading">
    <h2 id="activity-heading">Activity</h2>
    {ordered.length ? <ol className="timeline">{ordered.map(event => <li key={event.sequence}>
      <span className="timeline__dot" aria-hidden="true" />
      <time dateTime={event.timestamp ?? undefined}>{timelineTime(event.timestamp)}</time>
      <span>{activityLabel(event)}</span>
      <span className="timeline__sequence">#{event.sequence}</span>
    </li>)}</ol> : <p className="empty-section">No activity has been recorded yet.</p>}
  </section>
}

function QualityRow({ label, value }: { label: string; value: CheckState | null | undefined }) {
  return <div className="quality-row"><span>{label}</span><State value={value?.state} /><span className="quality-row__detail">{display(value?.detail, 'Evidence not available')}</span></div>
}

function BlockingFindings({ run }: { run: RunDetail }) {
  if (!run.error) return null
  const findings = ['review', 'verify'].flatMap(name => {
    const gate = run.checks?.[name === 'review' ? 'review' : 'qa']
    const role = [...(run.roles ?? [])].reverse().find(item => item.role === name)
    return gate?.state === 'failed' ? role?.findings ?? [] : []
  })
  if (!findings.length) return null
  return <section className="inline-alert" aria-label="Blocking findings">
    <strong>What blocked this run</strong>
    <ul>{findings.map((finding, index) => <li key={index}>{finding}</li>)}</ul>
    {run.error === 'repair limit exhausted' ? <p>The allowed repair budget ended before these findings were resolved.</p> : null}
  </section>
}

function EvidenceAndQuality({ run }: { run: RunDetail }) {
  const adjudication = run.investigation_adjudication
  return <section className="section lower-section" aria-labelledby="quality-heading">
    <h2 id="quality-heading">Quality and evidence</h2>
    <div className="quality-list">
      <QualityRow label="Review" value={run.checks?.review} />
      <QualityRow label="QA" value={run.checks?.qa} />
      <QualityRow label="Local checks" value={run.checks?.local} />
      <QualityRow label="CI" value={run.checks?.ci} />
    </div>
    {adjudication ? <div aria-label="Independent investigation disposition">
      <h3>Independent investigation disposition</h3>
      <p>Raw QA status: {adjudication.raw_status}. {adjudication.disposition.accepted_baseline_medium} accepted baseline Medium findings; {adjudication.disposition.remaining_blocker_high} remaining Blocker or High findings.</p>
      <ol><li>{adjudication.disposition.finding1}</li><li>{adjudication.disposition.finding2}</li><li>{adjudication.disposition.finding3}</li></ol>
      <h4>Retained raw findings</h4><ul>{adjudication.raw_findings.map((finding, index) => <li key={index}>{finding}</li>)}</ul>
      <p className="subtle">Completed gates apply to unchanged source. No additional native execution was performed.</p>
    </div> : null}
    {run.evidence?.length ? <ul className="evidence-list">{run.evidence.map(item => {
      const url = safeWebUrl(item.url)
      return <li key={item.id}>{url ? <a href={url} target="_blank" rel="noopener noreferrer">{display(item.label, item.id)} <ExternalIcon /></a> : display(item.label, item.id)} <small>{titleCase(item.state)}</small></li>
    })}</ul> : <p className="empty-section">No indexed evidence is available.</p>}
  </section>
}

function GateDetails({ gates }: { gates: PhaseGate[] | null | undefined }) {
  return <section className="section lower-section" aria-labelledby="gate-details-heading">
    <h2 id="gate-details-heading">Phase gates</h2>
    {gates?.length ? <div className="table-scroll"><table className="gate-table"><thead><tr><th>Gate</th><th>State</th><th>Evidence</th></tr></thead><tbody>{gates.map(gate => <tr key={gate.id}>
      <td>{gate.label}</td><td><State value={gate.state} /></td><td>{display(gate.detail, gate.evidence_refs?.length ? gate.evidence_refs.join(', ') : 'Not available')}</td>
    </tr>)}</tbody></table></div> : <p className="empty-section">No gate observations are available.</p>}
  </section>
}

function Operations({ run }: { run: RunDetail }) {
  return <section className="section lower-section" aria-labelledby="operations-heading">
    <h2 id="operations-heading">Operations</h2>
    <dl className="operations-grid">
      <div><dt>Service active roles</dt><dd>{display(run.capacity?.active)}</dd></div>
      <div><dt>Run queued</dt><dd>{run.queued == null ? 'Unknown' : run.queued ? 'Yes' : 'No'}</dd></div>
      <div><dt>Service capacity</dt><dd>{display(run.capacity?.limit)}</dd></div>
      <div><dt>Process/resource settlement</dt><dd>{titleCase(run.cleanup)}</dd></div>
      <div><dt>Retained worktrees</dt><dd>{run.retained_worktrees?.state === 'observed' ? run.retained_worktrees.count : 'Not observed'}{run.retained_worktrees?.roots.length ? <ul>{run.retained_worktrees.roots.map(root => <li key={root.path}>{root.path}{root.reason ? ` · Original retention: ${root.reason}` : ''}</li>)}</ul> : null}</dd></div>
      <div><dt>Tracker desired</dt><dd>{display(run.tracker?.desired)}</dd></div>
      <div><dt>Tracker observed</dt><dd>{display(run.tracker?.observed)}</dd></div>
      <div><dt>{run.feature ? "Historical tracker receipt" : "Tracker sync"}</dt><dd>{titleCase(run.tracker?.state)}</dd></div>
      <div><dt>Tracker readback</dt><dd>{time(run.tracker?.readback_at)}</dd></div>
      <div><dt>Candidate revision</dt><dd>{display(run.candidate?.revision)}</dd></div>
      <div><dt>Base SHA</dt><dd className="mono">{display(run.candidate?.base)}</dd></div>
      <div><dt>Content digest</dt><dd className="mono">{display(run.candidate?.content_digest)}</dd></div>
      <div><dt>Policy digest</dt><dd className="mono">{display(run.candidate?.policy_digest)}</dd></div>
      <div><dt>Environment digest</dt><dd className="mono">{display(run.candidate?.environment_digest)}</dd></div>
      <div><dt>Protocol revision</dt><dd>{display(run.protocol_revision)}</dd></div>
    </dl>
    {run.roles?.length ? <div className="role-provenance"><h3>Role provenance</h3><ul>{run.roles.map((role, index) => <li key={`${role.role}-${role.attempt_id ?? index}`}><strong>{titleCase(role.role)}</strong><span>Model {display(role.model, 'unobserved')} · Effort {display(role.effort, 'unobserved')} · Iteration {display(role.iteration, 'unknown')} · Cleanup {display(role.cleanup)} · Last activity {time(role.last_activity_at)}{role.summary ? ` · ${role.summary}` : ''}{role.findings?.length ? ` · Findings: ${role.findings.join('; ')}` : ''}</span></li>)}</ul></div> : null}
    {run.tracker?.conflict ? <p className="inline-alert">Tracker conflict: {run.tracker.conflict}</p> : null}
  </section>
}

function UsageSection({ run }: { run: RunDetail }) {
  const usage = run.usage
  return <section className="section lower-section" aria-labelledby="usage-heading">
    <h2 id="usage-heading">Usage</h2>
    <dl className="usage-grid">
      <div><dt>Input</dt><dd>{tokens(usage?.input_tokens)}</dd></div>
      <div><dt>Cache read</dt><dd>{tokens(usage?.cache_read_tokens ?? usage?.cached_input_tokens)}</dd></div>
      <div><dt>Cache creation</dt><dd>{tokens(usage?.cache_creation_tokens)}</dd></div>
      <div><dt>Output</dt><dd>{tokens(usage?.output_tokens)}</dd></div>
      <div><dt>Reasoning</dt><dd>{tokens(usage?.reasoning_tokens)}</dd></div>
      <div><dt>Total</dt><dd>{tokens(usage?.total_tokens)}</dd></div>
    </dl>
    <p className="subtle">{usage?.source === 'role_attempts' ? 'Totals sum available role-attempt readings; missing values remain unknown.' : `Telemetry: ${display(usage?.status)} · Observed ${time(usage?.observed_at)}`}</p>
    {usage?.gaps?.length ? <p className="inline-alert">Unknown: {usage.gaps.join(', ')}</p> : null}
  </section>
}

function DecisionCard({ run, decision, onRefresh }: { run: RunDetail; decision: Decision; onRefresh: () => Promise<void> }) {
  const intentKey = `devflow.decision:${run.id}:${decision.id}:${decision.revision}`
  const [saved] = useState(() => loadIntent<Parameters<typeof api.answer>[1]>(intentKey))
  const [choice, setChoice] = useState(() => decision.kind === 'question' ? '' : saved?.answer ?? '')
  const [freeText, setFreeText] = useState(() => decision.kind === 'question' ? saved?.answer ?? '' : saved?.response ?? '')
  const [commandId, setCommandId] = useState(() => saved?.command_id ?? crypto.randomUUID())
  const [intent, setIntent] = useState<Parameters<typeof api.answer>[1] | null>(saved)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(saved ? 'A previous decision request is retained. Check its operation.' : '')
  const [stale, setStale] = useState(false)

  const plan = decision.kind === 'plan' ? run.intake?.plans.find(item => item.revision === decision.plan_revision)?.content : null
  const notification = run.question_notifications?.find(item => item.decision_id === decision.id && item.decision_revision === decision.revision)
  const answerText = decision.kind === 'question' ? freeText.trim() || choice : choice
  const valid = Boolean(answerText) && (decision.kind !== 'plan' || choice !== 'change' || Boolean(freeText.trim()))

  async function answer() {
    if (run.execution_retired || busy || stale || (!intent && (!valid || run.protocol_revision == null || decision.candidate_revision == null))) return
    const request = intent ?? {
      command_id: commandId, expected_revision: run.protocol_revision!,
      decision_id: decision.id, decision_revision: decision.revision,
      candidate_revision: decision.candidate_revision!, answer: answerText,
      ...(decision.kind === 'plan' && choice === 'change' ? { response: freeText.trim() } : {}),
    }
    saveIntent(intentKey, request); setIntent(request); setBusy(true); setError('')
    try {
      const operation = await api.createOperation({
        operation_id: `decision:${request.command_id}`, run_id: run.id, kind: 'decision', request,
      })
      if (operation.state === 'succeeded') {
        saveIntent(intentKey, null); setIntent(null)
        await onRefresh()
      } else if (operation.state === 'refused' || operation.state === 'failed') {
        saveIntent(intentKey, null); setIntent(null); setStale(true)
        setError(operation.error || 'The decision was refused; review the latest run.')
        await onRefresh()
      } else {
        setError(`Operation ${operation.operation_id} is ${operation.state}. Check durable operations before retrying.`)
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not submit the decision.')
    } finally { setBusy(false) }
  }

  return <section className="decision-card" aria-labelledby={`decision-${decision.id}`}>
    <div className="decision-card__heading"><h2 id={`decision-${decision.id}`}>{decision.kind === 'question' ? 'Clarification needed' : decision.kind === 'plan' ? 'Review Devflow plan' : decision.kind === 'merge' ? 'Merge feature' : 'Decision needed'}</h2><span>Revision {decision.revision}</span></div>
    <p>{decision.prompt}</p>
    {decision.kind === 'merge' ? <p>This authorizes merging {run.pull_request?.stack_id ? `stack #${run.pull_request.stack_id}` : 'the feature PR'} with {(run.pull_request?.pull_requests ?? []).map(pr => `#${pr.number}`).join(', ')} and closing the feature and its workstream issues after GitHub confirms the merge.</p> : null}
    {decision.blocker ? <div className="intake-plan"><h3>Why input is needed</h3><p>{decision.blocker.unknown}</p><p>{decision.blocker.why_no_safe_default}</p><h3>Evidence checked</h3><ul>{decision.blocker.evidence_checked.map((item, index) => <li key={index}>{item}</li>)}</ul></div> : null}
    {notification ? <p className="subtle">Question callback: {titleCase(notification.state)}. {notification.state === 'queued' ? 'The native queue acknowledged the message; display and an answer are not confirmed.' : notification.state === 'unavailable' ? 'No originating thread is bound. Answer here.' : notification.state === 'unknown' ? 'The queue effect is uncertain and will not be retried automatically.' : notification.receipt?.reason ?? ''}</p> : null}
    {plan ? <div className="intake-plan"><h3>Scope</h3><p>{plan.scope}</p><h3>Steps</h3><ol>{plan.steps.map((item, index) => <li key={index}>{item}</li>)}</ol><h3>Verification</h3><ul>{plan.verification.map((item, index) => <li key={index}>{item}</li>)}</ul><h3>Acceptance</h3><ul>{plan.acceptance.map((item, index) => <li key={index}>{item}</li>)}</ul></div> : null}
    {plan ? <Workstreams plan={plan} /> : null}
    {decision.candidate_revision != null ? <p className="subtle">Candidate revision {decision.candidate_revision}</p> : null}
    {run.execution_retired ? <p className="subtle">Saved decision · read-only</p> : <><fieldset disabled={busy || stale || intent !== null}>
      <legend className="sr-only">Choose a response</legend>
      {decision.options.map(option => {
        const value = typeof option === 'string' ? option : option.value
        const label = typeof option === 'string' ? decision.kind === 'question' ? option : titleCase(option) : option.label
        const consequence = typeof option === 'string' ? null : option.consequence
        return <label className="choice" key={value}>
          <input type="radio" name={`decision-${decision.id}`} value={value} checked={choice === value} onChange={() => { setChoice(value); setFreeText(''); setCommandId(crypto.randomUUID()) }} />
          <span>{label}{consequence ? <small>{consequence}</small> : null}</span>
        </label>
      })}
    </fieldset>
    {decision.kind === 'question' || (decision.kind === 'plan' && choice === 'change') ? <label className="field-wide">{decision.kind === 'question' ? 'Your answer (or choose a suggestion)' : 'What should change in the plan?'}<textarea value={freeText} onChange={event => { setFreeText(event.target.value); if (decision.kind === 'question') setChoice(''); setCommandId(crypto.randomUUID()) }} rows={3} maxLength={4000} disabled={busy || stale || intent !== null} /></label> : null}
    {error ? <p className="form-error" role="alert">{error}</p> : null}
    <button className="primary-button" type="button" disabled={busy || stale || (!intent && (!valid || run.protocol_revision == null || decision.candidate_revision == null))} onClick={() => void answer()}>{busy ? 'Submitting…' : intent ? 'Check submitted decision' : decision.kind === 'plan' && choice === 'proceed' ? 'Accept this plan' : decision.kind === 'question' ? 'Submit answer' : decision.kind === 'plan' ? 'Submit plan response' : decision.kind === 'merge' ? 'Merge this feature' : 'Submit decision'}</button>
    </>}
  </section>
}

function IntakeHistory({ run }: { run: RunDetail }) {
  const intake = run.intake
  if (!intake) return null
  return <section className="section lower-section" aria-labelledby="intake-heading">
    <h2 id="intake-heading">Investigation and plan</h2>
    {intake.answers.length ? <div><h3>Clarifications</h3><dl>{intake.answers.map(answer => <div key={answer.question_id}><dt>{answer.prompt}</dt><dd>{answer.answer}</dd></div>)}</dl></div> : null}
    {intake.plans.map(plan => <div className="intake-plan" key={plan.revision}>
      <h3>Plan revision {plan.revision} · {titleCase(plan.state)}</h3>
      {intake.accepted_plan?.revision === plan.revision && intake.accepted_plan.authorization?.source === 'run_authorization' ? <p>Accepted automatically under this run’s authorization.</p> : null}
      <p>{plan.content.scope}</p>
      <ol>{plan.content.steps.map((item, index) => <li key={index}>{item}</li>)}</ol>
      <p><strong>Verification:</strong> {plan.content.verification.join('; ')}</p>
      <p><strong>Acceptance:</strong> {plan.content.acceptance.join('; ')}</p>
      <Workstreams plan={plan.content} />
      {plan.change_request ? <p><strong>Requested change:</strong> {plan.change_request}</p> : null}
    </div>)}
  </section>
}

function CancelRun({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const intentKey = `devflow.cancel:${run.id}`
  const [saved] = useState(() => {
    try {
      const raw = sessionStorage.getItem(intentKey)
      if (raw === null) return { request: null, blocked: false }
      const value: unknown = JSON.parse(raw)
      if (!value || typeof value !== 'object' || Array.isArray(value)) return { request: null, blocked: true }
      const request = value as Record<string, unknown>
      if (Object.keys(request).length !== 3 || typeof request.command_id !== 'string' || !request.command_id ||
          typeof request.expected_revision !== 'number' || !Number.isInteger(request.expected_revision) ||
          typeof request.reason !== 'string' || !request.reason.trim()) return { request: null, blocked: true }
      return { request: request as Parameters<typeof api.cancel>[1], blocked: false }
    } catch { return { request: null, blocked: true } }
  })
  const [reason, setReason] = useState(saved.request?.reason ?? '')
  const intent = useRef<Parameters<typeof api.cancel>[1] | null>(saved.request)
  const [open, setOpen] = useState(Boolean(saved.request))
  const [busy, setBusy] = useState(false)
  const [uncertain, setUncertain] = useState(Boolean(saved.request))
  const [settled, setSettled] = useState<{ acknowledged: boolean; revision: number } | null>(null)
  const [observed, setObserved] = useState<RunDetail | null>(null)
  const [error, setError] = useState(saved.blocked ? 'The retained cancellation request is unavailable. Resolve it before starting another cancellation.' : saved.request ? 'A previous cancellation request is retained. Retry to confirm the same request.' : '')
  const canRequest = !run.execution_retired && run.outcome == null && !run.checks?.terminal_tracker_checkpoint && !['blocked', 'terminal', 'cancelling'].includes(run.execution_state ?? '')
  const observedCurrent = observed?.id === run.id && observed.protocol_revision === run.protocol_revision && observed.outcome === run.outcome && observed.execution_state === run.execution_state && Boolean(observed.execution_retired) === Boolean(run.execution_retired) && Boolean(observed.checks?.terminal_tracker_checkpoint) === Boolean(run.checks?.terminal_tracker_checkpoint)
  const canStart = canRequest && (!settled || !settled.acknowledged && observedCurrent)
  const awaitingObservation = Boolean(settled) && !canStart
  if (!canRequest && !intent.current && !saved.blocked) return null

  async function refresh(observeResponse: boolean, acknowledged = false) {
    let terminalObserved = false
    if (observeResponse) {
      // The parent refresh may resolve without a successful detail read.
      // Bind a fresh command only to a post-response read displayed by the page.
      try {
        const latest = await api.getRun(run.id)
        setObserved(latest)
        terminalObserved = latest.id === run.id && (latest.execution_state === 'cancelling' || latest.outcome != null || Boolean(latest.execution_retired))
      }
      catch { setObserved(null); setError(current => current || 'The run could not be refreshed. Review its latest state before continuing.') }
    }
    try {
      await onRefresh()
      if (acknowledged && terminalObserved) {
        try { sessionStorage.removeItem(intentKey) } catch { /* A retained receipt is safe to replay. */ }
      }
    } catch { setError(current => current || 'The run could not be refreshed. Review its latest state before continuing.') }
  }

  async function cancel() {
    if (busy || saved.blocked || (!intent.current && (!canStart || !reason.trim() || run.protocol_revision == null))) return
    const request = intent.current ?? { command_id: crypto.randomUUID(), expected_revision: run.protocol_revision!, reason: reason.trim() }
    if (!intent.current) {
      try { sessionStorage.setItem(intentKey, JSON.stringify(request)) }
      catch { setError('The cancellation request could not be retained. It was not submitted.'); return }
      intent.current = request
    }
    let knownResponse = false
    let acknowledged = false
    setSettled(null); setObserved(null)
    setBusy(true); setError('')
    try {
      await api.cancel(run.id, request)
      knownResponse = true
      acknowledged = true
      intent.current = null
      setSettled({ acknowledged: true, revision: request.expected_revision })
      setUncertain(false); setOpen(false)
    } catch (cause) {
      const unknown = !(cause instanceof ApiError) || cause.status === 0 || cause.status >= 500
      setUncertain(unknown)
      if (!unknown) {
        knownResponse = true
        try { sessionStorage.removeItem(intentKey) } catch { /* A retained refusal is safe to recheck. */ }
        intent.current = null
        setSettled({ acknowledged: false, revision: request.expected_revision })
      }
      setError(unknown ? 'The cancellation outcome is unknown. Retry to confirm the same request.' : cause.status === 409 ? 'Run state changed. Review the latest state before cancelling.' : cause.message)
    } finally {
      await refresh(knownResponse, acknowledged)
      setBusy(false)
    }
  }
  return <div className="cancel-control">
    {settled?.acknowledged && awaitingObservation ? <p role="status">Cancellation requested. Waiting for the latest run state.</p> : open ? <div className="cancel-form"><label htmlFor="cancel-reason">Reason for cancellation</label><input id="cancel-reason" value={reason} disabled={busy || uncertain} onChange={event => setReason(event.target.value)} required />
      <div className="button-row"><button type="button" className="danger-button" onClick={() => void cancel()} disabled={busy || (!intent.current && (!canStart || !reason.trim() || run.protocol_revision == null))}>{busy ? 'Requesting…' : uncertain ? 'Retry cancellation request' : 'Request cancellation'}</button><button type="button" className="text-button" disabled={busy || uncertain} onClick={() => setOpen(false)}>Keep run</button></div>
    </div> : <button type="button" className="text-button" onClick={() => setOpen(true)} disabled={saved.blocked || !canStart || run.protocol_revision == null}>Cancel run</button>}
    {awaitingObservation ? <button type="button" className="text-button" disabled={busy} onClick={async () => { if (busy) return; setBusy(true); await refresh(true); setBusy(false) }}>Refresh run state</button> : null}
    {error ? <p className="form-error" role="alert">{error}</p> : null}
  </div>
}

function TrackerRecovery({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const intentKey = `devflow.reconcile-tracker:${run.id}`
  const [body, setBody] = useState<{ command_id: string; expected_revision: number } | null>(() => loadIntent(intentKey))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(body ? 'A previous tracker request is retained. Check its operation.' : '')
  const checkpoint = run.checks?.terminal_tracker_checkpoint
  if (!checkpoint || run.execution_retired || run.outcome != null) return null
  const available = run.phase === 'waiting_tracker' && (checkpoint.waiting || checkpoint.closed)

  async function reconcile() {
    if (busy || (!body && (run.protocol_revision == null || !available))) return
    const request = body ?? { command_id: crypto.randomUUID(), expected_revision: run.protocol_revision! }
    saveIntent(intentKey, request); setBody(request); setBusy(true); setError('')
    try {
      const operation = await api.createOperation({
        operation_id: `tracker:${request.command_id}`, run_id: run.id,
        kind: 'reconcile-tracker', request,
      })
      if (operation.state === 'succeeded') {
        saveIntent(intentKey, null); setBody(null)
        await onRefresh()
      } else if (operation.state === 'refused' || operation.state === 'failed') {
        saveIntent(intentKey, null); setBody(null)
        setError(operation.error || 'Tracker reconciliation was refused.')
        await onRefresh()
      } else {
        setError(`Operation ${operation.operation_id} is ${operation.state}. Check durable operations before retrying.`)
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Tracker readback could not be confirmed.')
      await onRefresh()
    } finally { setBusy(false) }
  }

  return <section className="section" aria-labelledby="tracker-recovery-heading">
    <h2 id="tracker-recovery-heading">Confirm terminal tracker</h2>
    <p>The terminal transition is fixed. Reconciliation confirms the issue, assignee and Project readback without rerunning development.</p>
    <p className="subtle">{available ? 'Readback is waiting for explicit reconciliation.' : 'The current bounded reconciliation is running or queued.'}</p>
    <button type="button" className="primary-button" disabled={busy || (!body && (run.protocol_revision == null || !available))} onClick={() => void reconcile()}>{busy ? 'Reconciling…' : error && body ? 'Retry same reconciliation' : 'Reconcile tracker'}</button>
    {error ? <p className="form-error" role="alert">{error} {body ? 'The request identity is retained while its response is unconfirmed.' : 'Review the refreshed run before reconciling again.'}</p> : null}
  </section>
}

export function RunDetails({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const issueUrl = safeWebUrl(run.issue_url)
  const decisions = (run.decisions ?? []).filter(decision => decision.state === 'pending' || decision.state === 'open')
  return <div className="run-details">
    <header className="run-heading">
      <h1>{display(run.title || run.goal, 'Untitled run')}</h1>
      <div className="run-heading__meta"><span>{display(run.repository || run.repository_key, 'Repository unknown')}</span><span className="meta-separator" aria-hidden="true" />{issueUrl ? <a href={issueUrl} target="_blank" rel="noopener noreferrer">{display(run.issue, 'Issue')} <ExternalIcon /></a> : <span>{display(run.issue, 'Issue unknown')}</span>}<span className="meta-separator" aria-hidden="true" /><span>ID: {display(run.work_id || run.id)}</span></div>
    </header>
    {run.execution_retired ? <p className="inline-alert">Historical run · read-only. Its execution backend is retired.</p> : null}
    {run.error ? <p className="inline-alert" role="alert">{run.error}</p> : null}
    <BlockingFindings run={run} />
    {!run.feature_delivery ? <PhaseStrip gates={run.phase_gates} /> : null}
    <MergeRun key={`merge-${run.id}`} run={run} onRefresh={onRefresh} />
    {decisions.map(decision => <DecisionCard key={`${decision.id}:${decision.revision}`} run={run} decision={decision} onRefresh={onRefresh} />)}
    <IntakeHistory run={run} />
    <FeatureDelivery key={`feature-${run.id}`} run={run} onRefresh={onRefresh} />
    {!run.feature_delivery ? <RunControls run={run} onRefresh={onRefresh} /> : null}
    <Facts run={run} />
    <RoleTable roles={run.roles} />
    <Activity events={run.events} />
    <p className="stream-note">Updates stream from the local service. Waiting uses no model calls. Last observation: {time(run.observed_at || run.updated_at)}.</p>
    {!run.feature_delivery ? <><EvidenceAndQuality run={run} /><GateDetails gates={run.phase_gates} /></> : null}
    <Operations run={run} />
    <DurableOperations runId={run.id} />
    <UsageSection run={run} />
    <TrackerRecovery key={run.id} run={run} onRefresh={onRefresh} />
    <CancelRun key={`cancel-${run.id}`} run={run} onRefresh={onRefresh} />
  </div>
}

function MergeRun({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const [busy, setBusy] = useState(false)
  const intentKey = `devflow.merge:${run.id}`
  const [body, setBody] = useState<Parameters<typeof api.merge>[1] | null>(() => loadIntent(intentKey))
  const [error, setError] = useState(body ? 'A previous merge request is retained. Check its operation.' : '')
  const merge = run.merge
  if (!merge || merge.phase === 'unavailable' || run.feature_delivery || run.execution_retired) return null
  const ready = merge.available && run.protocol_revision != null && !!merge.candidate_id && !!merge.head && !!merge.pr_number

  async function submit() {
    if (busy || (!ready && !body)) return
    const request = body ?? {
      command_id: crypto.randomUUID(), expected_revision: run.protocol_revision!,
      candidate_id: merge!.candidate_id!, head: merge!.head!,
      pr_number: merge!.pr_number!, answer: 'merge' as const,
    }
    saveIntent(intentKey, request); setBody(request); setBusy(true); setError('')
    try {
      const operation = await api.createOperation({
        operation_id: `merge:${request.command_id}`, run_id: run.id, kind: 'merge', request,
      })
      if (operation.state === 'succeeded') {
        saveIntent(intentKey, null); setBody(null)
        await onRefresh()
      } else if (operation.state === 'refused' || operation.state === 'failed') {
        saveIntent(intentKey, null); setBody(null)
        setError(operation.error || 'Merge admission was refused.')
        await onRefresh()
      } else {
        setError(`Operation ${operation.operation_id} is ${operation.state}. Check durable operations before retrying.`)
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Merge admission could not be confirmed.')
      await onRefresh()
    } finally { setBusy(false) }
  }

  return <section className="section" aria-labelledby="merge-heading">
    <h2 id="merge-heading">{merge.phase === 'merged' ? 'PR merged' : 'Merge delivered work'}</h2>
    {merge.available ? <>
      <p>Merge PR #{merge.pr_number} and close its issue. Devflow will reconcile changes from main, wait for the required checks on the combined result, and report the merge here.</p>
      <p className="subtle">Conflicting changes that need a product decision will be reported for your input.</p>
    </> : <p role="status">{merge.phase === 'merged' ? `Merge confirmed ${time(merge.result?.merged_at)}.` : `Merge status: ${titleCase(merge.phase)}.`}{merge.result?.reason ? ` ${merge.result.reason}` : ''}</p>}
    {merge.available || body ? <button type="button" className="primary-button" disabled={busy || (!ready && !body)} onClick={() => void submit()}>{busy ? 'Requesting merge…' : body && error ? 'Retry same merge request' : merge.resume ? 'Continue merge reconciliation' : `Merge PR #${merge.pr_number}`}</button> : null}
    {merge.result?.conflicts?.length ? <p>Resolving {merge.result.conflicts.length} conflicting files while preserving the feature and current main.</p> : null}
    {merge.result?.decision ? <div role="alert">
      <p>{merge.result.decision.question}</p>
      <p>Feature requirement: {merge.result.decision.feature_requirement}</p>
      <p>Main requirement: {merge.result.decision.main_requirement}</p>
      <p>{merge.result.decision.why_incompatible}</p>
    </div> : null}
    {merge.result?.merged_commit ? <p className="mono">Merged commit: {merge.result.merged_commit}</p> : null}
    {error || merge.error ? <p className="form-error" role="alert">{error || merge.error}{body ? ' The request identity is retained; retrying uses the same authorization.' : ''}</p> : null}
  </section>
}
