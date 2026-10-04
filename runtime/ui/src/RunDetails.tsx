import { useState } from 'react'
import { api, ApiError, safeWebUrl } from './api'
import { CheckIcon, ExternalIcon } from './icons'
import { display, time, timelineTime, titleCase, tokens, tone } from './format'
import type { ActivityEvent, CheckState, Decision, PhaseGate, RoleState, RunDetail } from './model'

function State({ value }: { value: string | null | undefined }) {
  return <span className={`state state--${tone(value)}`}><span className="state__mark" aria-hidden="true">{tone(value) === 'good' ? <CheckIcon /> : null}</span>{titleCase(value)}</span>
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
    <div><dt>Candidate</dt><dd className="mono" title={run.candidate?.head ?? undefined}>{display(run.candidate?.head, 'Not available')}</dd></div>
    <div><dt>Pull request</dt><dd>{url ? <a href={url} target="_blank" rel="noopener noreferrer">{run.pull_request?.number ? `#${run.pull_request.number}` : 'Open pull request'} <ExternalIcon /></a> : display(run.pull_request?.number, 'Not published')}</dd></div>
    <div><dt>Authorized endpoint</dt><dd><span className="outline-pill">{titleCase(run.authorized_endpoint)}</span></dd></div>
    <div><dt>Tracker</dt><dd><span className={`soft-pill soft-pill--${trackerTone}`}>{trackerLabel}</span></dd></div>
  </dl>
}

function RoleTable({ roles }: { roles: RoleState[] | null | undefined }) {
  return <section className="section role-section" aria-labelledby="roles-heading">
    <h2 id="roles-heading">Roles</h2>
    <div className="table-scroll"><table>
      <thead><tr><th>Role</th><th>State</th><th>Session</th><th>Tokens</th></tr></thead>
      <tbody>{roles?.length ? roles.map((role, index) => <tr key={`${role.role}-${role.iteration ?? role.attempt_id ?? index}`}>
        <td>{titleCase(role.role)}</td>
        <td><State value={role.state} /></td>
        <td className="mono">{display(role.session_id, 'Unknown')}</td>
        <td>{tokens(role.usage?.total_tokens)}</td>
      </tr>) : <tr><td className="empty-row" colSpan={4}>No role attempts have been recorded.</td></tr>}</tbody>
    </table></div>
  </section>
}

function Activity({ events }: { events: ActivityEvent[] | null | undefined }) {
  const ordered = [...(events ?? [])].sort((a, b) => b.sequence - a.sequence)
  return <section className="section activity-section" aria-labelledby="activity-heading">
    <h2 id="activity-heading">Activity</h2>
    {ordered.length ? <ol className="timeline">{ordered.map(event => <li key={event.sequence}>
      <span className="timeline__dot" aria-hidden="true" />
      <time dateTime={event.timestamp ?? undefined}>{timelineTime(event.timestamp)}</time>
      <span>{display(event.message, titleCase(event.type))}</span>
      <span className="timeline__sequence">#{event.sequence}</span>
    </li>)}</ol> : <p className="empty-section">No activity has been recorded yet.</p>}
  </section>
}

function QualityRow({ label, value }: { label: string; value: CheckState | null | undefined }) {
  return <div className="quality-row"><span>{label}</span><State value={value?.state} /><span className="quality-row__detail">{display(value?.detail, 'Evidence not available')}</span></div>
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
      <div><dt>Cleanup</dt><dd>{titleCase(run.cleanup)}</dd></div>
      <div><dt>Tracker desired</dt><dd>{display(run.tracker?.desired)}</dd></div>
      <div><dt>Tracker observed</dt><dd>{display(run.tracker?.observed)}</dd></div>
      <div><dt>Tracker sync</dt><dd>{titleCase(run.tracker?.state)}</dd></div>
      <div><dt>Tracker readback</dt><dd>{time(run.tracker?.readback_at)}</dd></div>
      <div><dt>Candidate revision</dt><dd>{display(run.candidate?.revision)}</dd></div>
      <div><dt>Base SHA</dt><dd className="mono">{display(run.candidate?.base)}</dd></div>
      <div><dt>Content digest</dt><dd className="mono">{display(run.candidate?.content_digest)}</dd></div>
      <div><dt>Policy digest</dt><dd className="mono">{display(run.candidate?.policy_digest)}</dd></div>
      <div><dt>Environment digest</dt><dd className="mono">{display(run.candidate?.environment_digest)}</dd></div>
      <div><dt>Protocol revision</dt><dd>{display(run.protocol_revision)}</dd></div>
    </dl>
    {run.roles?.length ? <div className="role-provenance"><h3>Role provenance</h3><ul>{run.roles.map((role, index) => <li key={`${role.role}-${role.iteration ?? role.attempt_id ?? index}`}><strong>{titleCase(role.role)}</strong><span>Model {display(role.model, 'unobserved')} · Effort {display(role.effort, 'unobserved')} · Iteration {display(role.iteration, 'unknown')} · Cleanup {display(role.cleanup)} · Last activity {time(role.last_activity_at)}{role.summary ? ` · ${role.summary}` : ''}{role.findings?.length ? ` · Findings: ${role.findings.join('; ')}` : ''}</span></li>)}</ul></div> : null}
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
  const [choice, setChoice] = useState('')
  const [freeText, setFreeText] = useState('')
  const [commandId, setCommandId] = useState(() => crypto.randomUUID())
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [stale, setStale] = useState(false)

  const plan = decision.kind === 'plan' ? run.intake?.plans.find(item => item.revision === decision.plan_revision)?.content : null
  const notification = run.question_notifications?.find(item => item.decision_id === decision.id && item.decision_revision === decision.revision)
  const answerText = decision.kind === 'question' ? freeText.trim() || choice : choice
  const valid = Boolean(answerText) && (decision.kind !== 'plan' || choice !== 'change' || Boolean(freeText.trim()))

  async function answer() {
    if (run.execution_retired || !valid || run.protocol_revision == null || decision.candidate_revision == null || busy || stale) return
    setBusy(true); setError('')
    try {
      await api.answer(run.id, {
        command_id: commandId, expected_revision: run.protocol_revision,
        decision_id: decision.id, decision_revision: decision.revision,
        candidate_revision: decision.candidate_revision, answer: answerText,
        ...(decision.kind === 'plan' && choice === 'change' ? { response: freeText.trim() } : {}),
      })
      await onRefresh()
    } catch (cause) {
      if (cause instanceof ApiError && cause.status === 409) {
        setStale(true)
        setError('The decision changed. The latest run is loading; review it before responding.')
        await onRefresh()
      } else setError(cause instanceof Error ? cause.message : 'Could not submit the decision.')
    } finally { setBusy(false) }
  }

  return <section className="decision-card" aria-labelledby={`decision-${decision.id}`}>
    <div className="decision-card__heading"><h2 id={`decision-${decision.id}`}>{decision.kind === 'question' ? 'Clarification needed' : decision.kind === 'plan' ? 'Review Devflow plan' : 'Decision needed'}</h2><span>Revision {decision.revision}</span></div>
    <p>{decision.prompt}</p>
    {decision.blocker ? <div className="intake-plan"><h3>Why input is needed</h3><p>{decision.blocker.unknown}</p><p>{decision.blocker.why_no_safe_default}</p><h3>Evidence checked</h3><ul>{decision.blocker.evidence_checked.map((item, index) => <li key={index}>{item}</li>)}</ul></div> : null}
    {notification ? <p className="subtle">Question callback: {titleCase(notification.state)}. {notification.state === 'queued' ? 'The native queue acknowledged the message; display and an answer are not confirmed.' : notification.state === 'unavailable' ? 'No originating thread is bound. Answer here.' : notification.state === 'unknown' ? 'The queue effect is uncertain and will not be retried automatically.' : notification.receipt?.reason ?? ''}</p> : null}
    {plan ? <div className="intake-plan"><h3>Scope</h3><p>{plan.scope}</p><h3>Steps</h3><ol>{plan.steps.map((item, index) => <li key={index}>{item}</li>)}</ol><h3>Verification</h3><ul>{plan.verification.map((item, index) => <li key={index}>{item}</li>)}</ul><h3>Acceptance</h3><ul>{plan.acceptance.map((item, index) => <li key={index}>{item}</li>)}</ul></div> : null}
    {decision.candidate_revision != null ? <p className="subtle">Candidate revision {decision.candidate_revision}</p> : null}
    {run.execution_retired ? <p className="subtle">Saved decision · read-only</p> : <><fieldset disabled={busy || stale}>
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
    {decision.kind === 'question' || (decision.kind === 'plan' && choice === 'change') ? <label className="field-wide">{decision.kind === 'question' ? 'Your answer (or choose a suggestion)' : 'What should change in the plan?'}<textarea value={freeText} onChange={event => { setFreeText(event.target.value); if (decision.kind === 'question') setChoice(''); setCommandId(crypto.randomUUID()) }} rows={3} maxLength={4000} disabled={busy || stale} /></label> : null}
    {error ? <p className="form-error" role="alert">{error}</p> : null}
    <button className="primary-button" type="button" disabled={!valid || busy || stale || run.protocol_revision == null || decision.candidate_revision == null} onClick={() => void answer()}>{busy ? 'Submitting…' : decision.kind === 'plan' && choice === 'proceed' ? 'Accept this plan' : decision.kind === 'question' ? 'Submit answer' : decision.kind === 'plan' ? 'Submit plan response' : 'Submit decision'}</button>
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
      {plan.change_request ? <p><strong>Requested change:</strong> {plan.change_request}</p> : null}
    </div>)}
  </section>
}

function CancelRun({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const [reason, setReason] = useState('')
  const [commandId, setCommandId] = useState(() => crypto.randomUUID())
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  if (run.execution_retired || run.outcome != null || run.checks?.terminal_tracker_checkpoint || ['blocked', 'terminal', 'cancelling'].includes(run.execution_state ?? '')) return null
  async function cancel() {
    if (!reason.trim() || run.protocol_revision == null) return
    setBusy(true); setError('')
    try {
      await api.cancel(run.id, { command_id: commandId, expected_revision: run.protocol_revision, reason: reason.trim() })
      setOpen(false)
      await onRefresh()
    } catch (cause) {
      setError(cause instanceof ApiError && cause.status === 409 ? 'Run state changed. Review the latest state before cancelling.' : cause instanceof Error ? cause.message : 'Could not request cancellation.')
      if (cause instanceof ApiError && cause.status === 409) await onRefresh()
    } finally { setBusy(false) }
  }
  return <div className="cancel-control">
    {open ? <div className="cancel-form"><label htmlFor="cancel-reason">Reason for cancellation</label><input id="cancel-reason" value={reason} onChange={event => { setReason(event.target.value); setCommandId(crypto.randomUUID()) }} required />
      <div className="button-row"><button type="button" className="danger-button" onClick={() => void cancel()} disabled={!reason.trim() || busy || run.protocol_revision == null}>{busy ? 'Requesting…' : 'Request cancellation'}</button><button type="button" className="text-button" onClick={() => setOpen(false)}>Keep run</button></div>
    </div> : <button type="button" className="text-button" onClick={() => setOpen(true)} disabled={run.protocol_revision == null}>Cancel run</button>}
    {error ? <p className="form-error" role="alert">{error}</p> : null}
  </div>
}

function TrackerRecovery({ run, onRefresh }: { run: RunDetail; onRefresh: () => Promise<void> }) {
  const [body, setBody] = useState<{ command_id: string; expected_revision: number } | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const checkpoint = run.checks?.terminal_tracker_checkpoint
  if (!checkpoint || run.execution_retired || run.outcome != null) return null
  const available = run.phase === 'waiting_tracker' && (checkpoint.waiting || checkpoint.closed)

  async function reconcile() {
    if (busy || run.protocol_revision == null || (!body && !available)) return
    const request = body ?? { command_id: crypto.randomUUID(), expected_revision: run.protocol_revision }
    setBody(request); setBusy(true); setError('')
    try {
      await api.reconcileTracker(run.id, request)
      await onRefresh()
      setBody(null)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Tracker readback could not be confirmed.')
      await onRefresh()
      if (cause instanceof ApiError && cause.status === 409 && cause.message.includes('stale run revision')) setBody(null)
    } finally { setBusy(false) }
  }

  return <section className="section" aria-labelledby="tracker-recovery-heading">
    <h2 id="tracker-recovery-heading">Confirm terminal tracker</h2>
    <p>The terminal transition is fixed. Reconciliation confirms the issue, assignee and Project readback without rerunning development.</p>
    <p className="subtle">{available ? 'Readback is waiting for explicit reconciliation.' : 'The current bounded reconciliation is running or queued.'}</p>
    <button type="button" className="primary-button" disabled={busy || run.protocol_revision == null || (!body && !available)} onClick={() => void reconcile()}>{busy ? 'Reconciling…' : error && body ? 'Retry same reconciliation' : 'Reconcile tracker'}</button>
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
    <PhaseStrip gates={run.phase_gates} />
    {decisions.map(decision => <DecisionCard key={`${decision.id}:${decision.revision}`} run={run} decision={decision} onRefresh={onRefresh} />)}
    <IntakeHistory run={run} />
    <Facts run={run} />
    <RoleTable roles={run.roles} />
    <Activity events={run.events} />
    <p className="stream-note">Updates stream from the local service. Waiting uses no model calls. Last observation: {time(run.observed_at || run.updated_at)}.</p>
    <EvidenceAndQuality run={run} />
    <GateDetails gates={run.phase_gates} />
    <Operations run={run} />
    <UsageSection run={run} />
    <TrackerRecovery key={run.id} run={run} onRefresh={onRefresh} />
    <CancelRun run={run} onRefresh={onRefresh} />
  </div>
}
