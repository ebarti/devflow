import { afterEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { api, ApiError } from '../src/api'
import { NewRun } from '../src/NewRun'
import { RunDetails } from '../src/RunDetails'
import type { RunDetail } from '../src/model'
import { mockRun, mockService } from './fixtures'
import realBackendProjection from './real-backend-projection.json'
import waitingDecisionProjection from './waiting-decision-projection.json'

const completedOperation = (result: unknown = {}) => ({ operation_id: 'fixture-op', run_id: 'fixture-run', kind: 'fixture', stage: 'execution', state: 'succeeded' as const, progress: 'succeeded', next_action: 'inspect_result', result, error: null, authority: 'current_authority_unverified' })

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); sessionStorage.clear() })

describe('dashboard commands', () => {
  it('shows semantic adjudication separately while retaining failed QA and all raw findings', () => {
    const raw = ['Medium dependency provenance', 'Medium baseline layout', 'Medium baseline orphans']
    render(<RunDetails run={{ ...mockRun, decisions: [], checks: { qa: { state: 'failed' } },
      investigation_adjudication: { raw_status: 'findings', raw_findings: raw,
        disposition: { finding1: 'Receipt staging semantics reproduced', finding2: 'Accepted layout baseline', finding3: 'Accepted heading baseline', accepted_baseline_medium: 2, remaining_blocker_high: 0 },
        historical_runtime: 'old-runtime', controller_source: 'current-controller', additional_native_execution: false },
    }} onRefresh={vi.fn()} />)
    expect(screen.getByText(/Raw QA status: findings/).textContent).toContain('2 accepted baseline Medium')
    expect(screen.getByText('Failed')).toBeTruthy()
    for (const finding of raw) expect(screen.getByText(finding)).toBeTruthy()
    expect(screen.getByText(/No additional native execution/)).toBeTruthy()
  })

  it.each([false, true])('offers tracker reconciliation with frozen cancellation (closed=%s)', async closed => {
    const reconcile = vi.spyOn(api, 'createOperation')
      .mockRejectedValueOnce(new ApiError('receipt unavailable', 503))
      .mockResolvedValue({ operation_id: 'tracker:fixture', run_id: 'run-1', kind: 'reconcile-tracker', stage: 'execution', state: 'succeeded', progress: 'succeeded', next_action: 'inspect_result', result: {}, error: null, authority: 'current_authority_unverified' })
    const refresh = vi.fn().mockResolvedValue(undefined)
    const user = userEvent.setup()
    const run: RunDetail = { ...mockRun, decisions: [], outcome: null,
      phase: 'waiting_tracker', execution_state: 'waiting_tracker',
      tracker: { state: 'pending', pending: true },
      checks: { terminal_tracker_checkpoint: { state: 'pending', waiting: !closed, closed,
        deadline: '2026-10-03T16:10:00+00:00', cycles: 1, attempts: 3 } },
    }
    const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
    expect(screen.getByText(/terminal transition is fixed/i)).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Reconcile tracker' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(reconcile.mock.calls[0][0].request).toMatchObject({ expected_revision: 5 })
    expect((reconcile.mock.calls[0][0].request as { command_id: string }).command_id).toBeTruthy()
    // The accepted cycle arrives over SSE before the lost HTTP ACK is retried.
    rerender(<RunDetails run={{ ...run, protocol_revision: 9,
      checks: { terminal_tracker_checkpoint: { ...run.checks?.terminal_tracker_checkpoint,
        cycles: 2, deadline: '2026-10-03T16:20:00+00:00', waiting: false } },
    }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Retry same reconciliation' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
    expect(reconcile.mock.calls[1]).toEqual(reconcile.mock.calls[0])
  })

  it('retains tracker intent when the durable operation is still running', async () => {
    const reconcile = vi.spyOn(api, 'createOperation').mockResolvedValue({ operation_id: 'tracker:fixture', run_id: 'run-1', kind: 'reconcile-tracker', stage: 'execution', state: 'running', progress: 'running', next_action: 'wait', result: null, error: null, authority: null })
    vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
    const refresh = vi.fn().mockResolvedValue(undefined)
    const run: RunDetail = { ...mockRun, decisions: [], outcome: null, phase: 'waiting_tracker',
      checks: { terminal_tracker_checkpoint: { waiting: true } } }
    render(<RunDetails run={run} onRefresh={refresh} />)
    await userEvent.click(screen.getByRole('button', { name: 'Reconcile tracker' }))
    expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
    expect(refresh).not.toHaveBeenCalled()
    await userEvent.click(screen.getByRole('button', { name: 'Retry same reconciliation' }))
    expect(reconcile.mock.calls[1]).toEqual(reconcile.mock.calls[0])
  })

  it('disables a new tracker command while a bounded cycle is already running', () => {
    render(<RunDetails run={{ ...mockRun, decisions: [], outcome: null,
      phase: 'waiting_tracker', checks: { terminal_tracker_checkpoint: { waiting: false } },
    }} onRefresh={vi.fn()} />)
    expect((screen.getByRole('button', { name: 'Reconcile tracker' }) as HTMLButtonElement).disabled).toBe(true)
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
  })

  it.each(['question', 'plan'] as const)('keeps a retired nonterminal %s readable without action controls', kind => {
    const answer = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation())
    const cancel = vi.spyOn(api, 'cancel').mockResolvedValue(undefined)
    render(<RunDetails run={{
      ...mockRun, execution_retired: true, outcome: null, execution_state: 'waiting',
      decisions: [{ id: 'old-decision', revision: 1, kind, candidate_revision: 1,
        plan_revision: 1, prompt: 'Saved historical prompt', options: ['proceed', 'cancel'], state: 'pending' }],
      intake: { questions: [], answers: [], accepted_plan: null, plans: [
        { revision: 1, digest: 'old-plan', state: 'proposed', content: {
          scope: 'Saved historical plan', steps: ['Retained step'],
          verification: ['Retained verification'], acceptance: ['Retained acceptance'],
        } },
      ] },
    }} onRefresh={vi.fn()} />)
    expect(screen.getByText('Saved historical prompt')).toBeTruthy()
    expect(screen.getAllByText('Saved historical plan').length).toBeGreaterThan(0)
    expect(screen.getByText('Historical run · read-only. Its execution backend is retired.')).toBeTruthy()
    expect(screen.queryByRole('radio')).toBeNull()
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(screen.queryByRole('button', { name: /Submit|Accept|Cancel run|Request cancellation/ })).toBeNull()
    expect(answer).not.toHaveBeenCalled()
    expect(cancel).not.toHaveBeenCalled()
  })

  it('submits a string option from the managed waiting-decision projection', async () => {
    // Mirrors DeliveryWorkflow's projected options and DeliveryStore.detail's public run shape.
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => waitingDecisionProjection })))
    const run = await api.getRun('fixture-run')
    const answer = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation())
    const refresh = vi.fn().mockResolvedValue(undefined)
    const user = userEvent.setup()
    render(<RunDetails run={run} onRefresh={refresh} />)

    const proceed = screen.getByRole('radio', { name: 'Proceed' }) as HTMLInputElement
    expect(proceed.value).toBe('proceed')
    expect(screen.getByRole('radio', { name: 'Cancel' })).toBeTruthy()
    const submit = screen.getByRole('button', { name: 'Submit decision' }) as HTMLButtonElement
    expect(submit.disabled).toBe(true)
    await user.click(proceed)
    expect(submit.disabled).toBe(false)
    await user.click(submit)
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(answer.mock.calls[0][0].request).toMatchObject({
      expected_revision: 3, decision_id: 'fixture-run:initial', decision_revision: 1,
      candidate_revision: 1, answer: 'proceed',
    })
    expect(screen.getByText('Run queued').closest('div')?.querySelector('dd')?.textContent).toBe('No')
    expect(screen.getByText('Process/resource settlement').closest('div')?.querySelector('dd')?.textContent).toBe('None')
    expect(screen.getByText('Tracker observed').closest('div')?.querySelector('dd')?.textContent).toBe('{"state":"consistent","claim":true}')
    expect(screen.queryByText('[object Object]')).toBeNull()
    expect(screen.getByText('Total').closest('div')?.querySelector('dd')?.textContent).toBe('Unknown')
  })

  it('renders the observed backend gate order and hides cancellation for a blocked outcome', () => {
    // Sanitized projection captured from the real service/Temporal verifier run at PR #44 head 7eb6fb7.
    render(<RunDetails run={realBackendProjection as RunDetail} onRefresh={vi.fn()} />)
    const strip = screen.getByRole('region', { name: 'Workflow phase gates' })
    const gates = Array.from(strip.querySelectorAll('.phase'))
    expect(gates.map(gate => gate.textContent)).toEqual([
      'Prepare: Completed', 'Before PR checks: Pending', 'Publish: Pending',
      'Local checks: Pending', 'Required CI: Pending', 'Tracker: Pending',
    ])
    expect(strip.textContent).not.toContain('Implement')
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
  })

  it('shows future gate labels and does not repeat a pending cancellation', () => {
    render(<RunDetails run={{
      ...mockRun, outcome: null, execution_state: 'cancelling',
      phase_gates: [{ id: 'future_gate', label: 'Future gate', state: 'unobserved' }],
    }} onRefresh={vi.fn()} />)
    const strip = screen.getByRole('region', { name: 'Workflow phase gates' })
    expect(strip.textContent).toContain('Future gate: Unobserved')
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
  })

  it('does not present an unconfigured tracker readback as synchronized', () => {
    render(<RunDetails run={{ ...mockRun, tracker: { state: 'unconfigured', observed: 'issue open' } }} onRefresh={vi.fn()} />)
    expect(screen.getAllByText('Unconfigured').length).toBeGreaterThan(0)
    expect(screen.queryByText('Readback confirmed')).toBeNull()
  })

  it('does not resubmit a stale decision after a 409 and refreshes the run', async () => {
    const user = userEvent.setup()
    const answer = vi.spyOn(api, 'createOperation').mockResolvedValue({ ...completedOperation(), state: 'refused', result: null, error: 'The decision changed. The latest run is loading; review it before responding.', next_action: 'inspect_error', authority: null })
    const refresh = vi.fn().mockResolvedValue(undefined)
    render(<RunDetails run={mockRun} onRefresh={refresh} />)
    await user.click(screen.getByRole('radio', { name: 'Yes' }))
    await user.click(screen.getByRole('button', { name: 'Submit decision' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(screen.getByRole('alert').textContent).toMatch(/decision changed/i)
    expect((screen.getByRole('button', { name: 'Submit decision' }) as HTMLButtonElement).disabled).toBe(true)
    expect(answer).toHaveBeenCalledTimes(1)
    expect(answer.mock.calls[0][0].request).toMatchObject({ expected_revision: 5, decision_id: 'fixture-decision', decision_revision: 3, candidate_revision: 2, answer: 'yes' })
  })

  it('uses the workflow revision for cancellation and disables commands until it is observed', async () => {
    const user = userEvent.setup()
    const cancel = vi.spyOn(api, 'cancel').mockResolvedValue(undefined)
    render(<RunDetails run={{ ...mockRun, decisions: [] }} onRefresh={vi.fn().mockResolvedValue(undefined)} />)
    await user.click(screen.getByRole('button', { name: 'Cancel run' }))
    await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(cancel).toHaveBeenCalledTimes(1))
    expect(cancel.mock.calls[0][1]).toMatchObject({ expected_revision: 5, reason: 'Stop this run' })
  })

  it('allows cancellation after a known stale refusal without editing the reason', async () => {
    const user = userEvent.setup()
    const cancel = vi.spyOn(api, 'cancel')
      .mockRejectedValueOnce(new ApiError('stale run revision', 409))
      .mockResolvedValue(undefined)
    const refresh = vi.fn().mockResolvedValue(undefined)
    const run = { ...mockRun, decisions: [] }
    vi.spyOn(api, 'getRun').mockResolvedValue({ ...run, protocol_revision: 6 })
    const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Cancel run' }))
    await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(screen.getByRole('alert').textContent).toContain('Run state changed')

    rerender(<RunDetails run={{ ...run, protocol_revision: 6 }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
    expect(cancel.mock.calls[1][1]).toMatchObject({ expected_revision: 6, reason: 'Stop this run' })
    expect(cancel.mock.calls[1][1].command_id).not.toBe(cancel.mock.calls[0][1].command_id)
  })

  it.each([false, true])('holds a refused cancellation until a post-response detail read succeeds (refresh rejects=%s)', async rejects => {
    const user = userEvent.setup()
    let refuse: (error: Error) => void = () => { throw new Error('request has not started') }
    const cancel = vi.spyOn(api, 'cancel').mockImplementationOnce(() => new Promise((_, reject) => { refuse = reject })).mockResolvedValue(undefined)
    const run = { ...mockRun, decisions: [] }
    const read = vi.spyOn(api, 'getRun').mockRejectedValue(new ApiError('Read unavailable', 503))
    const refresh = rejects ? vi.fn().mockRejectedValue(new Error('Read unavailable')) : vi.fn().mockResolvedValue(undefined)
    const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Cancel run' }))
    await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    // A stream update during the pending command is not a post-response read.
    rerender(<RunDetails run={{ ...run, protocol_revision: 6 }} onRefresh={refresh} />)
    refuse(new ApiError('stale run revision', 409))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect((screen.getByRole('button', { name: 'Request cancellation' }) as HTMLButtonElement).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    expect(cancel).toHaveBeenCalledTimes(1)

    read.mockResolvedValue({ ...run, protocol_revision: 7 })
    await user.click(screen.getByRole('button', { name: 'Refresh run state' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
    // A delayed older page read also cannot satisfy the current observation.
    expect((screen.getByRole('button', { name: 'Request cancellation' }) as HTMLButtonElement).disabled).toBe(true)
    rerender(<RunDetails run={{ ...run, protocol_revision: 7 }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(cancel).toHaveBeenCalledTimes(2))
    expect(cancel.mock.calls[1][1]).toMatchObject({ expected_revision: 7, reason: 'Stop this run' })
    expect(cancel.mock.calls[1][1].command_id).not.toBe(cancel.mock.calls[0][1].command_id)
  })

  it('does not offer a second cancellation after acknowledgement while detail refresh fails', async () => {
    const user = userEvent.setup()
    const cancel = vi.spyOn(api, 'cancel').mockResolvedValue(undefined)
    vi.spyOn(api, 'getRun').mockRejectedValue(new ApiError('Read unavailable', 503))
    const refresh = vi.fn().mockResolvedValue(undefined)
    const run = { ...mockRun, decisions: [] }
    const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Cancel run' }))
    await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(screen.getByRole('status').textContent).toContain('Cancellation requested')
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
    rerender(<RunDetails run={{ ...run, protocol_revision: 6 }} onRefresh={refresh} />)
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
    expect(cancel).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['lost transport', new ApiError('Connection lost', 0)],
    ['server response', new ApiError('Receipt unavailable', 503)],
    ['unreadable response', new Error('Response could not be read')],
  ])('freezes cancellation across a %s and streamed revision change', async (_label, failure) => {
    const user = userEvent.setup()
    const cancel = vi.spyOn(api, 'cancel').mockRejectedValueOnce(failure).mockResolvedValue(undefined)
    const refresh = vi.fn().mockResolvedValue(undefined)
    const run = { ...mockRun, decisions: [] }
    const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Cancel run' }))
    await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(screen.getByRole('alert').textContent).toMatch(/outcome is unknown/i)
    expect((screen.getByLabelText('Reason for cancellation') as HTMLInputElement).disabled).toBe(true)
    expect((screen.getByRole('button', { name: 'Keep run' }) as HTMLButtonElement).disabled).toBe(true)

    rerender(<RunDetails run={{ ...run, protocol_revision: 9 }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Retry cancellation request' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
    expect(cancel.mock.calls[1]).toEqual(cancel.mock.calls[0])
    expect(screen.queryByLabelText('Reason for cancellation')).toBeNull()
  })

  it('keeps an uncertain cancellation available for exact replay after terminal readback', async () => {
    const user = userEvent.setup()
    const cancel = vi.spyOn(api, 'cancel')
      .mockRejectedValueOnce(new ApiError('Receipt unavailable', 503))
      .mockResolvedValue(undefined)
    const refresh = vi.fn().mockResolvedValue(undefined)
    const run = { ...mockRun, decisions: [] }
    const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Cancel run' }))
    await user.type(screen.getByLabelText('Reason for cancellation'), 'Stop this run')
    await user.click(screen.getByRole('button', { name: 'Request cancellation' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))

    rerender(<RunDetails run={{ ...run, protocol_revision: null, phase: 'cancelled', execution_state: 'terminal', outcome: 'cancelled' }} onRefresh={refresh} />)
    await user.click(screen.getByRole('button', { name: 'Retry cancellation request' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(2))
    expect(cancel.mock.calls[1]).toEqual(cancel.mock.calls[0])
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
  })

  it('does not enable decisions or cancellation without a workflow revision', () => {
    render(<RunDetails run={{ ...mockRun, protocol_revision: null }} onRefresh={vi.fn()} />)
    expect((screen.getByRole('button', { name: 'Submit decision' }) as HTMLButtonElement).disabled).toBe(true)
    expect((screen.getByRole('button', { name: 'Cancel run' }) as HTMLButtonElement).disabled).toBe(true)
  })

  it('submits a detailed goal and separate publication summary with the allowlisted endpoint', async () => {
    const user = userEvent.setup()
    const submit = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation({ run_id: 'fixture-new', dashboard_url: '/runs/fixture-new', existing: false, phase: 'queued' }))
    const onCreated = vi.fn()
    render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
    await user.type(screen.getByLabelText('Work ID'), 'fixture-work')
    await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
    await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
    await user.type(screen.getByLabelText('Branch'), 'feat/fixture')
    const goal = 'Investigate fixture behavior. Preserve evidence and publish only the design document.'
    await user.type(screen.getByLabelText('Goal'), goal)
    await user.type(screen.getByLabelText('Publication summary'), 'docs: investigate fixture behavior')
    await user.click(screen.getByRole('button', { name: 'Start investigation' }))
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith('fixture-new'))
    const body = submit.mock.calls[0][0].request
    expect(body).toMatchObject({ repository_key: 'fixture-repo', base_ref: 'main', authorized_endpoint: 'published_unmerged', goal, publication_summary: 'docs: investigate fixture behavior' })
    expect(body).not.toHaveProperty('accepted_plan')
    expect(body).not.toHaveProperty('repo_path')
    expect(body).not.toHaveProperty('state_dir')
    expect(body).not.toHaveProperty('model')
  })

  it('shows admission errors and keeps the goal and summary available for correction', async () => {
    const user = userEvent.setup()
    const message = 'publication_summary must use Conventional Commit syntax'
    const submit = vi.spyOn(api, 'createOperation').mockResolvedValue({ ...completedOperation(), state: 'refused', result: null, error: message, next_action: 'inspect_error', authority: null })
    const onCreated = vi.fn()
    render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
    await user.type(screen.getByLabelText('Work ID'), 'fixture-work')
    await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
    await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
    await user.type(screen.getByLabelText('Branch'), 'docs/fixture')
    const goal = 'Investigate fixture behavior. Keep all original execution instructions.'
    await user.type(screen.getByLabelText('Goal'), goal)
    await user.type(screen.getByLabelText('Publication summary'), 'Plain prose')
    await user.click(screen.getByRole('button', { name: 'Start investigation' }))
    expect((await screen.findByRole('alert')).textContent).toBe(message)
    expect(submit.mock.calls[0][0].request).toMatchObject({ goal, publication_summary: 'Plain prose' })
    expect((screen.getByLabelText('Goal') as HTMLTextAreaElement).value).toBe(goal)
    expect((screen.getByLabelText('Goal') as HTMLTextAreaElement).disabled).toBe(false)
    expect((screen.getByLabelText('Publication summary') as HTMLInputElement).value).toBe('Plain prose')
    expect(onCreated).not.toHaveBeenCalled()
  })

  it('requires and submits the accepted plan for a service without intake', async () => {
    const user = userEvent.setup()
    const submit = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation({ run_id: 'legacy-new', dashboard_url: '/runs/legacy-new', existing: false, phase: 'queued' }))
    const onCreated = vi.fn()
    render(<NewRun service={{ ...mockService, policy: { ...mockService.policy, intake_enabled: false } }} onCreated={onCreated} onBack={vi.fn()} />)
    await user.type(screen.getByLabelText('Work ID'), 'legacy-work')
    await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/2')
    await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
    await user.type(screen.getByLabelText('Branch'), 'feat/legacy')
    await user.type(screen.getByLabelText('Goal'), 'Legacy task')
    expect(screen.getByLabelText('Accepted plan').hasAttribute('required')).toBe(true)
    await user.type(screen.getByLabelText('Accepted plan'), 'Implement the previously accepted scope')
    await user.click(screen.getByRole('button', { name: 'Start run' }))
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith('legacy-new'))
    expect(submit.mock.calls[0][0].request).toMatchObject({
      goal: 'Legacy task', accepted_plan: 'Implement the previously accepted scope',
    })
    expect(submit.mock.calls[0][0].request).not.toHaveProperty('publication_summary')
  })

  it('sends a free-text clarification and renders the retained answer', async () => {
    const answer = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation())
    const user = userEvent.setup()
    render(<RunDetails run={{
      ...mockRun, protocol_revision: 4,
      intake: {
        questions: [{ id: '0:scope', revision: 1, prompt: 'Which scope?', options: ['Small'], state: 'pending' }],
        answers: [{ question_id: 'older', question_revision: 1, prompt: 'Why?', answer: 'Needed by users' }],
        plans: [], accepted_plan: null,
      },
      decisions: [{ id: 'fixture:question:0:scope', revision: 1, kind: 'question', candidate_revision: 1, prompt: 'Which scope?', options: ['Small'], state: 'pending', blocker: { unknown: 'Required consumer contract is missing', evidence_checked: ['README and tests checked'], why_no_safe_default: 'Guessing could break the required consumer' } }],
      question_notifications: [{ decision_id: 'fixture:question:0:scope', decision_revision: 1, state: 'queued' }],
    }} onRefresh={vi.fn().mockResolvedValue(undefined)} />)
    expect(screen.getByText('Needed by users')).toBeTruthy()
    expect(screen.getByText('Required consumer contract is missing')).toBeTruthy()
    expect(screen.getByText('README and tests checked')).toBeTruthy()
    expect(screen.getByText(/native queue acknowledged the message; display and an answer are not confirmed/)).toBeTruthy()
    expect(answer).not.toHaveBeenCalled()
    await user.type(screen.getByLabelText('Your answer (or choose a suggestion)'), 'Include both paths')
    await user.click(screen.getByRole('button', { name: 'Submit answer' }))
    await waitFor(() => expect(answer).toHaveBeenCalledTimes(1))
    expect(answer.mock.calls[0][0].request).toMatchObject({
      expected_revision: 4, decision_id: 'fixture:question:0:scope', answer: 'Include both paths',
    })
  })

  it('shows the exact proposed plan and sends a requested revision', async () => {
    const answer = vi.spyOn(api, 'createOperation').mockResolvedValue(completedOperation())
    const user = userEvent.setup()
    render(<RunDetails run={{
      ...mockRun, protocol_revision: 8,
      intake: {
        questions: [], answers: [],
        plans: [{ revision: 2, digest: 'fixture-digest', state: 'proposed', content: {
          scope: 'Update README', steps: ['Edit one file'],
          verification: ['Run fixture check'], acceptance: ['Text is visible'],
        } }], accepted_plan: null,
      },
      decisions: [{ id: 'fixture:plan:2', revision: 2, kind: 'plan', plan_revision: 2, plan_digest: 'fixture-digest', candidate_revision: 1, prompt: 'Review this Devflow plan before implementation.', options: ['proceed', 'change', 'cancel'], state: 'pending' }],
    }} onRefresh={vi.fn().mockResolvedValue(undefined)} />)
    expect(screen.getAllByText('Update README').length).toBeGreaterThan(0)
    await user.click(screen.getByRole('radio', { name: 'Change' }))
    await user.type(screen.getByLabelText('What should change in the plan?'), 'Include API verification')
    await user.click(screen.getByRole('button', { name: 'Submit plan response' }))
    await waitFor(() => expect(answer).toHaveBeenCalledTimes(1))
    expect(answer.mock.calls[0][0].request).toMatchObject({
      expected_revision: 8, decision_id: 'fixture:plan:2', answer: 'change', response: 'Include API verification',
    })
  })
})

it('keeps submitted run intent and never navigates while the operation is pending', async () => {
  const operation = vi.spyOn(api, 'createOperation').mockImplementation(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null }))
  const onCreated = vi.fn()
  const user = userEvent.setup()
  render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
  await user.type(screen.getByLabelText('Work ID'), 'fixture-work')
  await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
  await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
  await user.type(screen.getByLabelText('Branch'), 'feat/fixture')
  await user.type(screen.getByLabelText('Goal'), 'Investigate fixture')
  await user.click(screen.getByRole('button', { name: 'Start investigation' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  expect(onCreated).not.toHaveBeenCalled()
  expect((screen.getByLabelText('Goal') as HTMLTextAreaElement).disabled).toBe(true)
  await user.click(screen.getByRole('button', { name: 'Check submitted operation' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})

it('keeps the original decision while its durable operation is pending', async () => {
  const operation = vi.spyOn(api, 'createOperation').mockResolvedValue({ ...completedOperation(), state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null })
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const refresh = vi.fn().mockResolvedValue(undefined)
  const run: RunDetail = { ...mockRun, decisions: [{ id: 'pending-decision', revision: 1, kind: 'question', candidate_revision: 1, prompt: 'Choose a scope', options: ['Proceed'], state: 'pending' }] }
  const user = userEvent.setup()
  const { rerender } = render(<RunDetails run={run} onRefresh={refresh} />)
  await user.click(screen.getByRole('radio', { name: 'Proceed' }))
  await user.click(screen.getByRole('button', { name: 'Submit answer' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  expect(refresh).not.toHaveBeenCalled()
  rerender(<RunDetails run={{ ...run, protocol_revision: 99 }} onRefresh={refresh} />)
  await user.click(screen.getByRole('button', { name: 'Check submitted decision' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})


it('restores a submitted operation handle after the form is reopened', async () => {
  sessionStorage.setItem('devflow.pending-submit-operation', 'submit:retained')
  const inspect = vi.spyOn(api, 'getOperation')
    .mockResolvedValueOnce({ ...completedOperation(), operation_id: 'submit:retained', state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null })
    .mockResolvedValueOnce({ ...completedOperation({ run_id: 'restored-run' }), operation_id: 'submit:retained' })
  const onCreated = vi.fn()
  render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
  expect(await screen.findByText(/Saved submission submit:retained: running/)).toBeTruthy()
  expect((screen.getByRole('button', { name: 'Start investigation' }) as HTMLButtonElement).disabled).toBe(true)
  expect(onCreated).not.toHaveBeenCalled()
  await userEvent.click(screen.getByRole('button', { name: 'Refresh operation status' }))
  await userEvent.click(await screen.findByRole('button', { name: 'Open submitted run' }))
  expect(onCreated).toHaveBeenCalledWith('restored-run')
  expect(inspect).toHaveBeenCalledTimes(2)
})

it('restores the exact submitted request after reopening and retries the same operation', async () => {
  const operation = vi.spyOn(api, 'createOperation')
    .mockImplementationOnce(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null }))
    .mockResolvedValueOnce(completedOperation({ run_id: 'restored-exact' }))
  const onCreated = vi.fn()
  const user = userEvent.setup()
  const view = render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
  await user.type(screen.getByLabelText('Work ID'), 'retained-work')
  await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
  await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
  await user.type(screen.getByLabelText('Branch'), 'feat/retained')
  await user.type(screen.getByLabelText('Goal'), 'Retain this exact goal')
  await user.click(screen.getByRole('button', { name: 'Start investigation' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  view.unmount()
  render(<NewRun service={null} onCreated={onCreated} onBack={vi.fn()} />)
  expect((screen.getByLabelText('Goal') as HTMLTextAreaElement).value).toBe('Retain this exact goal')
  expect((screen.getByLabelText('Goal') as HTMLTextAreaElement).disabled).toBe(true)
  await user.click(screen.getByRole('button', { name: 'Check submitted operation' }))
  expect(JSON.stringify(operation.mock.calls[1])).toBe(JSON.stringify(operation.mock.calls[0]))
  expect(onCreated).toHaveBeenCalledWith('restored-exact')
})

it('restores the exact decision after reopening while its operation is pending', async () => {
  const operation = vi.spyOn(api, 'createOperation').mockImplementation(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null }))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run: RunDetail = { ...mockRun, decisions: [{ id: 'retained-decision', revision: 1, kind: 'question', candidate_revision: 1, prompt: 'Choose a scope', options: ['Proceed'], state: 'pending' }] }
  const user = userEvent.setup()
  const view = render(<RunDetails run={run} onRefresh={vi.fn()} />)
  await user.click(screen.getByRole('radio', { name: 'Proceed' }))
  await user.click(screen.getByRole('button', { name: 'Submit answer' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  view.unmount()
  render(<RunDetails run={{ ...run, protocol_revision: 99 }} onRefresh={vi.fn()} />)
  expect(screen.getByText(/previous decision request is retained/i)).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Check submitted decision' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})

it('keeps the original decision when operation submission reports a conflict', async () => {
  const operation = vi.spyOn(api, 'createOperation')
    .mockRejectedValueOnce(new ApiError('Operation ID conflicts', 409))
    .mockImplementationOnce(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'unknown', result: null, progress: 'unknown', next_action: 'reconcile', authority: null }))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run: RunDetail = { ...mockRun, decisions: [{ id: 'conflict-decision', revision: 1, kind: 'question', candidate_revision: 1, prompt: 'Choose a scope', options: ['Proceed'], state: 'pending' }] }
  const user = userEvent.setup()
  render(<RunDetails run={run} onRefresh={vi.fn()} />)
  await user.click(screen.getByRole('radio', { name: 'Proceed' }))
  await user.click(screen.getByRole('button', { name: 'Submit answer' }))
  expect(await screen.findByText('Operation ID conflicts')).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Check submitted decision' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})

it('restores the exact tracker request after reopening', async () => {
  const operation = vi.spyOn(api, 'createOperation').mockImplementation(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'running', result: null, progress: 'running', next_action: 'wait', authority: null }))
  vi.spyOn(api, 'runOperations').mockResolvedValue({ operations: [] })
  const run: RunDetail = { ...mockRun, decisions: [], outcome: null, phase: 'waiting_tracker', checks: { terminal_tracker_checkpoint: { waiting: true } } }
  const user = userEvent.setup()
  const view = render(<RunDetails run={run} onRefresh={vi.fn()} />)
  await user.click(screen.getByRole('button', { name: 'Reconcile tracker' }))
  expect(await screen.findByText(/operation .* is running/i)).toBeTruthy()
  view.unmount()
  render(<RunDetails run={{ ...run, protocol_revision: 99 }} onRefresh={vi.fn()} />)
  expect(screen.getByText(/previous tracker request is retained/i)).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Retry same reconciliation' }))
  expect(operation.mock.calls[1]).toEqual(operation.mock.calls[0])
})


it('reopens an unknown submitted request, reconciles its original receipt, and opens the run', async () => {
  const user = userEvent.setup()
  const operation = vi.spyOn(api, 'createOperation').mockImplementation(async envelope => ({ ...completedOperation(), operation_id: envelope.operation_id, state: 'unknown', result: null, progress: 'unknown', next_action: 'reconcile', authority: null }))
  const inspect = vi.spyOn(api, 'getOperation').mockImplementation(async id => ({ ...completedOperation(), operation_id: id, state: 'unknown', result: null, progress: 'unknown', next_action: 'reconcile', authority: null }))
  const reconcile = vi.spyOn(api, 'reconcileOperation').mockImplementation(async id => ({ ...completedOperation({ run_id: 'recovered-run' }), operation_id: id }))
  const onCreated = vi.fn()
  const view = render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
  await user.type(screen.getByLabelText('Work ID'), 'recover-work')
  await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
  await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
  await user.type(screen.getByLabelText('Branch'), 'feat/recover')
  await user.type(screen.getByLabelText('Goal'), 'Recover the original run')
  await user.click(screen.getByRole('button', { name: 'Start investigation' }))
  expect(await screen.findByText(/operation .* is unknown/i)).toBeTruthy()
  view.unmount()
  render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
  expect(await screen.findByRole('button', { name: 'Reconcile original operation' })).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Reconcile original operation' }))
  await user.click(await screen.findByRole('button', { name: 'Open submitted run' }))
  expect(onCreated).toHaveBeenCalledWith('recovered-run')
  expect(operation).toHaveBeenCalledTimes(1)
  expect(inspect).toHaveBeenCalled()
  expect(reconcile).toHaveBeenCalledTimes(1)
})
