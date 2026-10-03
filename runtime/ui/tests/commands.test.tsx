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

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals() })

describe('dashboard commands', () => {
  it.each(['question', 'plan'] as const)('keeps a retired nonterminal %s readable without action controls', kind => {
    const answer = vi.spyOn(api, 'answer').mockResolvedValue(undefined)
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
    const answer = vi.spyOn(api, 'answer').mockResolvedValue(undefined)
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
    expect(answer.mock.calls[0][1]).toMatchObject({
      expected_revision: 3, decision_id: 'fixture-run:initial', decision_revision: 1,
      candidate_revision: 1, answer: 'proceed',
    })
    expect(screen.getByText('Run queued').closest('div')?.querySelector('dd')?.textContent).toBe('No')
    expect(screen.getByText('Cleanup').closest('div')?.querySelector('dd')?.textContent).toBe('None')
    expect(screen.getByText('Tracker observed').closest('div')?.querySelector('dd')?.textContent).toBe('{"state":"consistent","claim":true}')
    expect(screen.queryByText('[object Object]')).toBeNull()
    expect(screen.getByText('Total').closest('div')?.querySelector('dd')?.textContent).toBe('Unknown')
  })

  it('renders the observed backend gate order and hides cancellation for a blocked outcome', () => {
    // Sanitized projection captured from the real service/Temporal verifier run at PR #44 head 7eb6fb7.
    render(<RunDetails run={realBackendProjection as RunDetail} onRefresh={vi.fn()} />)
    const strip = screen.getByRole('region', { name: 'Workflow phase gates' })
    const gates = Array.from(strip.querySelectorAll('.phase'))
    expect(gates.map(gate => gate.querySelector('.phase__label')?.textContent)).toEqual([
      'Prepare', 'Before PR checks', 'Publish', 'Local checks', 'Required CI', 'Tracker',
    ])
    expect(gates.map(gate => gate.className)).toEqual([
      'phase phase--good', 'phase phase--waiting', 'phase phase--waiting',
      'phase phase--waiting', 'phase phase--waiting', 'phase phase--waiting',
    ])
    expect(strip.textContent).not.toContain('Implement')
    expect(strip.querySelector('.phase-strip__progress')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
  })

  it('keeps future gate IDs neutral and does not repeat a pending cancellation', () => {
    render(<RunDetails run={{
      ...mockRun, outcome: null, execution_state: 'cancelling',
      phase_gates: [{ id: 'future_gate', label: 'Future gate', state: 'unobserved' }],
    }} onRefresh={vi.fn()} />)
    const strip = screen.getByRole('region', { name: 'Workflow phase gates' })
    expect(strip.querySelector('.phase')?.className).toBe('phase phase--unknown')
    expect(strip.textContent).toContain('Future gate')
    expect(screen.queryByRole('button', { name: 'Cancel run' })).toBeNull()
  })

  it('does not present an unconfigured tracker readback as synchronized', () => {
    render(<RunDetails run={{ ...mockRun, tracker: { state: 'unconfigured', observed: 'issue open' } }} onRefresh={vi.fn()} />)
    expect(screen.getAllByText('Unconfigured').length).toBeGreaterThan(0)
    expect(screen.queryByText('Readback confirmed')).toBeNull()
  })

  it('does not resubmit a stale decision after a 409 and refreshes the run', async () => {
    const user = userEvent.setup()
    const answer = vi.spyOn(api, 'answer').mockRejectedValue(new ApiError('revision conflict', 409))
    const refresh = vi.fn().mockResolvedValue(undefined)
    render(<RunDetails run={mockRun} onRefresh={refresh} />)
    await user.click(screen.getByRole('radio', { name: 'Yes' }))
    await user.click(screen.getByRole('button', { name: 'Submit decision' }))
    await waitFor(() => expect(refresh).toHaveBeenCalledTimes(1))
    expect(screen.getByRole('alert').textContent).toMatch(/decision changed/i)
    expect((screen.getByRole('button', { name: 'Submit decision' }) as HTMLButtonElement).disabled).toBe(true)
    expect(answer).toHaveBeenCalledTimes(1)
    expect(answer.mock.calls[0][1]).toMatchObject({ expected_revision: 5, decision_id: 'fixture-decision', decision_revision: 3, candidate_revision: 2, answer: 'yes' })
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

  it('does not enable decisions or cancellation without a workflow revision', () => {
    render(<RunDetails run={{ ...mockRun, protocol_revision: null }} onRefresh={vi.fn()} />)
    expect((screen.getByRole('button', { name: 'Submit decision' }) as HTMLButtonElement).disabled).toBe(true)
    expect((screen.getByRole('button', { name: 'Cancel run' }) as HTMLButtonElement).disabled).toBe(true)
  })

  it('submits a raw goal with the allowlisted repository and endpoint', async () => {
    const user = userEvent.setup()
    const submit = vi.spyOn(api, 'newRun').mockResolvedValue({ run_id: 'fixture-new', dashboard_url: '/runs/fixture-new', existing: false, phase: 'queued' })
    const onCreated = vi.fn()
    render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
    await user.type(screen.getByLabelText('Work ID'), 'fixture-work')
    await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
    await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
    await user.type(screen.getByLabelText('Branch'), 'feat/fixture')
    await user.type(screen.getByLabelText('Goal'), 'Fixture task')
    await user.click(screen.getByRole('button', { name: 'Start investigation' }))
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith('fixture-new'))
    const body = submit.mock.calls[0][0]
    expect(body).toMatchObject({ repository_key: 'fixture-repo', base_ref: 'main', authorized_endpoint: 'published_unmerged', goal: 'Fixture task' })
    expect(body).not.toHaveProperty('accepted_plan')
    expect(body).not.toHaveProperty('repo_path')
    expect(body).not.toHaveProperty('state_dir')
    expect(body).not.toHaveProperty('model')
  })

  it('requires and submits the accepted plan for a service without intake', async () => {
    const user = userEvent.setup()
    const submit = vi.spyOn(api, 'newRun').mockResolvedValue({ run_id: 'legacy-new', dashboard_url: '/runs/legacy-new', existing: false, phase: 'queued' })
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
    expect(submit.mock.calls[0][0]).toMatchObject({
      goal: 'Legacy task', accepted_plan: 'Implement the previously accepted scope',
    })
  })

  it('sends a free-text clarification and renders the retained answer', async () => {
    const answer = vi.spyOn(api, 'answer').mockResolvedValue(undefined)
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
    expect(answer.mock.calls[0][1]).toMatchObject({
      expected_revision: 4, decision_id: 'fixture:question:0:scope', answer: 'Include both paths',
    })
  })

  it('shows the exact proposed plan and sends a requested revision', async () => {
    const answer = vi.spyOn(api, 'answer').mockResolvedValue(undefined)
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
    expect(answer.mock.calls[0][1]).toMatchObject({
      expected_revision: 8, decision_id: 'fixture:plan:2', answer: 'change', response: 'Include API verification',
    })
  })
})
