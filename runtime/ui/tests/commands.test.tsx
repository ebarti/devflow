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

  it('submits only allowlisted repository, endpoint and accepted plan fields', async () => {
    const user = userEvent.setup()
    const submit = vi.spyOn(api, 'newRun').mockResolvedValue({ run_id: 'fixture-new', dashboard_url: '/runs/fixture-new', existing: false, phase: 'queued' })
    const onCreated = vi.fn()
    render(<NewRun service={mockService} onCreated={onCreated} onBack={vi.fn()} />)
    await user.type(screen.getByLabelText('Work ID'), 'fixture-work')
    await user.type(screen.getByLabelText('GitHub issue URL'), 'https://github.com/example/repository/issues/1')
    await user.selectOptions(screen.getByLabelText('Repository'), 'fixture-repo')
    await user.type(screen.getByLabelText('Branch'), 'feat/fixture')
    await user.type(screen.getByLabelText('Goal'), 'Fixture task')
    await user.type(screen.getByLabelText('Accepted plan'), 'A fixture plan accepted for this UI test.')
    await user.click(screen.getByRole('button', { name: 'Start run' }))
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith('fixture-new'))
    const body = submit.mock.calls[0][0]
    expect(body).toMatchObject({ repository_key: 'fixture-repo', base_ref: 'main', authorized_endpoint: 'published_unmerged', accepted_plan: 'A fixture plan accepted for this UI test.' })
    expect(body).not.toHaveProperty('repo_path')
    expect(body).not.toHaveProperty('state_dir')
    expect(body).not.toHaveProperty('model')
  })
})
