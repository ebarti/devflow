import { afterEach, expect, it, vi } from 'vitest'
import { api, ApiError, type DurableOperation } from '../src/api'

afterEach(() => vi.restoreAllMocks())
const body = { command_id: 'cancel-original', expected_revision: 7, reason: 'Stop this run' }
const operation = (state: DurableOperation['state']): DurableOperation & { request: typeof body } => ({
  operation_id: 'cancel:cancel-original', run_id: 'run-1', kind: 'cancel', stage: 'execution',
  state, progress: state, next_action: state === 'unknown' ? 'reconcile' : 'wait',
  result: state === 'succeeded' ? { run_id: 'run-1', phase: 'cancelling', revision: 8 } : null,
  error: state === 'refused' ? 'stale run revision' : null, authority: null, request: body,
})

it('keeps pending, running, unknown and generic failure outcomes uncertain', async () => {
  const create = vi.spyOn(api, 'createOperation')
  for (const state of ['pending', 'running', 'unknown', 'failed'] as const) {
    create.mockResolvedValueOnce(operation(state))
    await expect(api.cancel('run-1', body)).rejects.toMatchObject({ status: 0 })
  }
  expect(create).toHaveBeenCalledTimes(4)
  for (const [request] of create.mock.calls) {
    expect(request).toMatchObject({ run_id: 'run-1', kind: 'cancel', request: body })
    expect(request.operation_id).toMatch(/^cancel:[a-f0-9]{64}$/)
  }
})

it('preserves the exact request after lost responses and operation-level conflicts', async () => {
  const create = vi.spyOn(api, 'createOperation')
    .mockRejectedValueOnce(new ApiError('connection lost', 0))
    .mockRejectedValueOnce(new ApiError('operation ID conflicts', 409))
    .mockResolvedValueOnce(operation('succeeded'))
  await expect(api.cancel('run-1', body)).rejects.toMatchObject({ status: 0 })
  await expect(api.cancel('run-1', body)).rejects.toMatchObject({ status: 0 })
  await expect(api.cancel('run-1', body)).resolves.toBeUndefined()
  expect(create.mock.calls[1]).toEqual(create.mock.calls[0])
  expect(create.mock.calls[2]).toEqual(create.mock.calls[0])
})

it('treats only a durable refusal as a known rejection', async () => {
  const create = vi.spyOn(api, 'createOperation')
    .mockResolvedValueOnce(operation('refused'))
    .mockResolvedValueOnce({ ...operation('succeeded'), request: { ...body, command_id: 'cancel-new', expected_revision: 8 } } as DurableOperation)
  await expect(api.cancel('run-1', body)).rejects.toMatchObject({ status: 409, message: 'stale run revision' })
  await expect(api.cancel('run-1', { ...body, command_id: 'cancel-new', expected_revision: 8 })).resolves.toBeUndefined()
  expect(create.mock.calls[1][0]).toMatchObject({ request: { expected_revision: 8 } })
  expect(create.mock.calls[1][0].operation_id).not.toBe(create.mock.calls[0][0].operation_id)
})


it('never accepts a mismatched or incomplete durable receipt', async () => {
  vi.spyOn(api, 'createOperation')
    .mockResolvedValueOnce({ ...operation('succeeded'), run_id: 'another-run' })
    .mockResolvedValueOnce({ ...operation('refused'), request: { ...body, reason: 'changed' } } as DurableOperation)
    .mockResolvedValueOnce({ ...operation('succeeded'), result: { run_id: 'run-1' } })
  for (let i = 0; i < 3; i++) await expect(api.cancel('run-1', body)).rejects.toMatchObject({ status: 0 })
})
