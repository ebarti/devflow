import { afterEach, expect, it, vi } from 'vitest'
import { act, cleanup, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { App } from '../src/App'
import { api } from '../src/api'
import { mockRun, mockService } from './fixtures'

vi.mock('../src/stream', () => ({ subscribeRun: vi.fn(() => () => {}) }))
afterEach(() => { cleanup(); vi.restoreAllMocks(); window.history.replaceState(null, '', '/') })

it('loads older rows explicitly and refreshes only the recent page', async () => {
  let poll: (() => void) | undefined
  const interval = window.setInterval.bind(window)
  vi.spyOn(window, 'setInterval').mockImplementation(((fn: () => void, delay: number) => {
    if (delay === 5000) { poll = fn; return 1 }
    return interval(fn, delay)
  }) as typeof window.setInterval)
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const older = { ...mockRun, id: 'old-run', title: 'Historical row' }
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [mockRun], next_cursor: 'older' })
    .mockResolvedValueOnce({ runs: [older], next_cursor: null })
    .mockResolvedValue({ runs: [{ ...mockRun, title: 'Current updated row' }], next_cursor: 'older' })
  render(<App />)
  await screen.findByText(mockRun.title!)
  expect(list).toHaveBeenCalledTimes(1)
  await userEvent.setup().click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Historical row')
  expect(list).toHaveBeenLastCalledWith(false, 'older')
  await act(async () => { poll!() })
  await screen.findByText('Current updated row')
  expect(screen.getByText('Historical row')).toBeTruthy()
  expect(list.mock.calls).toEqual([[false], [false, 'older'], [false]])
})

it('discards a late older page after archive toggle', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  let finish: ((page: { runs: typeof mockRun[]; next_cursor: null }) => void) | undefined
  const list = vi.spyOn(api, 'listRunsPage').mockImplementation(async (archived, cursor) => {
    if (archived) return { runs: [], next_cursor: null }
    if (cursor) return new Promise(resolve => { finish = resolve })
    return { runs: [mockRun], next_cursor: 'older' }
  })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText(mockRun.title!)
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await user.click(screen.getByRole('button', { name: 'Show archived tasks' }))
  await screen.findByRole('heading', { name: 'Archived tasks' })
  await act(async () => { finish!({ runs: [{ ...mockRun, id: 'old', title: 'Wrong collection' }], next_cursor: null }) })
  expect(screen.queryByText('Wrong collection')).toBeNull()
  expect(list).toHaveBeenLastCalledWith(true)
})

it('retries a failed older page with the same cursor without losing loaded rows', async () => {
  vi.spyOn(api, 'getService').mockResolvedValue(mockService)
  const list = vi.spyOn(api, 'listRunsPage').mockResolvedValueOnce({ runs: [mockRun], next_cursor: 'older' })
    .mockRejectedValueOnce(new Error('Fixture page unavailable'))
    .mockResolvedValueOnce({ runs: [{ ...mockRun, id: 'old', title: 'Recovered older row' }], next_cursor: null })
  render(<App />)
  const user = userEvent.setup()
  await screen.findByText(mockRun.title!)
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Fixture page unavailable')
  expect(screen.getByText(mockRun.title!)).toBeTruthy()
  await user.click(screen.getByRole('button', { name: 'Load older tasks' }))
  await screen.findByText('Recovered older row')
  expect(list.mock.calls).toEqual([[false], [false, 'older'], [false, 'older']])
})
