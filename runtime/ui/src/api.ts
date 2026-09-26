import type { NewRunRequest, RunDetail, RunSummary, ServiceInfo } from './model'

/** The only HTTP path/contract adapter. No credentials are persisted or put in URLs. */
export class ApiError extends Error {
  constructor(message: string, readonly status: number) {
    super(message)
    this.name = 'ApiError'
  }
}

let csrfToken: string | undefined

function observedCount(value: unknown): number | null {
  return typeof value === 'number' ? value : Array.isArray(value) ? value.length : null
}

function observedState(value: unknown): string | null {
  if (typeof value === 'string') return value
  if (value && typeof value === 'object' && 'state' in value && typeof value.state === 'string') return value.state
  return null
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response
  try {
    response = await fetch(path, { credentials: 'same-origin', cache: 'no-store', ...init })
  } catch {
    throw new ApiError('The local service could not be reached.', 0)
  }
  if (!response.ok) {
    let message = `The service returned ${response.status}.`
    try {
      const error = await response.json() as { detail?: string; message?: string }
      message = error.detail || error.message || message
    } catch { /* A non-JSON error still has its status. */ }
    throw new ApiError(message, response.status)
  }
  return await response.json() as T
}

async function session(): Promise<boolean> {
  const result = await request<{ authenticated: boolean; csrf_token?: string }>('/api/session')
  csrfToken = result.authenticated && result.csrf_token ? result.csrf_token : undefined
  return Boolean(csrfToken)
}

async function login(token: string): Promise<void> {
  const result = await request<{ csrf_token?: string }>('/api/session', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }),
  })
  if (!result.csrf_token) throw new ApiError('The service did not establish a session.', 0)
  csrfToken = result.csrf_token
}

async function command<T>(path: string, body: object): Promise<T> {
  if (!csrfToken && !await session()) throw new ApiError('Sign in to the local service first.', 401)
  try {
    return await request<T>(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Devflow-CSRF': csrfToken! },
      body: JSON.stringify(body),
    })
  } catch (error) {
    if (error instanceof ApiError && error.status === 403) csrfToken = undefined
    throw error
  }
}

export const api = {
  session,
  login,
  listRuns: async (): Promise<RunSummary[]> => (await request<{ runs: RunSummary[] }>('/api/runs')).runs,
  getRun: async (id: string): Promise<RunDetail> => {
    const result = await request<{ run: RunDetail; events?: RunDetail['events']; evidence?: RunDetail['evidence'] }>(`/api/runs/${encodeURIComponent(id)}`)
    const run = result.run
    const observed = run.tracker?.observed
    return {
      ...run,
      capacity: {
        ...run.capacity,
        queued: observedCount(run.capacity?.queued ?? run.queued),
        cleanup: observedState(run.capacity?.cleanup ?? run.cleanup),
      },
      candidate: run.candidate ? {
        ...run.candidate,
        base: run.candidate.base ?? run.candidate.base_sha,
        content_digest: run.candidate.content_digest ?? run.candidate.content_sha256,
      } : null,
      tracker: run.tracker ? {
        ...run.tracker,
        pending: run.tracker.pending ?? run.tracker.state === 'pending',
        conflict: run.tracker.conflict ?? (run.tracker.state === 'conflict' ? 'Tracker synchronization conflict' : null),
        observed: typeof observed === 'string' ? observed : observed == null ? null : JSON.stringify(observed),
      } : null,
      events: result.events ?? run.events,
      evidence: result.evidence ?? run.evidence,
    }
  },
  getService: async (): Promise<ServiceInfo> => {
    const info = await request<ServiceInfo>('/api/service')
    return { ...info, repositories: info.repositories ?? info.policy?.repositories ?? [] }
  },
  newRun: async (body: NewRunRequest): Promise<{ run_id: string; dashboard_url: string; existing: boolean; phase: string }> =>
    command('/api/runs', body),
  answer: async (runId: string, body: { command_id: string; expected_revision: number; decision_id: string; decision_revision: number; candidate_revision?: number | null; answer: string }): Promise<void> => {
    await command(`/api/runs/${encodeURIComponent(runId)}/decision`, body)
  },
  cancel: async (runId: string, body: { command_id: string; expected_revision: number; reason: string }): Promise<void> => {
    await command(`/api/runs/${encodeURIComponent(runId)}/cancel`, body)
  },
  eventUrl: (runId: string, after: number): string => `/api/runs/${encodeURIComponent(runId)}/events?after=${after}`,
}

/** Only backend-provided web links are rendered; reject script/data and credential-bearing URLs. */
export function safeWebUrl(value: string | null | undefined): string | undefined {
  if (!value) return undefined
  try {
    const url = new URL(value, window.location.origin)
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) return undefined
    return url.href
  } catch { return undefined }
}
