import type { NewRunRequest, ProjectBoard, RepositoryAccess, RunDetail, RunSummary, ServiceInfo, Statistics, Usage } from './model'

/** The only HTTP path/contract adapter. No credentials are persisted or put in URLs. */
export class ApiError extends Error {
  constructor(message: string, readonly status: number) {
    super(message)
    this.name = 'ApiError'
  }
}

let pendingSession: Promise<string> | undefined

type BackendTracker = Omit<NonNullable<RunDetail['tracker']>, 'observed'> & { observed?: unknown }
type BackendRunDetail = Omit<RunDetail, 'tracker' | 'usage'> & {
  tracker?: BackendTracker | null
  usage?: Usage | Record<string, Usage | null> | null
}

const tokenFields = ['input_tokens', 'output_tokens', 'cache_read_tokens', 'cache_creation_tokens', 'total_tokens', 'reasoning_tokens', 'cached_input_tokens'] as const

function numericTokens(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
}

function observedUsage(value: unknown): Usage | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  const raw = value as Record<string, unknown>
  if (tokenFields.some(field => field in raw) || 'status' in raw || 'gaps' in raw) return value as Usage
  const entries = Object.entries(raw)
  const usage: Usage = { source: 'role_attempts' }
  const partialFields: string[] = []
  for (const field of tokenFields) {
    const observed = entries.map(([, entry]) => entry && typeof entry === 'object' ? (entry as Record<string, unknown>)[field] : null).filter(numericTokens)
    if (observed.length) {
      usage[field] = observed.reduce((sum, count) => sum + count, 0)
    }
    if (observed.length > 0 && observed.length !== entries.length) partialFields.push(field)
  }
  const missing = entries.filter(([, entry]) => !entry || typeof entry !== 'object' || !tokenFields.some(field => numericTokens((entry as Record<string, unknown>)[field])))
  const gaps = [...missing.map(([role]) => `${role} usage`), ...partialFields.map(field => `${field} for some role attempts`)]
  if (gaps.length) usage.gaps = gaps
  return usage
}

function observedText(value: unknown): string | null {
  if (typeof value === 'string') return value
  if (value == null) return null
  return JSON.stringify(value) ?? null
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

async function session(): Promise<string> {
  // Concurrent first commands share one cookie bootstrap. Renew before each later
  // command so an expired cookie or an API reload never requires user input.
  if (!pendingSession) {
    pendingSession = request<{ csrf_token?: string }>('/api/session').then(result => {
      if (!result.csrf_token) throw new ApiError('The service did not establish CSRF protection.', 0)
      return result.csrf_token
    }).finally(() => { pendingSession = undefined })
  }
  return pendingSession
}

async function command<T>(path: string, body: object): Promise<T> {
  const csrfToken = await session()
  return request<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Devflow-CSRF': csrfToken },
    body: JSON.stringify(body),
  })
}

async function cancellationOperationId(commandId: string): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(commandId))
  return `cancel:${Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('')}`
}

export interface DurableOperation {
  operation_id: string
  run_id: string | null
  kind: string
  stage: string
  state: 'pending' | 'running' | 'succeeded' | 'refused' | 'failed' | 'unknown'
  progress: string
  next_action: string
  result: unknown
  error: string | null
  authority: string | null
}

export const api = {
  createOperation: (body: { operation_id: string; run_id: string | null; kind: string; request: object }): Promise<DurableOperation> => command('/api/operations', body),
  getOperation: (id: string): Promise<DurableOperation> => request(`/api/operations/${encodeURIComponent(id)}`),
  runOperations: (runId: string): Promise<{ operations: DurableOperation[] }> => request(`/api/runs/${encodeURIComponent(runId)}/operations`),
  reconcileOperation: (id: string): Promise<DurableOperation> => command(`/api/operations/${encodeURIComponent(id)}/reconcile`, {}),
  session,
  getProjectBoard: (): Promise<ProjectBoard> => request('/api/project-board'),
  listRuns: async (archived = false): Promise<RunSummary[]> => (await api.listRunsPage(archived)).runs,
  listRunsPage: async (archived = false, cursor?: string): Promise<{ runs: RunSummary[]; next_cursor: string | null }> => {
    const params = new URLSearchParams()
    if (archived) params.set('archived', 'true')
    if (cursor) params.set('cursor', cursor)
    const query = params.toString()
    const page = await request<{ runs: RunSummary[]; next_cursor?: string | null }>(`/api/runs${query ? `?${query}` : ''}`)
    return { ...page, next_cursor: page.next_cursor ?? null }
  },
  getStatistics: (): Promise<Statistics> => request('/api/statistics'),
  resetStatistics: (body: { command_id: string }): Promise<Statistics> => command('/api/statistics/reset', body),
  archive: async (runId: string, body: { command_id: string; expected_revision: number; archived: boolean }): Promise<void> => {
    await command(`/api/runs/${encodeURIComponent(runId)}/archive`, body)
  },
  steer: async (runId: string, body: { command_id: string; expected_revision: number; message: string }): Promise<void> => {
    await command(`/api/runs/${encodeURIComponent(runId)}/steer`, body)
  },
  getRun: async (id: string): Promise<RunDetail> => {
    const result = await request<{ run: BackendRunDetail; events?: RunDetail['events']; evidence?: RunDetail['evidence'] }>(`/api/runs/${encodeURIComponent(id)}`)
    const run = result.run
    const observed = run.tracker?.observed
    return {
      ...run,
      usage: observedUsage(run.usage),
      candidate: run.candidate ? {
        ...run.candidate,
        base: run.candidate.base ?? run.candidate.base_sha,
        content_digest: run.candidate.content_digest ?? run.candidate.content_sha256,
      } : null,
      tracker: run.tracker ? {
        ...run.tracker,
        pending: run.tracker.pending ?? run.tracker.state === 'pending',
        conflict: run.tracker.conflict ?? (run.tracker.state === 'conflict' ? 'Tracker synchronization conflict' : null),
        observed: observedText(observed),
      } : null,
      events: result.events ?? run.events,
      evidence: result.evidence ?? run.evidence,
    }
  },
  saveRepositoryAccess(body: { expected_revision: number; allowed_repositories: string[] }) {
    return command<RepositoryAccess>('/api/settings/repositories', body)
  },
  getService: async (): Promise<ServiceInfo> => {
    const info = await request<ServiceInfo>('/api/service')
    return { ...info, repositories: info.repositories ?? info.policy?.repositories ?? [] }
  },
  newRun: async (body: NewRunRequest): Promise<{ run_id: string; dashboard_url: string; existing: boolean; phase: string }> =>
    command('/api/runs', body),
  continueFeature: async (runId: string, body: { command_id: string; expected_revision: number }): Promise<{ run_id: string; phase: string }> =>
    command(`/api/runs/${encodeURIComponent(runId)}/continue-feature`, body),
  merge: async (runId: string, body: { command_id: string; expected_revision: number; candidate_id: string; head: string; pr_number: number; answer: 'merge' }): Promise<{ run_id: string; phase: string; command_id: string }> =>
    command(`/api/runs/${encodeURIComponent(runId)}/merge`, body),
  reviseFeaturePlan: async (runId: string, body: { command_id: string; expected_revision: number; reason: string }): Promise<{ run_id: string; phase: string; revision_phase: string }> =>
    command(`/api/runs/${encodeURIComponent(runId)}/revise-feature-plan`, body),
  answer: async (runId: string, body: { command_id: string; expected_revision: number; decision_id: string; decision_revision: number; candidate_revision: number; answer: string; response?: string }): Promise<void> => {
    await command(`/api/runs/${encodeURIComponent(runId)}/decision`, body)
  },
  cancel: async (runId: string, body: { command_id: string; expected_revision: number; reason: string }): Promise<void> => {
    let operation: DurableOperation
    try {
      operation = await api.createOperation({
        operation_id: await cancellationOperationId(body.command_id), run_id: runId, kind: 'cancel', request: body,
      })
    } catch (cause) {
      // A transport or envelope-level refusal does not establish the domain
      // outcome; preserve the caller's exact request and inspect the operation.
      throw new ApiError(cause instanceof Error ? cause.message : 'Cancellation status is unavailable.', 0)
    }
    const recorded = (operation as DurableOperation & { request?: unknown })?.request
    if (!operation || typeof operation.operation_id !== 'string' || !operation.operation_id ||
        operation.kind !== 'cancel' || operation.run_id !== runId || !recorded ||
        typeof recorded !== 'object' || Array.isArray(recorded)) {
      throw new ApiError('Cancellation operation identity is unavailable.', 0)
    }
    const request = recorded as Record<string, unknown>
    if (Object.keys(request).length !== 3 || request.command_id !== body.command_id ||
        request.expected_revision !== body.expected_revision || request.reason !== body.reason) {
      throw new ApiError('Cancellation operation does not match the retained request.', 0)
    }
    if (operation.state === 'succeeded') {
      const result = operation.result as { run_id?: unknown; phase?: unknown; revision?: unknown } | null
      if (!result || result.run_id !== runId || typeof result.phase !== 'string' ||
          typeof result.revision !== 'number' || !Number.isInteger(result.revision)) {
        throw new ApiError('Cancellation receipt is incomplete.', 0)
      }
      return
    }
    if (operation.state === 'refused') {
      // The operation owner has recorded a definite refusal. CancelRun's
      // existing 409 path refreshes the run before admitting a new intent.
      throw new ApiError(operation.error || 'Cancellation was refused.', 409)
    }
    throw new ApiError(`Cancellation operation ${operation.operation_id} is ${operation.state}.`, 0)
  },
  reconcileTracker: async (runId: string, body: { command_id: string; expected_revision: number }): Promise<void> => {
    await command(`/api/runs/${encodeURIComponent(runId)}/reconcile-tracker`, body)
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
