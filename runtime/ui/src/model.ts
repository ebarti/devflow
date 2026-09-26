export type GateState = string | null

export interface Usage {
  input_tokens?: number | null
  cached_input_tokens?: number | null
  output_tokens?: number | null
  reasoning_tokens?: number | null
  total_tokens?: number | null
  status?: string | null
  observed_at?: string | null
  gaps?: string[] | null
}

export interface RunSummary {
  id: string
  run_id?: string | null
  work_id?: string | null
  title?: string | null
  goal?: string | null
  repository?: string | null
  repository_key?: string | null
  issue?: string | null
  issue_url?: string | null
  phase?: string | null
  execution_state?: string | null
  updated_at?: string | null
  created_at?: string | null
  revision?: number | null
  authorized_endpoint?: string | null
}

export interface PhaseGate {
  id: string
  label: string
  state?: GateState
  detail?: string | null
  evidence_refs?: string[] | null
}

export interface RoleState {
  role: string
  state?: string | null
  session_id?: string | null
  attempt_id?: string | null
  model?: string | null
  effort?: string | null
  last_activity_at?: string | null
  usage?: Usage | null
  cleanup?: string | null
  summary?: string | null
  findings?: string[] | null
}

export interface CheckState {
  state?: string | null
  detail?: string | null
  observed_at?: string | null
  evidence_refs?: string[] | null
}

export interface Decision {
  id: string
  revision: number
  candidate_revision?: number | null
  prompt: string
  options: Array<{ value: string; label: string; consequence?: string | null }>
  state?: string | null
}

export interface ActivityEvent {
  sequence: number
  timestamp?: string | null
  type?: string | null
  message?: string | null
  run_revision?: number | null
  evidence_refs?: string[] | null
}

export interface Evidence {
  id: string
  label?: string | null
  type?: string | null
  url?: string | null
  state?: string | null
}

export interface RunDetail extends RunSummary {
  outcome?: string | null
  protocol_revision?: number | null
  error?: string | null
  sequence?: number | null
  observed_at?: string | null
  phase_gates?: PhaseGate[] | null
  roles?: RoleState[] | null
  capacity?: {
    active?: number | null
    queued?: number | null
    limit?: number | null
    cleanup?: string | null
  } | null
  queued?: number | null
  cleanup?: string | null
  candidate?: {
    base?: string | null
    base_sha?: string | null
    head?: string | null
    revision?: number | null
    content_digest?: string | null
    content_sha256?: string | null
    policy_digest?: string | null
    environment_digest?: string | null
  } | null
  pull_request?: {
    number?: number | null
    url?: string | null
    state?: string | null
  } | null
  checks?: { review?: CheckState | null; qa?: CheckState | null; local?: CheckState | null; ci?: CheckState | null } | null
  tracker?: {
    state?: string | null
    desired?: string | null
    observed?: string | null
    pending?: boolean | null
    conflict?: string | null
    readback_at?: string | null
  } | null
  usage?: Usage | null
  decisions?: Decision[] | null
  events?: ActivityEvent[] | null
  evidence?: Evidence[] | null
}

export interface RepositoryInfo {
  key: string
  label?: string | null
  base_ref?: string | null
  base_sha?: string | null
  recovery_keys?: string[] | null
}

export interface ServiceInfo {
  status?: string | null
  version?: string | null
  temporal?: string | { status?: string | null; address?: string | null } | null
  capacity?: { active?: number | null; queued?: number | null; limit?: number | null } | null
  repositories?: RepositoryInfo[] | null
  policy?: { roles?: Record<string, { model?: string | null; effort?: string | null }> | null; repositories?: RepositoryInfo[] | null; authorized_endpoint?: string | null } | null
}

export interface NewRunRequest {
  command_id: string
  run_id: string
  work_id: string
  issue_url: string
  repository_key: string
  goal: string
  accepted_plan: string
  base_ref: string
  branch: string
  authorized_endpoint: 'published_unmerged'
  recovery_key?: string
}
