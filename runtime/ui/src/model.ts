export type GateState = string | null

export interface Usage {
  input_tokens?: number | null
  cached_input_tokens?: number | null
  cache_read_tokens?: number | null
  cache_creation_tokens?: number | null
  output_tokens?: number | null
  reasoning_tokens?: number | null
  total_tokens?: number | null
  status?: string | null
  observed_at?: string | null
  gaps?: string[] | null
  source?: 'role_attempts'
}

export interface FeatureState {
  issue: string
  run_id: string
  status: string
  version: number
  mirror?: { state: string; last_error?: string | null; checked_at?: string | null }
  pull_requests: { url: string; checked_at?: string | null; error?: string | null;
    observation?: { state: string; head: string; base: string; merged_at?: string | null } | null }[]
}

export interface RunSummary {
  feature?: FeatureState | null
  archived?: boolean
  runtime_identity?: RuntimeIdentity | null
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
  execution_retired?: boolean | null
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
  iteration?: number | null
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
  kind?: 'question' | 'plan' | 'merge' | null
  question_id?: string | null
  plan_revision?: number | null
  plan_digest?: string | null
  allow_free_text?: boolean | null
  candidate_revision?: number | null
  prompt: string
  options: Array<string | { value: string; label: string; consequence?: string | null }>
  state?: string | null
  blocker?: { unknown: string; evidence_checked: string[]; why_no_safe_default: string } | null
}

export interface IntakePlan {
  scope: string
  steps: string[]
  verification: string[]
  acceptance: string[]
  workstreams?: Array<{ id: string; title: string; issue_number: number | null; chunks: Array<{ id: string; title: string; acceptance: string[]; depends_on: string[] }> }>
}

export interface IntakeState {
  questions: Array<{ id: string; revision: number; prompt: string; options: string[]; state: string }>
  answers: Array<{ question_id: string; question_revision: number; prompt: string; answer: string }>
  plans: Array<{ revision: number; digest: string; content: IntakePlan; state: string; change_request?: string }>
  accepted_plan?: { revision: number; digest: string; content: IntakePlan; authorization?: { source: 'run_authorization'; command_id: string; request_digest: string; policy_digest: string; authorized_endpoint: string } } | null
  change_requests?: Array<{ plan_revision: number; response: string }>
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

export interface FeaturePlanState {
  plan_identity: {
    plan_revision: number
    plan_digest: string | null
    comment_id: number | null
    comment_node_id: string | null
    workstream_issues: Record<string, { id: string; number: number; url: string }>
  }
  phase: string | null
  reason: string | null
  evidence: Array<{ path: string; sha256: string }>
  affected_chunks: string[]
  repair_budget: { used: number; maximum: number; learning_required: boolean }
  can_revise: boolean
  reason_ineligible: string | null
  expected_revision: number | null
  child_plan_links: Record<string, { comment_id: number; comment_node_id: string; digest: string; url: string }>
  authority: { version: number; allowed_roots: string[]; allowed_files: string[]; protected_paths: string[] }
  expected_paths: Record<string, string[]>
}

export interface RunDetail extends RunSummary {
  feature_plan?: FeaturePlanState | null
  can_steer?: boolean
  steering?: Array<{ id: number; message: string; created_at: string; included_in: Array<{ role: string; job_key: string }> }>
  investigation_adjudication?: {
    raw_status: 'findings'
    raw_findings: string[]
    disposition: { finding1: string; finding2: string; finding3: string; accepted_baseline_medium: number; remaining_blocker_high: number }
    historical_runtime: string
    controller_source: string
    additional_native_execution: false
  } | null
  outcome?: string | null
  protocol_revision?: number | null
  projection_revision?: number | null
  iteration?: number | null
  error?: string | null
  sequence?: number | null
  observed_at?: string | null
  phase_gates?: PhaseGate[] | null
  roles?: RoleState[] | null
  capacity?: {
    active?: number | null
    limit?: number | null
  } | null
  queued?: boolean | null
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
    stack_id?: number | null
    scope_complete?: boolean
    pull_requests?: Array<{ number: number; url: string; chunk_id: string }>
  } | null
  checks?: { review?: CheckState | null; qa?: CheckState | null; local?: CheckState | null; ci?: CheckState | null;
    terminal_tracker_checkpoint?: { waiting?: boolean; closed?: boolean; state?: string; deadline?: string; cycles?: number; attempts?: number } | null } | null
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
  question_notifications?: Array<{ decision_id: string; decision_revision: number; state: string; receipt?: { reason?: string } | null }> | null
  intake?: IntakeState | null
  events?: ActivityEvent[] | null
  feature_delivery?: {
    issue_id: string
    owner: { issue_id: string; run_id: string; store_path: string; generation: number } | null
    github_plan_url: string | null
    ownership_state: string
    can_continue: boolean
    repair_budget: { used: number; maximum: number; learning_required: boolean }
    workers: Array<{ run_id: string; chunk_id: string; workstream_id?: string; issue_url?: string; kind: 'build' | 'chunk'; phase: string; outcome: string | null; cleanup: string; pull_request?: { url: string; number: number } | null }>
  } | null
  evidence?: Evidence[] | null
}

export interface RepositoryInfo {
  github_repo?: string | null
  key: string
  label?: string | null
  base_ref?: string | null
  base_sha?: string | null
  recovery_keys?: string[] | null
}

export interface ServiceInfo {
  repository_access?: RepositoryAccess | null
  runtime_identity?: RuntimeIdentity | null
  status?: string | null
  version?: string | null
  temporal?: string | { status?: string | null; address?: string | null } | null
  capacity?: { active?: number | null; queued?: number | null; limit?: number | null } | null
  repositories?: RepositoryInfo[] | null
  policy?: { roles?: Record<string, { model?: string | null; effort?: string | null }> | null; repositories?: RepositoryInfo[] | null; authorized_endpoint?: string | null; intake_enabled?: boolean | null } | null
}

export interface RepositoryAccess {
  revision: number
  repositories: { name: string; allowed: boolean }[]
}

export interface RuntimeIdentity {
  release: string | null
  revision: string | null
  local_digest: string | null
  dirty?: boolean | null
}

export interface Cohort extends RuntimeIdentity {
  provider: string
  runs: number; terminal: number; active: number; delivered: number; blocked: number; cancelled: number
  first_pass_delivered: number; success_rate: number | null; repairs: number
  unknown_outcomes: number
  median_duration_seconds: number | null; duration_observations: number
  attempts: number; token_observations: number; observed_tokens: number | null
  cost_observations: number; observed_cost_usd: number | null
  roles: Record<string, { attempts: number; observed_tokens: number; token_observations: number }>
}

export interface Statistics { total_runs: number; cohorts: Cohort[]; definitions: string }

export interface NewRunRequest {
  command_id: string
  run_id: string
  work_id: string
  issue_url: string
  repository_key: string
  goal: string
  publication_summary?: string
  accepted_plan?: string
  base_ref: string
  branch: string
  authorized_endpoint: 'published_unmerged'
  recovery_key?: string
}
