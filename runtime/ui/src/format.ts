export function display(value: string | number | null | undefined, fallback = 'Unknown'): string {
  return value === null || value === undefined || value === '' ? fallback : String(value)
}

export function titleCase(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replaceAll('-', ' ').replace(/\b\w/g, char => char.toUpperCase())
}

export function tone(value: string | null | undefined): 'good' | 'active' | 'bad' | 'waiting' | 'unknown' {
  const state = value?.toLowerCase()
  if (!state) return 'unknown'
  if (['passed', 'complete', 'completed', 'delivered', 'success', 'succeeded', 'confirmed', 'healthy'].includes(state)) return 'good'
  if (['running', 'active', 'in_progress', 'in progress', 'implementing', 'reviewing', 'verifying'].includes(state)) return 'active'
  if (['failed', 'blocked', 'conflict', 'error', 'rejected', 'stale'].includes(state)) return 'bad'
  if (['queued', 'pending', 'waiting', 'cancelling', 'requested', 'not_started'].includes(state)) return 'waiting'
  return 'unknown'
}

export function time(value: string | null | undefined): string {
  if (!value) return 'Time unknown'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(date)
}

export function shortTime(value: string | null | undefined): string {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', hour12: false }).format(date)
}

export function timelineTime(value: string | null | undefined): string {
  if (!value) return 'Time unknown'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat('sv-SE', {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(date)
}

export function tokens(value: number | null | undefined): string {
  return value === null || value === undefined ? 'Unknown' : new Intl.NumberFormat().format(value)
}
