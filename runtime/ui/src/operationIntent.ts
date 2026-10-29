/** Keep an uncertain command stable across dashboard reopening in this tab. */
export function loadIntent<T extends { command_id: string }>(key: string): T | null {
  try {
    const raw = sessionStorage.getItem(key)
    if (!raw) return null
    const value: unknown = JSON.parse(raw)
    if (!value || typeof value !== 'object' || typeof (value as { command_id?: unknown }).command_id !== 'string') return null
    return value as T
  } catch { return null }
}

export function saveIntent(key: string, value: { command_id: string } | null): void {
  try { if (value) sessionStorage.setItem(key, JSON.stringify(value)); else sessionStorage.removeItem(key) } catch { /* The live component retains its intent. */ }
}
