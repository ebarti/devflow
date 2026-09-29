import { useState } from 'react'
import { api } from './api'

export function SignIn({ onSignedIn }: { onSignedIn: () => void }) {
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!token || busy) return
    setBusy(true); setError('')
    try {
      await api.login(token)
      setToken('')
      onSignedIn()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not sign in to the local service.')
    } finally { setBusy(false) }
  }

  return <div className="sign-in-page"><h1>Connect to Devflow</h1>
    <p>Enter the token from the local service CLI. It is used only to establish this browser session.</p>
    <form onSubmit={event => void submit(event)}>
      <label htmlFor="service-token">Local service token</label>
      <input id="service-token" type="password" value={token} onChange={event => setToken(event.target.value)} autoComplete="off" spellCheck={false} required />
      {error ? <p className="form-error" role="alert">{error}</p> : null}
      <button className="primary-button" disabled={busy || !token} type="submit">{busy ? 'Connecting…' : 'Connect'}</button>
    </form>
  </div>
}
