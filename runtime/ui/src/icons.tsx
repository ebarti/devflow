import type { SVGProps } from 'react'

type Props = SVGProps<SVGSVGElement>
const base = { width: 20, height: 20, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.9, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const, 'aria-hidden': true as const }

export function PlayIcon(props: Props) { return <svg {...base} {...props}><path d="m6 3 14 9-14 9V3Z" fill="currentColor" stroke="none" /></svg> }
export function SettingsIcon(props: Props) { return <svg {...base} {...props}><path d="M10.4 2.6h3.2l.5 2.1a7.8 7.8 0 0 1 1.7.7l1.9-1.1L20 6.6l-1.1 1.9c.3.5.5 1.1.7 1.7l2.1.5v3.2l-2.1.5a7.8 7.8 0 0 1-.7 1.7l1.1 1.9-2.3 2.3-1.9-1.1a7.8 7.8 0 0 1-1.7.7l-.5 2.1h-3.2l-.5-2.1a7.8 7.8 0 0 1-1.7-.7l-1.9 1.1L4 18l1.1-1.9a7.8 7.8 0 0 1-.7-1.7L2.3 14v-3.2l2.1-.5a7.8 7.8 0 0 1 .7-1.7L4 6.6l2.3-2.3 1.9 1.1a7.8 7.8 0 0 1 1.7-.7l.5-2.1Z" /><circle cx="12" cy="12.3" r="3.2" /></svg> }
export function PlusIcon(props: Props) { return <svg {...base} {...props}><path d="M12 4v16M4 12h16" /></svg> }
export function ExternalIcon(props: Props) { return <svg {...base} {...props}><path d="M13 5h6v6M19 5l-9 9" /><path d="M19 14v4a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h4" /></svg> }
export function RefreshIcon(props: Props) { return <svg {...base} {...props}><path d="M20 7v5h-5M4 17v-5h5" /><path d="M5.8 9A7 7 0 0 1 18 7l2 5M4 12l2 5a7 7 0 0 0 12.2-2" /></svg> }
export function CheckIcon(props: Props) { return <svg {...base} {...props}><path d="m5 12 4 4L19 6" /></svg> }
export function ChevronIcon(props: Props) { return <svg {...base} {...props}><path d="m9 6 6 6-6 6" /></svg> }
