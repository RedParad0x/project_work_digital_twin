const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export async function api<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${await res.text()}`);
  }
  return res.json();
}

export async function postApi<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { method: "POST" });
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${await res.text()}`);
  }
  return res.json();
}

export const companies = [
  "NVDA", "TSLA", "AAPL", "META", "GOOGL", "MSFT", "DIS", "NFLX",
  "AMZN", "JPM", "PYPL", "COIN", "XOM", "BA", "RACE"
];

export function pct(n: number, total: number) {
  if (!total) return 0;
  return Math.round((n / total) * 100);
}
