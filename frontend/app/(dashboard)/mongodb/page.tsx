"use client";

import { useEffect, useMemo, useState } from "react";
import { api, companies } from "@/lib/api";
import { MetricTile, Panel, SectionTitle } from "@/components/IntelCards";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  Activity,
  BarChart3,
  CalendarDays,
  Database,
  FileText,
  RefreshCw,
  TrendingUp,
} from "lucide-react";

type HistoryData = {
  ticker: string;
  days: number;
  metrics?: {
    latest_close?: number | null;
    first_close?: number | null;
    change_pct?: number | null;
    avg_volume?: number | null;
    ohlcv_points?: number;
    events?: number;
  };
  documents_by_day: { day: string; count: number }[];
  documents_by_source: { source: string; count: number }[];
  sentiment_by_day: { day: string; sentiment: string; count: number }[];
  ohlcv: any[];
  events: any[];
};

function n(v: any) {
  return Number(v || 0);
}

function fmtNumber(v: any) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return Number(v).toLocaleString();
}

function fmtMoney(v: any) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return `$${Number(v).toFixed(2)}`;
}

function fmtPct(v: any) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  const num = Number(v);
  return `${num > 0 ? "+" : ""}${num.toFixed(2)}%`;
}

function titleOf(event: any) {
  const p = event.payload || {};
  return (
    p.title ||
    p.video_title ||
    p.post_title ||
    p.description ||
    p.text ||
    p.keyword ||
    p.ticker ||
    "Evento senza titolo"
  );
}

function sentimentOf(event: any) {
  return event.payload?.sentiment_label || event.payload?.sentiment || "—";
}

function toDateLabel(v: any) {
  const raw = String(v || "");
  if (!raw) return "—";
  return raw.slice(0, 10);
}

function normalizeOhlcv(rows: any[]) {
  return (rows || [])
    .map((p) => ({
      date: toDateLabel(p.timestamp || p.date),
      timestamp: p.timestamp || p.date,
      open: p.open !== undefined ? Number(p.open) : null,
      high: p.high !== undefined ? Number(p.high) : null,
      low: p.low !== undefined ? Number(p.low) : null,
      close: p.close !== undefined ? Number(p.close) : null,
      volume: p.volume !== undefined ? Number(p.volume) : null,
    }))
    .filter((p) => p.date !== "—")
    .sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
}

function rollingAverage(rows: any[], window = 7) {
  return rows.map((row, i) => {
    const slice = rows.slice(Math.max(0, i - window + 1), i + 1);
    const vals = slice.map((r) => r.close).filter((v) => typeof v === "number");
    const avg = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
    return {
      ...row,
      ma7: avg,
    };
  });
}

function buildSentimentSeries(rows: HistoryData["sentiment_by_day"]) {
  const map: Record<string, any> = {};

  rows.forEach((r) => {
    if (!map[r.day]) {
      map[r.day] = {
        day: r.day,
        positive: 0,
        neutral: 0,
        negative: 0,
      };
    }

    map[r.day][r.sentiment] = Number(r.count || 0);
  });

  return Object.values(map).sort((a: any, b: any) => a.day.localeCompare(b.day));
}

function buildDocumentsSeries(rows: HistoryData["documents_by_day"]) {
  return (rows || []).map((r) => ({
    day: r.day,
    documents: Number(r.count || 0),
  }));
}

export default function TimeSeriesPage() {
  const [ticker, setTicker] = useState("NVDA");
  const [days, setDays] = useState(365);
  const [data, setData] = useState<HistoryData | null>(null);
  const [loading, setLoading] = useState(false);
  const [marketMode, setMarketMode] = useState<"live" | "mongo">("live");

  async function loadHistory() {
    setLoading(true);
    try {
      const res = await api<HistoryData>(
        `/history/${ticker}?days=${days}&market_mode=${marketMode}`
      );
      setData(res);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadHistory();
  }, [ticker, days, marketMode]);

  const priceSeries = useMemo(() => {
    return rollingAverage(normalizeOhlcv(data?.ohlcv || []), 7);
  }, [data]);

  const documentsSeries = useMemo(() => {
    return buildDocumentsSeries(data?.documents_by_day || []);
  }, [data]);

  const sentimentSeries = useMemo(() => {
    return buildSentimentSeries(data?.sentiment_by_day || []);
  }, [data]);

  const latest = priceSeries.length ? priceSeries[priceSeries.length - 1] : null;
  const first = priceSeries.length ? priceSeries[0] : null;

  const changePct = useMemo(() => {
    if (!latest?.close || !first?.close) return data?.metrics?.change_pct ?? null;
    return ((latest.close - first.close) / first.close) * 100;
  }, [latest, first, data]);

  const avgVolume = useMemo(() => {
    const vals = priceSeries.map((p) => p.volume).filter((v) => typeof v === "number");
    return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : data?.metrics?.avg_volume ?? null;
  }, [priceSeries, data]);

  return (
    <div className="space-y-5">
      <Panel className="p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <SectionTitle
            title="Time Series Analysis"
            subtitle="Analisi temporale di prezzi e volumi da Yahoo Finance, integrata con eventi, fonti e sentiment raccolti dalle pipeline."
          />

          <button
            onClick={loadHistory}
            className="inline-flex items-center gap-2 rounded-xl border border-outline-variant bg-surface-high px-4 py-2 text-xs font-black uppercase tracking-wider text-primary hover:border-primary"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>

        <div className="mt-5 flex flex-wrap gap-2">
          {companies.map((c) => (
            <button
              key={c}
              onClick={() => setTicker(c)}
              className={`rounded-full border px-3 py-1.5 text-xs font-black ${
                ticker === c
                  ? "border-cyan bg-cyan text-[#001f24]"
                  : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"
              }`}
            >
              {c}
            </button>
          ))}
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          {[7, 30, 90, 180, 365].map((d) => (
            <button
              key={d}
              onClick={() => setDays(d)}
              className={`rounded-full border px-3 py-1.5 text-xs font-black ${
                days === d
                  ? "border-tertiary bg-tertiary text-[#001f24]"
                  : "border-outline-variant bg-surface-high text-on-variant hover:border-tertiary hover:text-tertiary"
              }`}
            >
              {d} giorni
            </button>
          ))}
        </div>
      </Panel>

        <div className="flex gap-2">
          <button
            onClick={() => setMarketMode("live")}
            className={`rounded-xl px-3 py-2 text-xs font-bold ${
              marketMode === "live"
                ? "bg-cyan text-[#001f24]"
                : "bg-surface-low text-on-variant"
            }`}
          >
            Live Yahoo
          </button>

          <button
            onClick={() => setMarketMode("mongo")}
            className={`rounded-xl px-3 py-2 text-xs font-bold ${
              marketMode === "mongo"
                ? "bg-cyan text-[#001f24]"
                : "bg-surface-low text-on-variant"
            }`}
          >
            Stored Pipeline
          </button>
        </div>

      <section className="grid grid-cols-2 gap-4 xl:grid-cols-6">
        <MetricTile label="Ultimo close" value={fmtMoney(latest?.close ?? data?.metrics?.latest_close)} tone="primary" />
        <MetricTile label="Variazione periodo" value={fmtPct(changePct)} tone={Number(changePct || 0) >= 0 ? "tertiary" : "error"} />
        <MetricTile label="Volume medio" value={fmtNumber(avgVolume)} tone="secondary" />
        <MetricTile label="Punti OHLCV" value={fmtNumber(priceSeries.length)} tone="primary" />
        <MetricTile label="Eventi media" value={fmtNumber(data?.events?.length || data?.metrics?.events)} tone="tertiary" />
        <MetricTile label="Fonti attive" value={fmtNumber(data?.documents_by_source?.length)} tone="secondary" />
      </section>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 p-5 xl:col-span-8">
          <SectionTitle
            title={`${ticker} · andamento close`}
            subtitle="Prezzo di chiusura storico con media mobile a 7 periodi."
          />

          <div className="mt-5 h-[420px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={priceSeries}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.18} />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={28} />
                <YAxis domain={["auto", "auto"]} tick={{ fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    background: "#111827",
                    border: "1px solid rgba(125,249,255,0.25)",
                    borderRadius: "12px",
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="close"
                  name="Close"
                  stroke="#22d3ee"
                  strokeWidth={2.4}
                  dot={false}
                  activeDot={{ r: 5 }}
                />
                <Line
                  type="monotone"
                  dataKey="ma7"
                  name="Media mobile 7"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  dot={false}
                  strokeDasharray="5 5"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-4">
          <SectionTitle
            title="Documenti per sorgente"
            subtitle="Distribuzione dei dati raccolti nel periodo."
          />

          <div className="mt-5 h-[420px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data?.documents_by_source || []} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" opacity={0.18} />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis dataKey="source" type="category" width={92} tick={{ fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    background: "#111827",
                    border: "1px solid rgba(125,249,255,0.25)",
                    borderRadius: "12px",
                  }}
                />
                <Bar dataKey="count" name="Documenti" fill="#22d3ee" radius={[0, 8, 8, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Panel>
      </section>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 p-5 xl:col-span-7">
          <SectionTitle
            title={`${ticker} · volume scambiato`}
            subtitle="Il volume indica quante azioni sono state scambiate nel periodo."
          />

          <div className="mt-5 h-[360px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={priceSeries}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.18} />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={28} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    background: "#111827",
                    border: "1px solid rgba(125,249,255,0.25)",
                    borderRadius: "12px",
                  }}
                />
                <Bar dataKey="volume" name="Volume" fill="#a78bfa" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-5">
          <SectionTitle
            title="Media signals nel tempo"
            subtitle="Documenti raccolti e sentiment aggregato per giorno."
          />

          <div className="mt-5 h-[360px]">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={documentsSeries}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.18} />
                <XAxis dataKey="day" tick={{ fontSize: 11 }} minTickGap={28} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    background: "#111827",
                    border: "1px solid rgba(125,249,255,0.25)",
                    borderRadius: "12px",
                  }}
                />
                <Bar dataKey="documents" name="Documenti" fill="#22d3ee" radius={[6, 6, 0, 0]} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </Panel>
      </section>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 p-5 xl:col-span-6">
          <SectionTitle
            title="Sentiment nel tempo"
            subtitle="Distribuzione giornaliera dei sentiment processati."
          />

          <div className="mt-5 h-[360px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sentimentSeries}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.18} />
                <XAxis dataKey="day" tick={{ fontSize: 11 }} minTickGap={28} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip
                  contentStyle={{
                    background: "#111827",
                    border: "1px solid rgba(125,249,255,0.25)",
                    borderRadius: "12px",
                  }}
                />
                <Area type="monotone" dataKey="positive" name="Positive" stackId="1" stroke="#34d399" fill="#34d399" />
                <Area type="monotone" dataKey="neutral" name="Neutral" stackId="1" stroke="#60a5fa" fill="#60a5fa" />
                <Area type="monotone" dataKey="negative" name="Negative" stackId="1" stroke="#f87171" fill="#f87171" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-6">
          <SectionTitle
            title="Ultimi punti OHLCV"
            subtitle="Dati finanziari storici salvati dalla pipeline yfinance."
          />

          <div className="custom-scrollbar mt-5 max-h-[360px] overflow-auto rounded-xl border border-outline-variant/20 bg-surface-lowest">
            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-surface-low text-outline">
                <tr>
                  <th className="p-3">Data</th>
                  <th className="p-3">Open</th>
                  <th className="p-3">High</th>
                  <th className="p-3">Low</th>
                  <th className="p-3">Close</th>
                  <th className="p-3">Volume</th>
                </tr>
              </thead>
              <tbody>
                {priceSeries.slice(-80).reverse().map((p, i) => (
                  <tr key={`${p.timestamp}-${i}`} className="border-t border-outline-variant/10">
                    <td className="p-3 font-mono text-on-variant">{p.date}</td>
                    <td className="p-3 font-mono">{fmtMoney(p.open)}</td>
                    <td className="p-3 font-mono">{fmtMoney(p.high)}</td>
                    <td className="p-3 font-mono">{fmtMoney(p.low)}</td>
                    <td className="p-3 font-mono text-primary">{fmtMoney(p.close)}</td>
                    <td className="p-3 font-mono text-tertiary">{fmtNumber(p.volume)}</td>
                  </tr>
                ))}

                {!priceSeries.length && (
                  <tr>
                    <td colSpan={6} className="p-5 text-center text-on-variant">
                      Nessun dato OHLCV disponibile. Esegui la pipeline yfinance.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Panel>
      </section>

      <Panel className="p-5">
        <SectionTitle
          title="Eventi e segnali raccolti"
          subtitle="Ultimi documenti collegati all’azienda nel periodo selezionato."
        />

        <div className="custom-scrollbar mt-5 max-h-[520px] overflow-auto rounded-xl border border-outline-variant/20 bg-surface-lowest">
          <table className="w-full text-left text-sm">
            <thead className="sticky top-0 bg-surface-low text-[10px] uppercase tracking-widest text-outline">
              <tr>
                <th className="p-4">Source</th>
                <th className="p-4">Contenuto</th>
                <th className="p-4">Sentiment</th>
                <th className="p-4">Ingested at</th>
              </tr>
            </thead>
            <tbody>
              {(data?.events || []).map((e: any) => (
                <tr key={e._id} className="border-t border-outline-variant/10 hover:bg-surface-high/40">
                  <td className="p-4">
                    <span className="rounded-lg bg-primary/10 px-2 py-1 text-[10px] font-black text-primary">
                      {e.source || "unknown"}
                    </span>
                  </td>
                  <td className="max-w-4xl truncate p-4 text-on-surface">
                    {String(titleOf(e)).slice(0, 180)}
                  </td>
                  <td className="p-4">
                    <span className="rounded-full bg-surface-high px-2.5 py-1 text-[10px] font-black uppercase text-on-variant">
                      {sentimentOf(e)}
                    </span>
                  </td>
                  <td className="p-4 font-mono text-outline">
                    {String(e.ingested_at || "").slice(0, 19).replace("T", " ")}
                  </td>
                </tr>
              ))}

              {!(data?.events || []).length && (
                <tr>
                  <td colSpan={4} className="p-8 text-center text-on-variant">
                    Nessun evento disponibile per il periodo selezionato.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}