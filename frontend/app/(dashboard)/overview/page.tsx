"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  BrainCircuit,
  Building2,
  Database,
  FileJson,
  FileText,
  GitCompare,
  GitFork,
  Network,
  Newspaper,
  Play,
  RefreshCw,
  Search,
  TerminalSquare,
  TrendingUp,
  Workflow,
} from "lucide-react";
import { api, postApi, companies } from "@/lib/api";

type Doc = {
  _id: string;
  source: string;
  data_type?: string;
  ingested_at?: string;
  payload?: any;
};

type PipelineItem = {
  key: string;
  label?: string;
  file?: string;
  desc?: string;
  documents?: number;
  docs?: number;
  last_run?: string | null;
  run_state?: { status?: string; message?: string; duration_seconds?: number; exit_code?: number };
};

const ALL_SOURCES = ["gdelt", "newsapi", "reddit", "youtube", "yfinance", "google_trends"];

const PIPELINE_GROUPS = [
  { key: "market", title: "Market Data", keys: ["yfinance"], icon: Database, purpose: "Prezzi OHLCV e volume." },
  { key: "trends", title: "Search & Trends", keys: ["google_trends"], icon: Search, purpose: "Topic e query correlate." },
  { key: "news", title: "News Intelligence", keys: ["gdelt", "newsapi"], icon: Newspaper, purpose: "Eventi e articoli." },
  { key: "social", title: "Social Intelligence", keys: ["reddit", "youtube"], icon: Activity, purpose: "Post, commenti e video." },
  { key: "ai", title: "AI Enrichment", keys: ["nlp"], icon: BrainCircuit, purpose: "Sentiment NLP sui testi." },
];

const MONGO_PRESETS = [
  ["documents_by_source", "Documenti per sorgente"],
  ["avg_sentiment_by_source", "Sentiment medio per sorgente"],
  ["top_mentioned_tickers", "Top ticker menzionati"],
  ["documents_by_day", "Documenti per giorno"],
  ["sentiment_by_ticker", "Sentiment per ticker"],
];

const NEO_PRESETS = [
  ["node_counts", "Nodi per tipo"],
  ["relationship_counts", "Relazioni per tipo"],
  ["top_companies", "Top aziende menzionate"],
  ["negative_companies", "Aziende più negative"],
  ["comentions", "Co-menzioni"],
  ["sector_hierarchy", "Gerarchia settori"],
  ["trending_topics_by_company", "Topic per azienda"],
  ["company_centrality", "Centralità aziende"],
];

function Panel({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <section className={`rounded-2xl border border-outline-variant/30 bg-surface-container/90 shadow-glow ${className}`}>{children}</section>;
}

function SectionTitle({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div>
      <h2 className="text-[11px] font-black uppercase tracking-[0.22em] text-primary">{title}</h2>
      {subtitle && <p className="mt-1 text-sm leading-6 text-on-variant">{subtitle}</p>}
    </div>
  );
}

function Metric({ label, value, detail, tone = "primary", icon }: { label: string; value: string | number; detail?: string; tone?: "primary" | "good" | "error" | "amber" | "secondary"; icon?: React.ReactNode }) {
  const color = tone === "good" ? "text-tertiary" : tone === "error" ? "text-error" : tone === "amber" ? "text-amber" : tone === "secondary" ? "text-secondary" : "text-primary";
  return (
    <div className="rounded-2xl border border-outline-variant/30 bg-surface-low p-4">
      <div className="flex items-start justify-between gap-3">
        <p className="text-[10px] font-bold uppercase tracking-widest text-on-variant">{label}</p>
        {icon && <div className={color}>{icon}</div>}
      </div>
      <p className={`mt-3 text-3xl font-black ${color}`}>{value}</p>
      {detail && <p className="mt-1 text-xs text-outline">{detail}</p>}
    </div>
  );
}

function StatusChip({ status }: { status?: string }) {
  const s = status || "idle";
  const cls = s === "running" || s === "active"
    ? "border-amber/30 bg-amber/10 text-amber"
    : s === "success" || s === "ok"
    ? "border-tertiary/30 bg-tertiary/10 text-tertiary"
    : s === "failed" || s === "error"
    ? "border-error/30 bg-error/10 text-error"
    : "border-outline-variant bg-surface-high text-on-variant";

  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-[10px] font-black uppercase tracking-wider ${cls}`}>{s}</span>;
}

function n(v: any) {
  return Number(v || 0);
}

function titleOf(d: Doc) {
  const p = d.payload || {};
  return p.title || p.description || p.body || p.text || p.keyword || p.ticker || p.channel || p.source_name || "—";
}

function mentionsOf(d: Doc) {
  const p = d.payload || {};
  if (Array.isArray(p.mentions) && p.mentions.length) return p.mentions.join(", ");
  if (typeof p.mentions === "string") return p.mentions;
  if (p.ticker) return p.ticker;
  return "—";
}

function sentimentOf(d: Doc) {
  const p = d.payload || {};
  return p.sentiment_label || p.sentiment || null;
}

function scoreOf(d: Doc) {
  const p = d.payload || {};
  const v = p.sentiment_score ?? p.tone ?? p.score;
  return typeof v === "number" ? v : null;
}

function barWidth(value: number, max: number) {
  if (!max) return "0%";
  return `${Math.max(3, Math.round((value / max) * 100))}%`;
}

export default function StreamlitIntegratedOverview() {
  const [selectedCompany, setSelectedCompany] = useState("NVDA");
  const [enabledSources, setEnabledSources] = useState<string[]>(["gdelt", "newsapi", "reddit", "youtube", "google_trends"]);
  const [sentimentFilter, setSentimentFilter] = useState("all");
  const [selectedPipeline, setSelectedPipeline] = useState("yfinance");
  const [selectedMongoPreset, setSelectedMongoPreset] = useState("documents_by_source");
  const [selectedNeoPreset, setSelectedNeoPreset] = useState("comentions");

  const [overview, setOverview] = useState<any>(null);
  const [docs, setDocs] = useState<Doc[]>([]);
  const [schema, setSchema] = useState<any>(null);
  const [companyFull, setCompanyFull] = useState<any>(null);
  const [compareFull, setCompareFull] = useState<any>(null);
  const [pipelineStatus, setPipelineStatus] = useState<any>(null);
  const [logs, setLogs] = useState("");
  const [mongoAgg, setMongoAgg] = useState<any>(null);
  const [neoQuery, setNeoQuery] = useState<any>(null);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);

  const sourceQuery = enabledSources.length === 1 ? enabledSources[0] : "all";
  const tickerQuery = selectedCompany;

  async function loadData() {
    setMessage("");
    try {
      const docUrl = `/mongodb/documents?source=${sourceQuery}&ticker=${tickerQuery}&sentiment=${sentimentFilter}&limit=350`;

      const [ov, docRes, sch, comp, cmp, pipe, logRes, mg, nq] = await Promise.all([
        api<any>("/overview").catch((e) => ({ error: String(e) })),
        api<any>(docUrl),
        Promise.resolve({ sources: [] }),
        api<any>(`/companies/${selectedCompany}?days=30`).catch((e) => ({ error: String(e) })),
        api<any>(`/compare/${selectedCompany}?days=30`).catch((e) => ({ error: String(e) })),
        api<any>("/pipeline/status").catch((e) => ({ error: String(e), pipeline: [] })),
        api<any>(`/pipeline/logs/${selectedPipeline}?lines=220`).catch((e) => ({ content: "Log pipeline non disponibile." })),
        Promise.resolve({ rows: [] }),
        Promise.resolve({ rows: [] }),
      ]);

      let loadedDocs: Doc[] = docRes.documents || [];
      if (enabledSources.length > 1 && enabledSources.length < ALL_SOURCES.length) {
        loadedDocs = loadedDocs.filter((d) => enabledSources.includes(d.source));
      }

      setOverview(ov);
      setDocs(loadedDocs);
      setSchema(sch);
      setCompanyFull(comp);
      setCompareFull(cmp);
      setPipelineStatus(pipe);
      setLogs(logRes.content || "");
      setMongoAgg(mg);
      setNeoQuery(nq);
    } catch (e: any) {
      setMessage(e?.message || "Errore caricamento dati.");
    }
  }

  async function runPipeline(key: string) {
    setLoading(key);
    setSelectedPipeline(key);
    setMessage("");
    try {
      const res = await postApi<any>(`/pipeline/run/${key}`);
      setMessage(res.message || `${key} avviato.`);
      setTimeout(loadData, 1200);
    } catch (e: any) {
      setMessage(e?.message || `Errore avvio ${key}.`);
    } finally {
      setLoading(null);
    }
  }

  async function runGroup(keys: string[]) {
    setLoading(keys.join("+"));
    setMessage("");
    try {
      for (const key of keys) {
        setSelectedPipeline(key);
        await postApi<any>(`/pipeline/run/${key}`);
      }
      setMessage(`Gruppo avviato: ${keys.join(", ")}`);
      setTimeout(loadData, 1500);
    } catch (e: any) {
      setMessage(e?.message || "Errore avvio gruppo pipeline.");
    } finally {
      setLoading(null);
    }
  }

  async function runFull() {
    setLoading("full");
    setMessage("");
    try {
      let res: any;
      try {
        res = await postApi<any>("/pipeline/run-full");
      } catch {
        res = await postApi<any>("/pipeline/run-all");
      }
      setMessage(res.message || "Full intelligence cycle avviato.");
      setTimeout(loadData, 1500);
    } catch (e: any) {
      setMessage(e?.message || "Errore avvio full cycle.");
    } finally {
      setLoading(null);
    }
  }

  useEffect(() => {
    loadData();
  }, [selectedCompany, sentimentFilter, selectedPipeline, selectedMongoPreset, selectedNeoPreset]);

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(loadData, 10000);
    return () => clearInterval(id);
  }, [autoRefresh, selectedCompany, enabledSources, sentimentFilter, selectedPipeline, selectedMongoPreset, selectedNeoPreset]);

  const sentiment = useMemo(() => {
    const stats = { positive: 0, neutral: 0, negative: 0, missing: 0 };
    docs.forEach((d) => {
      const s = sentimentOf(d);
      if (s === "positive") stats.positive += 1;
      else if (s === "negative") stats.negative += 1;
      else if (s === "neutral") stats.neutral += 1;
      else stats.missing += 1;
    });
    return stats;
  }, [docs]);

  const sentimentReady = sentiment.positive + sentiment.neutral + sentiment.negative;
  const pendingNlp = docs.filter((d) => ["newsapi", "reddit", "youtube"].includes(d.source) && !sentimentOf(d)).length;

  const docsBySource = useMemo(() => {
    const map: Record<string, number> = {};
    ALL_SOURCES.forEach((s) => (map[s] = 0));
    docs.forEach((d) => (map[d.source] = (map[d.source] || 0) + 1));
    return map;
  }, [docs]);

  const maxSource = Math.max(1, ...Object.values(docsBySource));
  const pipeline: PipelineItem[] = pipelineStatus?.pipeline || overview?.pipeline || [];
  const graph = companyFull?.graph ?? {};
  const companyInfo = graph.info ?? {};
  const companyTopics = graph.topics ?? [];

  const companyComentions =
    graph.co_mentions ??
    graph.comentions ??
    graph.coMentions ??
    [];

  const ohlcv = companyFull?.ohlcv || [];
  const recentEvents = companyFull?.recent_events ?? companyFull?.events ?? [];


  const compareMongo = compareFull?.mongo || {};
  const compareNeo = compareFull?.neo4j || {};

  return (
    <div className="space-y-5">
      <Panel className="p-5">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <SectionTitle
              title="Media Twin Dashboard"
              subtitle="Dashboard principale per analizzare aziende, fonti dati, sentiment e relazioni tra eventi."
              />
            <p className="mt-3 max-w-5xl text-sm leading-6 text-on-variant">
              Seleziona un’azienda, scegli le sorgenti da analizzare e consulta documenti raccolti, sentiment, pipeline, dati finanziari e relazioni Neo4j.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            <button onClick={() => setAutoRefresh(!autoRefresh)} className={`inline-flex items-center gap-2 rounded-xl border px-4 py-2 text-xs font-black uppercase tracking-wider ${autoRefresh ? "border-tertiary/40 bg-tertiary/15 text-tertiary" : "border-outline-variant bg-surface-high text-on-variant"}`}>
              <RefreshCw size={14} /> Auto {autoRefresh ? "on" : "off"}
            </button>
            <button onClick={loadData} className="inline-flex items-center gap-2 rounded-xl border border-outline-variant bg-surface-high px-4 py-2 text-xs font-black uppercase tracking-wider text-primary hover:border-primary">
              <RefreshCw size={14} /> Refresh
            </button>
            <button onClick={runFull} disabled={loading === "full"} className="inline-flex items-center gap-2 rounded-xl bg-cyan px-4 py-2 text-xs font-black uppercase tracking-wider text-[#001f24] shadow-glow hover:bg-primary disabled:cursor-not-allowed disabled:opacity-50">
              <Play size={14} /> {loading === "full" ? "Starting..." : "Run full cycle"}
            </button>
          </div>
        </div>

        {message && <div className="mt-4 rounded-xl border border-primary/30 bg-primary/10 p-3 text-sm text-primary">{message}</div>}
      </Panel>

      <section className="grid grid-cols-2 gap-4 xl:grid-cols-6">
        <Metric label="Total docs" value={n(overview?.total_documents).toLocaleString()} detail="MongoDB raw_data" icon={<Database size={18} />} />
        <Metric label="Filtered docs" value={docs.length.toLocaleString()} detail="filtri correnti" icon={<FileText size={18} />} />
        <Metric label="Sentiment ready" value={sentimentReady.toLocaleString()} detail="documenti arricchiti" tone="good" icon={<BrainCircuit size={18} />} />
        <Metric label="Pending NLP" value={pendingNlp.toLocaleString()} detail="testi senza sentiment" tone="error" icon={<AlertTriangle size={18} />} />
        <Metric label="OHLCV points" value={ohlcv.length} detail={selectedCompany} tone="secondary" icon={<TrendingUp size={18} />} />
        <Metric label="Co-mentions" value={companyComentions.length} detail="Neo4j" tone="amber" icon={<Network size={18} />} />
      </section>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 p-5 xl:col-span-3">
          <SectionTitle title="Controls" subtitle="Azienda, fonti e sentiment." />

          <div className="mt-5">
            <p className="mb-2 text-[10px] font-bold uppercase tracking-widest text-on-variant">Company</p>
            <div className="custom-scrollbar flex max-h-40 flex-wrap gap-2 overflow-auto">
              {companies.map((c) => (
                <button key={c} onClick={() => setSelectedCompany(c)} className={`rounded-full border px-3 py-1.5 text-xs font-black ${selectedCompany === c ? "border-cyan bg-cyan text-[#001f24]" : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"}`}>
                  {c}
                </button>
              ))}
            </div>
          </div>

          <div className="mt-5">
            <p className="mb-2 text-[10px] font-bold uppercase tracking-widest text-on-variant">Sources</p>
            <div className="flex flex-wrap gap-2">
              <button onClick={() => setEnabledSources(ALL_SOURCES)} className="rounded-full border border-outline-variant bg-surface-high px-3 py-1.5 text-xs font-bold text-on-variant hover:text-primary">all</button>
              <button onClick={() => setEnabledSources([])} className="rounded-full border border-outline-variant bg-surface-high px-3 py-1.5 text-xs font-bold text-on-variant hover:text-primary">none</button>
              {ALL_SOURCES.map((s) => (
                <button key={s} onClick={() => setEnabledSources((prev) => prev.includes(s) ? prev.filter((x) => x !== s) : [...prev, s])} className={`rounded-full border px-3 py-1.5 text-xs font-black ${enabledSources.includes(s) ? "border-cyan bg-cyan text-[#001f24]" : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>

          <div className="mt-5">
            <p className="mb-2 text-[10px] font-bold uppercase tracking-widest text-on-variant">Sentiment</p>
            <div className="flex flex-wrap gap-2">
              {["all", "positive", "neutral", "negative"].map((s) => (
                <button key={s} onClick={() => setSentimentFilter(s)} className={`rounded-full border px-3 py-1.5 text-xs font-black ${sentimentFilter === s ? "border-cyan bg-cyan text-[#001f24]" : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"}`}>
                  {s}
                </button>
              ))}
            </div>
          </div>

          <button onClick={loadData} className="mt-6 w-full rounded-xl bg-primary px-4 py-3 text-xs font-black uppercase tracking-wider text-[#001f24] hover:bg-cyan">
            Apply filters
          </button>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-5">
          <SectionTitle title={`${selectedCompany} Company Intelligence`} subtitle="Prezzi, topic, co-menzioni ed eventi recenti." />

          <div className="mt-5 grid grid-cols-2 gap-3 xl:grid-cols-4">
            <div className="rounded-xl bg-surface-low p-3">
              <p className="text-[10px] uppercase tracking-widest text-on-variant">Close</p>
              <p className="mt-1 text-2xl font-black text-primary">{companyInfo.close ? `$${Number(companyInfo.close).toFixed(2)}` : "—"}</p>
            </div>
            <div className="rounded-xl bg-surface-low p-3">
              <p className="text-[10px] uppercase tracking-widest text-on-variant">Volume</p>
              <p className="mt-1 text-2xl font-black text-tertiary">{companyInfo.volume ? Number(companyInfo.volume).toLocaleString() : "—"}</p>
            </div>
            <div className="rounded-xl bg-surface-low p-3">
              <p className="text-[10px] uppercase tracking-widest text-on-variant">Topics</p>
              <p className="mt-1 text-2xl font-black text-amber">{companyTopics.length}</p>
            </div>
            <div className="rounded-xl bg-surface-low p-3">
              <p className="text-[10px] uppercase tracking-widest text-on-variant">Events</p>
              <p className="mt-1 text-2xl font-black text-secondary">{recentEvents.length}</p>
            </div>
          </div>

          <div className="mt-4 rounded-xl bg-surface-low p-4">
            <p className="text-[10px] font-bold uppercase tracking-widest text-on-variant">Industry / Sector</p>
            <p className="mt-2 text-sm text-on-surface">{companyInfo.industry || "n/d"} · {companyInfo.sector || "n/d"}</p>
          </div>

          <div className="mt-5 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <div>
              <p className="mb-2 text-[10px] font-bold uppercase tracking-widest text-on-variant">Trending topics</p>
              <div className="flex flex-wrap gap-2">
                {companyTopics.length ? companyTopics.slice(0, 12).map((t: string) => (
                  <span key={t} className="rounded-full border border-tertiary/25 bg-tertiary/10 px-3 py-1.5 text-xs text-tertiary">{t}</span>
                )) : <p className="text-sm text-on-variant">Nessun topic. Run google_trends.</p>}
              </div>
            </div>
            <div>
              <p className="mb-2 text-[10px] font-bold uppercase tracking-widest text-on-variant">Top co-mentions</p>
              <div className="space-y-2">
                {companyComentions.slice(0, 6).map((c: any) => (
                  <div key={c.ticker} className="flex justify-between rounded-xl bg-surface-lowest p-2 text-xs">
                    <span>{selectedCompany} ↔ {c.ticker}</span>
                    <span className="font-mono text-primary">{c.count}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-4">
          <SectionTitle title="Real Sentiment" subtitle="Calcolato sui documenti filtrati." />
          <div className="mt-5 space-y-4">
            {[
              ["positive", sentiment.positive, "bg-tertiary", "text-tertiary"],
              ["neutral", sentiment.neutral, "bg-secondary", "text-secondary"],
              ["negative", sentiment.negative, "bg-error", "text-error"],
              ["missing", sentiment.missing, "bg-outline", "text-outline"],
            ].map(([label, value, bg, txt]: any) => (
              <div key={label}>
                <div className="mb-1 flex justify-between text-xs">
                  <span className={`font-black uppercase ${txt}`}>{label}</span>
                  <span className="font-mono text-on-variant">{Number(value).toLocaleString()}</span>
                </div>
                <div className="h-3 overflow-hidden rounded-full bg-surface-highest">
                  <div className={`h-full rounded-full ${bg}`} style={{ width: barWidth(Number(value), Math.max(1, docs.length)) }} />
                </div>
              </div>
            ))}
          </div>

          <div className="mt-6 rounded-xl border border-outline-variant/25 bg-surface-low p-4 text-sm leading-6 text-on-variant">
            NLP arricchisce NewsAPI, Reddit e YouTube. GDELT può avere già tone/sentiment. YFinance non usa NLP.
          </div>
        </Panel>
      </section>

      <Panel className="p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <SectionTitle title="Pipeline Control" subtitle="Funzioni della vecchia pagina Streamlit Pipeline integrate nel sito moderno." />
          <div className="flex flex-wrap gap-2">
            {PIPELINE_GROUPS.map((g) => (
              <button key={g.key} onClick={() => runGroup(g.keys)} disabled={!!loading} className="rounded-xl border border-outline-variant bg-surface-high px-3 py-2 text-xs font-black uppercase tracking-wider text-primary hover:border-primary disabled:opacity-50">
                Run {g.title}
              </button>
            ))}
          </div>
        </div>

        <div className="mt-5 grid grid-cols-1 gap-4 xl:grid-cols-5">
          {PIPELINE_GROUPS.map((g) => {
            const Icon = g.icon;
            const items = pipeline.filter((p) => g.keys.includes(p.key));
            const docsCount = items.reduce((a, p) => a + n(p.documents ?? p.docs), 0);

            return (
              <div key={g.key} className="rounded-2xl border border-outline-variant/30 bg-surface-low p-4">
                <div className="mb-4 flex items-start gap-3">
                  <div className="grid h-11 w-11 place-items-center rounded-xl border border-primary/20 bg-primary/5 text-primary"><Icon size={20} /></div>
                  <div>
                    <p className="font-bold text-on-surface">{g.title}</p>
                    <p className="mt-1 text-xs leading-5 text-on-variant">{g.purpose}</p>
                  </div>
                </div>

                <p className="text-3xl font-black text-primary">{docsCount.toLocaleString()}</p>
                <p className="text-[10px] uppercase tracking-widest text-outline">documents</p>

                <div className="mt-4 space-y-3">
                  {items.map((p) => {
                    const st = p.run_state?.status || "idle";
                    return (
                      <div key={p.key} className="rounded-xl border border-outline-variant/25 bg-surface-container p-3">
                        <button onClick={() => setSelectedPipeline(p.key)} className="w-full text-left">
                          <div className="flex items-center justify-between gap-2">
                            <span className="text-sm font-bold text-on-surface">{p.label || p.key}</span>
                            <StatusChip status={st} />
                          </div>
                          <p className="mt-1 text-[10px] text-on-variant">{p.file || p.key}</p>
                        </button>

                        <button onClick={() => runPipeline(p.key)} disabled={loading === p.key || st === "running"} className="mt-3 inline-flex w-full items-center justify-center gap-2 rounded-lg bg-cyan px-3 py-2 text-[11px] font-black uppercase tracking-wider text-[#001f24] hover:bg-primary disabled:cursor-not-allowed disabled:opacity-50">
                          <Play size={14} /> {loading === p.key ? "Starting..." : st === "running" ? "Running" : `Run ${p.key}`}
                        </button>
                      </div>
                    );
                  })}
                  {!items.length && <p className="rounded-xl bg-surface-container p-3 text-xs text-on-variant">Nessun modulo. Controlla /pipeline/status.</p>}
                </div>
              </div>
            );
          })}
        </div>
      </Panel>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 overflow-hidden xl:col-span-7">
          <div className="flex items-center justify-between border-b border-outline-variant/20 bg-surface-low/60 p-4">
            <SectionTitle title="MongoDB Data Lake Explorer" subtitle="Documenti raw filtrati, come nella vecchia pagina MongoDB." />
            <FileJson className="text-primary" />
          </div>

          <div className="custom-scrollbar max-h-[680px] overflow-auto">
            <table className="w-full text-left text-sm">
              <thead className="sticky top-0 bg-surface-low/95 text-[10px] uppercase tracking-widest text-outline">
                <tr>
                  <th className="p-4">Source</th>
                  <th className="p-4">Entity</th>
                  <th className="p-4">Title / Payload</th>
                  <th className="p-4">Sentiment</th>
                  <th className="p-4">Score</th>
                  <th className="p-4">Time</th>
                </tr>
              </thead>
              <tbody>
                {docs.map((d) => {
                  const s = sentimentOf(d) || "—";
                  return (
                    <tr key={d._id} className="border-t border-outline-variant/10 hover:bg-surface-high/40">
                      <td className="p-4"><span className="rounded-lg bg-primary/10 px-2 py-1 text-[10px] font-black text-primary">{d.source}</span></td>
                      <td className="max-w-[160px] truncate p-4 text-on-variant">{mentionsOf(d)}</td>
                      <td className="max-w-xl truncate p-4 text-on-surface">{String(titleOf(d)).slice(0, 130)}</td>
                      <td className="p-4">
                        <span className={`rounded-full px-2.5 py-1 text-[10px] font-black uppercase ${s === "positive" ? "bg-tertiary/10 text-tertiary" : s === "negative" ? "bg-error/10 text-error" : s === "neutral" ? "bg-secondary/10 text-secondary" : "bg-surface-high text-on-variant"}`}>{s}</span>
                      </td>
                      <td className="p-4 font-mono text-on-variant">{scoreOf(d)?.toFixed?.(3) ?? "—"}</td>
                      <td className="p-4 font-mono text-outline">{String(d.ingested_at || "").slice(0, 16).replace("T", " ")}</td>
                    </tr>
                  );
                })}
                {!docs.length && <tr><td colSpan={6} className="p-8 text-center text-on-variant">Nessun documento con questi filtri.</td></tr>}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel className="col-span-12 overflow-hidden xl:col-span-5">
          <div className="flex items-center justify-between border-b border-outline-variant/20 bg-surface-low/60 p-4">
            <SectionTitle title={`Pipeline Logs · ${selectedPipeline}`} subtitle="Ultime righe del modulo selezionato." />
            <button onClick={loadData} className="rounded-xl border border-outline-variant bg-surface-high px-4 py-2 text-xs font-black uppercase tracking-wider text-primary hover:border-primary">Refresh</button>
          </div>
          <pre className="custom-scrollbar h-[680px] overflow-auto bg-surface-lowest p-4 text-xs leading-5 text-on-variant">{logs || "Nessun log disponibile."}</pre>
        </Panel>
      </section>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 p-5 xl:col-span-6">
          <div className="flex items-center justify-between gap-3">
            <SectionTitle title="MongoDB Aggregations" subtitle="Preset della vecchia pagina MongoDB." />
            <select value={selectedMongoPreset} onChange={(e) => setSelectedMongoPreset(e.target.value)} className="rounded-xl border border-outline-variant bg-surface-low px-3 py-2 text-sm text-on-surface">
              {MONGO_PRESETS.map(([key, label]) => <option key={key} value={key}>{label}</option>)}
            </select>
          </div>

          <div className="custom-scrollbar mt-5 max-h-[420px] overflow-auto rounded-xl border border-outline-variant/20 bg-surface-lowest">
            <table className="w-full text-left text-xs">
              <tbody>
                {(mongoAgg?.rows || []).slice(0, 30).map((row: any, i: number) => (
                  <tr key={i} className="border-b border-outline-variant/10">
                    <td className="p-3 font-mono text-on-variant">{JSON.stringify(row)}</td>
                  </tr>
                ))}
                {!(mongoAgg?.rows || []).length && <tr><td className="p-5 text-on-variant">Nessun risultato.</td></tr>}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-6">
          <div className="flex items-center justify-between gap-3">
            <SectionTitle title="Neo4j Query Explorer" subtitle="Preset Cypher della vecchia pagina Neo4j." />
            <select value={selectedNeoPreset} onChange={(e) => setSelectedNeoPreset(e.target.value)} className="rounded-xl border border-outline-variant bg-surface-low px-3 py-2 text-sm text-on-surface">
              {NEO_PRESETS.map(([key, label]) => <option key={key} value={key}>{label}</option>)}
            </select>
          </div>

          <div className="custom-scrollbar mt-5 max-h-[420px] overflow-auto rounded-xl border border-outline-variant/20 bg-surface-lowest">
            <table className="w-full text-left text-xs">
              <tbody>
                {(neoQuery?.rows || []).slice(0, 30).map((row: any, i: number) => (
                  <tr key={i} className="border-b border-outline-variant/10">
                    <td className="p-3 font-mono text-on-variant">{JSON.stringify(row)}</td>
                  </tr>
                ))}
                {!(neoQuery?.rows || []).length && <tr><td className="p-5 text-on-variant">Nessun risultato.</td></tr>}
              </tbody>
            </table>
          </div>
        </Panel>
      </section>

      <section className="grid grid-cols-12 gap-4">
        <Panel className="col-span-12 p-5 xl:col-span-6">
          <SectionTitle title="MongoDB vs Neo4j" subtitle="Confronto diretto importato dalla vecchia pagina Confronto." />
          <div className="mt-5 grid grid-cols-2 gap-4">
            <div className="rounded-xl bg-surface-low p-4">
              <div className="mb-3 flex items-center gap-2 text-primary"><Database size={18} /><p className="font-bold">MongoDB</p></div>
              <p className="text-3xl font-black text-primary">{n(compareMongo.total).toLocaleString()}</p>
              <p className="text-xs text-on-variant">documenti su {selectedCompany}</p>
              <div className="mt-4 space-y-2">
                {(compareMongo.by_source || []).slice(0, 6).map((r: any) => (
                  <div key={r.key ?? r._id ?? r.source} className="flex justify-between rounded-lg bg-surface-lowest px-3 py-2 text-xs">
                    <span>{r.key ?? r._id ?? r.source}</span><span className="font-mono text-primary">{r.count}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="rounded-xl bg-surface-low p-4">
              <div className="mb-3 flex items-center gap-2 text-tertiary"><Network size={18} /><p className="font-bold">Neo4j</p></div>
              <p className="text-3xl font-black text-tertiary">{n(compareNeo.total).toLocaleString()}</p>
              <p className="text-xs text-on-variant">event nodes collegati</p>
              <div className="mt-4 space-y-2">
                {(compareNeo.comentions || []).slice(0, 6).map((r: any) => (
                  <div key={r.ticker} className="flex justify-between rounded-lg bg-surface-lowest px-3 py-2 text-xs">
                    <span>{selectedCompany} ↔ {r.ticker}</span><span className="font-mono text-tertiary">{r.count}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Panel>

        <Panel className="col-span-12 p-5 xl:col-span-6">
          <SectionTitle title="MongoDB Schema Flexibility" subtitle="Campi payload per sorgente e sample JSON." />
          <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-2">
            {(schema?.sources || []).map((s: any) => (
              <div key={s.source} className="rounded-xl border border-outline-variant/20 bg-surface-low p-3">
                <div className="flex items-center justify-between gap-2">
                  <p className="font-bold text-on-surface">{s.source}</p>
                  <StatusChip status={s.exists ? "ok" : "missing"} />
                </div>
                <p className="mt-2 text-xs text-on-variant">{s.payload_keys?.length || 0} campi reali · {s.expected_fields?.length || 0} attesi</p>
                <p className="mt-2 line-clamp-2 font-mono text-[10px] text-outline">{(s.payload_keys || []).slice(0, 8).join(", ") || "nessun documento"}</p>
              </div>
            ))}
          </div>
        </Panel>
      </section>
    </div>
  );
}
