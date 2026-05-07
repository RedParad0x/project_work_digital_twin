import { api } from "@/lib/api";
import { Panel, SectionTitle, StatusChip } from "@/components/IntelCards";

export default async function MongoPage({
  searchParams,
}: {
  searchParams: { source?: string; ticker?: string; sentiment?: string };
}) {
  const source = searchParams.source || "all";
  const ticker = searchParams.ticker || "all";
  const sentiment = searchParams.sentiment || "all";
  const data: any = await api(`/mongodb/documents?source=${source}&ticker=${ticker}&sentiment=${sentiment}&limit=120`);
  const docs = data.documents || [];

  return (
    <div className="grid grid-cols-12 gap-4">
      <Panel className="col-span-12 p-5">
        <div className="flex items-center justify-between">
          <SectionTitle
            title="Document Lab"
            subtitle="Raw MongoDB documents, flexible payloads and sentiment-enriched records."
          />
          <StatusChip status={`${docs.length} docs`} />
        </div>

        <div className="mt-5 flex flex-wrap gap-2">
          {["all", "gdelt", "newsapi", "reddit", "youtube", "yfinance", "google_trends"].map((s) => (
            <a
              key={s}
              href={`/mongodb?source=${s}`}
              className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${source === s ? "border-primary bg-primary text-on-primary" : "border-outline-variant bg-surface-high text-on-variant"}`}
            >
              {s}
            </a>
          ))}
        </div>
      </Panel>

      <Panel className="col-span-12 overflow-hidden">
        <div className="border-b border-outline-variant/20 bg-surface-low/60 p-4">
          <SectionTitle title="Indexed documents" />
        </div>

        <div className="custom-scrollbar max-h-[720px] overflow-auto">
          <table className="w-full text-left text-sm">
            <thead className="sticky top-0 bg-surface-low/95 text-[10px] uppercase tracking-widest text-outline">
              <tr>
                <th className="p-4">Source</th>
                <th className="p-4">Type</th>
                <th className="p-4">Entity / Title</th>
                <th className="p-4">Sentiment</th>
                <th className="p-4">Score</th>
                <th className="p-4">Ingested</th>
              </tr>
            </thead>
            <tbody>
              {docs.map((d: any) => {
                const title = d.payload?.title || d.payload?.ticker || d.payload?.keyword || d.payload?.source_name || "—";
                const sent = d.payload?.sentiment_label || "—";
                return (
                  <tr key={d._id} className="border-t border-outline-variant/10 hover:bg-surface-high/40">
                    <td className="p-4"><span className="rounded-lg bg-primary/10 px-2 py-1 text-[10px] font-bold text-primary">{d.source}</span></td>
                    <td className="p-4 text-on-variant">{d.data_type}</td>
                    <td className="max-w-xl truncate p-4 text-on-surface">{String(title).slice(0, 110)}</td>
                    <td className="p-4">
                      <span className={`rounded-full px-2.5 py-1 text-[10px] font-bold uppercase ${sent === "positive" ? "bg-tertiary/10 text-tertiary" : sent === "negative" ? "bg-error/10 text-error" : "bg-surface-high text-on-variant"}`}>
                        {sent}
                      </span>
                    </td>
                    <td className="p-4 font-mono text-on-variant">{d.payload?.sentiment_score?.toFixed?.(3) ?? "—"}</td>
                    <td className="p-4 font-mono text-outline">{String(d.ingested_at || "").slice(0, 16).replace("T", " ")}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Panel>

      <Panel className="col-span-12 p-5">
        <SectionTitle title="JSON sample" subtitle="First matching document payload." />
        <pre className="custom-scrollbar mt-4 max-h-[520px] overflow-auto rounded-xl border border-outline-variant/30 bg-surface-lowest p-4 text-xs leading-5 text-on-variant">
          {JSON.stringify(docs[0] || {}, null, 2)}
        </pre>
      </Panel>
    </div>
  );
}
