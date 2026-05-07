import { api } from "@/lib/api";
import { MetricTile, Panel, SectionTitle } from "@/components/IntelCards";
import { Network } from "lucide-react";

export default async function GraphPage() {
  const data: any = await api("/graph/stats");
  const nodeCounts = data.node_counts || [];
  const relCounts = data.relationship_counts || [];
  const comentions = data.comentions || [];

  return (
    <div className="grid grid-cols-12 gap-4">
      <Panel className="col-span-12 p-5">
        <SectionTitle title="Graph Explorer" subtitle="Neo4j relationship intelligence: companies, events, topics, sectors and industries." />
      </Panel>

      <section className="col-span-12 grid grid-cols-2 gap-4 lg:grid-cols-5">
        {nodeCounts.slice(0, 5).map((n: any, i: number) => (
          <MetricTile key={n.label || i} label={`Nodes · ${n.label}`} value={Number(n.count || 0).toLocaleString()} tone={i % 2 ? "tertiary" : "primary"} />
        ))}
      </section>

      <Panel className="col-span-12 p-5 lg:col-span-8">
        <SectionTitle title="Co-mention network" subtitle="Company ↔ Event ↔ Company relationships." />
        <div className="relative mt-5 grid min-h-[520px] place-items-center overflow-hidden rounded-2xl border border-outline-variant/25 bg-surface-lowest">
          <div className="absolute inset-0 data-grid-bg opacity-60" />
          <div className="relative grid h-[420px] w-[80%] place-items-center">
            {comentions.slice(0, 12).map((e: any, i: number) => {
              const angle = (i / Math.max(1, comentions.slice(0, 12).length)) * Math.PI * 2;
              const x = Math.cos(angle) * 42;
              const y = Math.sin(angle) * 38;
              return (
                <div
                  key={`${e.source}-${e.target}-${i}`}
                  className="absolute rounded-full border border-primary/30 bg-primary/10 px-3 py-1 text-xs font-bold text-primary shadow-glow"
                  style={{ transform: `translate(${x * 5}px, ${y * 4}px)` }}
                >
                  {e.source || e.Azienda1} ↔ {e.target || e.Azienda2}
                </div>
              );
            })}
            <div className="grid h-28 w-28 place-items-center rounded-full border border-tertiary/30 bg-tertiary/10 text-center text-sm font-bold text-tertiary shadow-glow-strong">
              <Network />
              <span>Media Graph</span>
            </div>
          </div>
        </div>
      </Panel>

      <Panel className="col-span-12 p-5 lg:col-span-4">
        <SectionTitle title="Relationship types" />
        <div className="mt-4 space-y-3">
          {relCounts.map((r: any) => (
            <div key={r.type} className="flex items-center justify-between rounded-xl border border-outline-variant/20 bg-surface-low p-3">
              <span className="text-sm text-on-surface">{r.type}</span>
              <span className="font-mono text-sm font-bold text-tertiary">{Number(r.count).toLocaleString()}</span>
            </div>
          ))}
        </div>

        <div className="mt-8 border-t border-outline-variant/20 pt-5">
          <SectionTitle title="Top co-mentions" />
          <div className="mt-4 space-y-2">
            {comentions.slice(0, 10).map((e: any, i: number) => (
              <div key={i} className="flex justify-between rounded-xl bg-surface-low p-3 text-sm">
                <span className="text-on-surface">{e.source} ↔ {e.target}</span>
                <span className="font-mono text-primary">{e.weight}</span>
              </div>
            ))}
          </div>
        </div>
      </Panel>
    </div>
  );
}
