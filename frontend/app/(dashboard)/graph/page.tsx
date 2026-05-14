import { api } from "@/lib/api";
import { MetricTile, Panel, SectionTitle } from "@/components/IntelCards";
import { Network } from "lucide-react";

type CoMention = {
  source: string;
  target: string;
  weight: number;
};

export default async function GraphPage() {
  const data: any = await api("/graph/stats");

  const nodeCounts = data.node_counts || [];
  const relCounts = data.relationship_counts || [];

  const coMentions: CoMention[] =
    data.comentions ??
    data.co_mentions ??
    data.coMentions ??
    [];

  const topEdges = coMentions.slice(0, 24);

  const nodes = Array.from(
    new Set(topEdges.flatMap((e) => [e.source, e.target]))
  );

  const centerX = 420;
  const centerY = 280;
  const radius = 210;

  const positions = Object.fromEntries(
    nodes.map((node, i) => {
      const angle = (i / Math.max(1, nodes.length)) * Math.PI * 2 - Math.PI / 2;
      return [
        node,
        {
          x: centerX + Math.cos(angle) * radius,
          y: centerY + Math.sin(angle) * radius,
        },
      ];
    })
  );

  const maxWeight = Math.max(1, ...topEdges.map((e) => Number(e.weight || 0)));

  return (
    <div className="grid grid-cols-12 gap-4">
      <Panel className="col-span-12 p-5">
        <SectionTitle
          title="Graph Explorer"
          subtitle="Neo4j relationship intelligence: nodi azienda e relazioni di co-menzione basate sugli eventi reali."
        />
      </Panel>

      <section className="col-span-12 grid grid-cols-2 gap-4 lg:grid-cols-5">
        {nodeCounts.slice(0, 5).map((n: any, i: number) => (
          <MetricTile
            key={n.label || i}
            label={`Nodes · ${n.label}`}
            value={Number(n.count || 0).toLocaleString()}
            tone={i % 2 ? "tertiary" : "primary"}
          />
        ))}
      </section>

      <Panel className="col-span-12 p-5 lg:col-span-8">
        <SectionTitle
          title="Co-mention network"
          subtitle="Ogni nodo è un’azienda. Ogni linea indica che due aziende sono citate nello stesso evento."
        />

        <div className="relative mt-5 min-h-[620px] overflow-hidden rounded-2xl border border-outline-variant/25 bg-surface-lowest">
          <div className="absolute inset-0 data-grid-bg opacity-60" />

          <svg
            viewBox="0 0 840 560"
            className="relative h-[620px] w-full"
            role="img"
          >
            {topEdges.map((edge, i) => {
              const a = positions[edge.source];
              const b = positions[edge.target];
              if (!a || !b) return null;

              const strokeWidth = 1 + (Number(edge.weight || 0) / maxWeight) * 7;
              const opacity = 0.25 + (Number(edge.weight || 0) / maxWeight) * 0.55;

              return (
                <g key={`${edge.source}-${edge.target}-${i}`}>
                  <line
                    x1={a.x}
                    y1={a.y}
                    x2={b.x}
                    y2={b.y}
                    stroke="currentColor"
                    strokeWidth={strokeWidth}
                    opacity={opacity}
                    className="text-primary"
                  />

                  <text
                    x={(a.x + b.x) / 2}
                    y={(a.y + b.y) / 2}
                    textAnchor="middle"
                    dominantBaseline="middle"
                    className="fill-on-variant text-[10px] font-mono"
                  >
                    {edge.weight}
                  </text>
                </g>
              );
            })}

            {nodes.map((node) => {
              const p = positions[node];
              const strength = topEdges
                .filter((e) => e.source === node || e.target === node)
                .reduce((sum, e) => sum + Number(e.weight || 0), 0);

              return (
                <g key={node}>
                  <circle
                    cx={p.x}
                    cy={p.y}
                    r={22 + Math.min(18, strength / 12)}
                    className="fill-surface-container stroke-primary"
                    strokeWidth="2"
                  />
                  <text
                    x={p.x}
                    y={p.y + 4}
                    textAnchor="middle"
                    className="fill-primary text-[13px] font-black"
                  >
                    {node}
                  </text>
                </g>
              );
            })}

            <g>
              <circle
                cx={centerX}
                cy={centerY}
                r="54"
                className="fill-tertiary/10 stroke-tertiary"
                strokeWidth="1"
              />
              <foreignObject x={centerX - 42} y={centerY - 42} width="84" height="84">
                <div className="flex h-full flex-col items-center justify-center text-center text-tertiary">
                  <Network size={24} />
                  <span className="mt-1 text-sm font-bold">Media Graph</span>
                </div>
              </foreignObject>
            </g>
          </svg>
        </div>
      </Panel>

      <Panel className="col-span-12 p-5 lg:col-span-4">
        <SectionTitle title="Relationship types" />
        <div className="mt-4 space-y-3">
          {relCounts.map((r: any) => (
            <div
              key={r.type}
              className="flex items-center justify-between rounded-xl border border-outline-variant/20 bg-surface-low p-3"
            >
              <span className="text-sm text-on-surface">{r.type}</span>
              <span className="font-mono text-sm font-bold text-tertiary">
                {Number(r.count).toLocaleString()}
              </span>
            </div>
          ))}
        </div>

        <div className="mt-8 border-t border-outline-variant/20 pt-5">
          <SectionTitle title="Top co-mentions" />
          <div className="mt-4 space-y-2">
            {coMentions.slice(0, 12).map((e: any, i: number) => (
              <div
                key={`${e.source}-${e.target}-${i}`}
                className="flex justify-between rounded-xl bg-surface-low p-3 text-sm"
              >
                <span className="text-on-surface">
                  {e.source} ↔ {e.target}
                </span>
                <span className="font-mono text-primary">{e.weight}</span>
              </div>
            ))}
          </div>
        </div>
      </Panel>
    </div>
  );
}