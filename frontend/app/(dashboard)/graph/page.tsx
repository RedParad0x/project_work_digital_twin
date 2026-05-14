"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useRef, useState } from "react";
import { api, companies } from "@/lib/api";
import { MetricTile, Panel, SectionTitle } from "@/components/IntelCards";
import { Network, RefreshCw } from "lucide-react";

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});

type CoMention = {
  source: string;
  target: string;
  weight: number;
};

type GraphNode = {
  id: string;
  strength: number;
};

type GraphLink = {
  source: string;
  target: string;
  weight: number;
};

export default function GraphPage() {
  const graphRef = useRef<any>(null);

  const [data, setData] = useState<any>(null);
  const [topN, setTopN] = useState(40);
  const [focusCompany, setFocusCompany] = useState("ALL");
  const [hoveredNode, setHoveredNode] = useState<any>(null);
  const [hoveredLink, setHoveredLink] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  async function loadGraph() {
    setLoading(true);
    try {
      const res = await api<any>("/graph/stats");
      setData(res);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadGraph();
  }, []);

  const nodeCounts = data?.node_counts || [];
  const relCounts = data?.relationship_counts || [];

  const coMentions: CoMention[] =
    data?.comentions ??
    data?.co_mentions ??
    data?.coMentions ??
    [];

  const filteredEdges = useMemo(() => {
    const base =
      focusCompany === "ALL"
        ? coMentions
        : coMentions.filter(
            (e) => e.source === focusCompany || e.target === focusCompany
          );

    return base.slice(0, topN);
  }, [coMentions, focusCompany, topN]);

  const graphData = useMemo(() => {
    const strength: Record<string, number> = {};

    filteredEdges.forEach((e) => {
      strength[e.source] = (strength[e.source] || 0) + Number(e.weight || 0);
      strength[e.target] = (strength[e.target] || 0) + Number(e.weight || 0);
    });

    const nodes: GraphNode[] = Object.keys(strength).map((id) => ({
      id,
      strength: strength[id],
    }));

    const links: GraphLink[] = filteredEdges.map((e) => ({
      source: e.source,
      target: e.target,
      weight: Number(e.weight || 0),
    }));

    return { nodes, links };
  }, [filteredEdges]);

  const maxWeight = Math.max(
    1,
    ...filteredEdges.map((e) => Number(e.weight || 0))
  );

  useEffect(() => {
    if (!graphRef.current) return;

    graphRef.current.d3Force("charge")?.strength(-550);
    graphRef.current.d3Force("link")?.distance(120);

    setTimeout(() => {
      graphRef.current?.zoomToFit?.(700, 90);
    }, 900);
  }, [graphData]);

  return (
    <div className="grid grid-cols-12 gap-4">
      <Panel className="col-span-12 p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <SectionTitle
            title="Graph Explorer"
            subtitle="Esplorazione interattiva delle relazioni Neo4j tra aziende citate negli stessi eventi."
          />

          <button
            onClick={loadGraph}
            className="inline-flex items-center gap-2 rounded-xl border border-outline-variant bg-surface-high px-4 py-2 text-xs font-black uppercase tracking-wider text-primary hover:border-primary"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>
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

      <Panel className="col-span-12 p-5 lg:col-span-9">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <SectionTitle
            title="Co-mention network"
            subtitle="Clicca su un nodo per focalizzare una singola azienda. Trascina, zooma e ispeziona le relazioni."
          />

          <div className="flex flex-wrap gap-2">
            <select
              value={topN}
              onChange={(e) => setTopN(Number(e.target.value))}
              className="rounded-xl border border-outline-variant bg-surface-low px-3 py-2 text-sm text-on-surface"
            >
              <option value={10}>Top 10</option>
              <option value={20}>Top 20</option>
              <option value={40}>Top 40</option>
              <option value={80}>Top 80</option>
            </select>
          </div>
        </div>

        <div className="mt-5 flex flex-wrap gap-2">
          <button
            onClick={() => setFocusCompany("ALL")}
            className={`rounded-full border px-3 py-1.5 text-xs font-black ${
              focusCompany === "ALL"
                ? "border-cyan bg-cyan text-[#001f24]"
                : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"
            }`}
          >
            ALL
          </button>

          {companies.map((c) => (
            <button
              key={c}
              onClick={() => setFocusCompany(c)}
              className={`rounded-full border px-3 py-1.5 text-xs font-black ${
                focusCompany === c
                  ? "border-cyan bg-cyan text-[#001f24]"
                  : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"
              }`}
            >
              {c}
            </button>
          ))}
        </div>

        <div className="mt-5 flex items-center justify-between rounded-xl border border-outline-variant/20 bg-surface-low p-3">
          <div className="flex items-center gap-2 text-tertiary">
            <Network size={18} />
            <span className="text-sm font-bold">Media Graph</span>
          </div>

          <span className="text-xs text-on-variant">
            Co-menzioni più frequenti = linee più marcate · Aziende più connesse = nodi più grandi
          </span>
        </div>

        <div className="relative mt-4 h-[680px] overflow-hidden rounded-2xl border border-outline-variant/25 bg-surface-lowest">
          <div className="absolute inset-0 data-grid-bg opacity-60" />

          {(hoveredNode || hoveredLink) && (
            <div className="absolute left-4 top-4 z-10 rounded-xl border border-primary/30 bg-surface-container px-4 py-3 text-sm shadow-glow">
              {hoveredNode && (
                <>
                  <p className="font-black text-primary">{hoveredNode.id}</p>
                  <p className="mt-1 text-xs text-on-variant">
                    Centralità:{" "}
                    <span className="font-mono text-tertiary">
                      {hoveredNode.strength}
                    </span>
                  </p>
                  <p className="mt-1 text-xs text-outline">
                    Click per filtrare questa azienda.
                  </p>
                </>
              )}

              {hoveredLink && (
                <>
                  <p className="font-black text-primary">
                    {hoveredLink.source.id ?? hoveredLink.source} ↔{" "}
                    {hoveredLink.target.id ?? hoveredLink.target}
                  </p>
                  <p className="mt-1 text-xs text-on-variant">
                    Eventi condivisi:{" "}
                    <span className="font-mono text-tertiary">
                      {hoveredLink.weight}
                    </span>
                  </p>
                </>
              )}
            </div>
          )}

          <ForceGraph2D
            ref={graphRef}
            graphData={graphData}
            width={1050}
            height={680}
            backgroundColor="rgba(0,0,0,0)"
            cooldownTicks={80}
            enableNodeDrag={true}
            enableZoomInteraction={true}
            enablePanInteraction={true}
            nodeRelSize={5}
            d3VelocityDecay={0.25}
            d3AlphaDecay={0.015}
            warmupTicks={100}
            cooldownTicks={180}
            linkDirectionalParticles={1}
            linkDirectionalParticleWidth={(link: any) =>
              Math.max(1, Number(link.weight || 0) / maxWeight * 4)
            }
            linkDirectionalParticleSpeed={0.004}
            linkWidth={(link: any) =>
              1 + (Number(link.weight || 0) / maxWeight) * 6
            }
            linkColor={(link: any) => {
              const source = link.source.id ?? link.source;
              const target = link.target.id ?? link.target;

              if (
                focusCompany !== "ALL" &&
                (source === focusCompany || target === focusCompany)
              ) {
                return "rgba(245, 158, 11, 0.85)";
              }

              return "rgba(34, 211, 238, 0.45)";
            }}
            linkLabel={(link: any) =>
              `${link.source.id ?? link.source} ↔ ${
                link.target.id ?? link.target
              } · ${link.weight} eventi`
            }
            nodeLabel={(node: any) =>
              `${node.id} · centralità ${node.strength}`
            }
            onNodeHover={(node: any) => {
              setHoveredNode(node || null);
              if (node) setHoveredLink(null);
            }}
            onLinkHover={(link: any) => {
              setHoveredLink(link || null);
              if (link) setHoveredNode(null);
            }}
            onNodeClick={(node: any) => {
              setFocusCompany((prev) => (prev === node.id ? "ALL" : node.id));
            }}
            nodeCanvasObject={(node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
              const label = node.id;
              const fontSize = Math.max(10, 14 / globalScale);
              const radius = 7 + Math.min(18, Number(node.strength || 0) / 18);

              const isFocus = focusCompany === node.id;
              const isRelated =
                focusCompany === "ALL" ||
                filteredEdges.some(
                  (e) =>
                    (e.source === focusCompany && e.target === node.id) ||
                    (e.target === focusCompany && e.source === node.id) ||
                    node.id === focusCompany
                );

              ctx.beginPath();
              ctx.arc(node.x, node.y, isFocus ? radius + 5 : radius, 0, 2 * Math.PI, false);
              ctx.fillStyle = isFocus
                ? "rgba(245, 158, 11, 0.95)"
                : isRelated
                ? "rgba(34, 211, 238, 0.85)"
                : "rgba(148, 163, 184, 0.25)";
              ctx.fill();

              ctx.lineWidth = isFocus ? 3 : 1.5;
              ctx.strokeStyle = isFocus
                ? "rgba(245, 158, 11, 1)"
                : "rgba(125, 249, 255, 0.75)";
              ctx.stroke();

              ctx.font = `800 ${fontSize}px Inter, Arial, sans-serif`;
              ctx.textAlign = "center";
              ctx.textBaseline = "middle";

              const textWidth = ctx.measureText(label).width;
              const paddingX = 7;
              const paddingY = 4;
              const boxWidth = textWidth + paddingX * 2;
              const boxHeight = fontSize + paddingY * 2;

              // badge dietro al testo
              ctx.beginPath();
              ctx.roundRect(
                node.x - boxWidth / 2,
                node.y - boxHeight / 2,
                boxWidth,
                boxHeight,
                7
              );
              ctx.fillStyle = isFocus
                ? "rgba(245, 158, 11, 0.95)"
                : "rgba(255, 255, 255, 0.92)";
              ctx.fill();

              ctx.strokeStyle = isFocus
                ? "rgba(245, 158, 11, 1)"
                : "rgba(34, 211, 238, 0.45)";
              ctx.lineWidth = 1;
              ctx.stroke();

              // testo
              ctx.fillStyle = "#001f24";
              ctx.fillText(label, node.x, node.y + 0.5);
            }}
          />

          {!filteredEdges.length && (
            <div className="absolute inset-0 grid place-items-center text-center text-on-variant">
              Nessuna relazione disponibile per il filtro selezionato.
            </div>
          )}
        </div>
      </Panel>

      <Panel className="col-span-12 p-5 lg:col-span-3">
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
          <SectionTitle title="Strongest relationships" />

          <div className="mt-4 space-y-2">
            {filteredEdges.slice(0, 16).map((e, i) => (
              <button
                key={`${e.source}-${e.target}-${i}`}
                onMouseEnter={() => setHoveredLink(e as any)}
                onMouseLeave={() => setHoveredLink(null)}
                onClick={() =>
                  setFocusCompany(
                    focusCompany === e.source ? e.target : e.source
                  )
                }
                className="flex w-full justify-between rounded-xl bg-surface-low p-3 text-left text-sm transition hover:bg-surface-high"
              >
                <span className="text-on-surface">
                  {e.source} ↔ {e.target}
                </span>

                <span className="font-mono text-primary">{e.weight}</span>
              </button>
            ))}
          </div>
        </div>
      </Panel>
    </div>
  );
}