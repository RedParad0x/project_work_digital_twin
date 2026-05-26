"use client";

import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, companies } from "@/lib/api";
import { MetricTile, Panel, SectionTitle } from "@/components/IntelCards";
import { Network, RefreshCw } from "lucide-react";

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});

type GraphMode = "overview" | "focus" | "raw";

type CoMention = {
  source: string;
  target: string;
  weight: number;
};

type GraphNode = {
  id: string;
  label?: string;
  name?: string;
  type?: string;
  strength?: number;
  count?: number;
  x?: number;
  y?: number;
};

type GraphLink = {
  source: string | GraphNode;
  target: string | GraphNode;
  type?: string;
  weight?: number;
};

const nodeKey = (node: string | GraphNode | undefined) =>
  typeof node === "string" ? node : node?.id ?? "";

const displayLabel = (node: string | GraphNode | undefined) => {
  if (!node) return "";
  if (typeof node === "string") return node;
  return String(node.label || node.name || node.id || "");
};

const compactLabel = (value: string, max = 34) =>
  value.length > max ? `${value.slice(0, max - 3)}...` : value;

const nodeColor = (type = "Company", alpha = 0.9) => {
  if (type === "Company") return `rgba(34, 211, 238, ${alpha})`;
  if (type === "Event") return `rgba(167, 139, 250, ${alpha})`;
  if (type === "EventCluster") return `rgba(167, 139, 250, ${alpha})`;
  if (type === "Topic") return `rgba(245, 158, 11, ${alpha})`;
  if (type === "TopicCluster") return `rgba(245, 158, 11, ${alpha})`;
  if (type === "Industry") return `rgba(52, 211, 153, ${alpha})`;
  if (type === "Sector") return `rgba(248, 113, 113, ${alpha})`;
  return `rgba(148, 163, 184, ${alpha})`;
};

function drawBadge(
  ctx: CanvasRenderingContext2D,
  node: GraphNode,
  label: string,
  color: string,
  fontSize: number,
  isFocus: boolean
) {
  if (typeof node.x !== "number" || typeof node.y !== "number") return;

  ctx.font = `800 ${fontSize}px Inter, Arial, sans-serif`;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";

  const textWidth = ctx.measureText(label).width;
  const paddingX = 7;
  const paddingY = 4;
  const boxWidth = textWidth + paddingX * 2;
  const boxHeight = fontSize + paddingY * 2;

  ctx.beginPath();
  ctx.roundRect(
    node.x - boxWidth / 2,
    node.y - boxHeight / 2,
    boxWidth,
    boxHeight,
    7
  );
  ctx.fillStyle = isFocus ? "rgba(245, 158, 11, 0.95)" : color;
  ctx.fill();
  ctx.strokeStyle = isFocus
    ? "rgba(245, 158, 11, 1)"
    : "rgba(34, 211, 238, 0.45)";
  ctx.lineWidth = 1;
  ctx.stroke();

  ctx.fillStyle = "#001f24";
  ctx.fillText(label, node.x, node.y + 0.5);
}

export default function GraphPage() {
  const graphRef = useRef<any>(null);

  const [data, setData] = useState<any>(null);
  const [topN, setTopN] = useState(40);
  const [focusCompany, setFocusCompany] = useState("ALL");
  const [hoveredNode, setHoveredNode] = useState<GraphNode | null>(null);
  const [hoveredLink, setHoveredLink] = useState<GraphLink | null>(null);
  const [loading, setLoading] = useState(false);
  const [graphMode, setGraphMode] = useState<GraphMode>("overview");
  const [exploreData, setExploreData] = useState<{
    nodes: GraphNode[];
    links: GraphLink[];
  }>({ nodes: [], links: [] });

  const loadGraph = useCallback(async () => {
    setLoading(true);

    try {
      const res = await api<any>("/graph/stats");
      setData(res);

      const explore = await api<any>(
        `/graph/explore?ticker=${focusCompany}&limit=160`
      ).catch(() => ({ nodes: [], links: [] }));

      setExploreData({
        nodes: explore.nodes || [],
        links: explore.links || [],
      });
    } finally {
      setLoading(false);
    }
  }, [focusCompany]);

  useEffect(() => {
    loadGraph();
  }, [loadGraph]);

  const nodeCounts = data?.node_counts || [];
  const relCounts = data?.relationship_counts || [];

  const coMentions: CoMention[] =
    data?.comentions ?? data?.co_mentions ?? data?.coMentions ?? [];

  const focusedCoMentions = useMemo(() => {
    const base =
      focusCompany === "ALL"
        ? coMentions
        : coMentions.filter(
            (edge) =>
              edge.source === focusCompany || edge.target === focusCompany
          );

    return base.slice(0, topN);
  }, [coMentions, focusCompany, topN]);

  const schemaTotals = useMemo(() => {
    const nodes = nodeCounts.reduce(
      (sum: number, item: any) => sum + Number(item.count || 0),
      0
    );
    const links = relCounts.reduce(
      (sum: number, item: any) => sum + Number(item.count || 0),
      0
    );

    return { nodes, links };
  }, [nodeCounts, relCounts]);

  const focusSummary = useMemo(() => {
    const eventCount = exploreData.nodes.filter(
      (node) => node.type === "Event"
    ).length;
    const topicNodes = exploreData.nodes.filter((node) => node.type === "Topic");
    const topicCount = topicNodes.length;
    const categoryNodes = exploreData.nodes.filter(
      (node) => node.type === "Industry" || node.type === "Sector"
    );
    const connectedCompanies = focusedCoMentions.map((edge) =>
      edge.source === focusCompany ? edge.target : edge.source
    );

    return {
      eventCount,
      topicCount,
      topicNodes,
      categoryNodes,
      connectedCompanies,
    };
  }, [exploreData.nodes, focusedCoMentions, focusCompany]);

  const graphData = useMemo(() => {
    if (graphMode === "raw") {
      const strength: Record<string, number> = {};

      exploreData.links.forEach((link) => {
        const source = nodeKey(link.source);
        const target = nodeKey(link.target);
        const weight = Number(link.weight || 1);

        strength[source] = (strength[source] || 0) + weight;
        strength[target] = (strength[target] || 0) + weight;
      });

      return {
        nodes: exploreData.nodes.map((node) => ({
          ...node,
          strength: Number(node.strength ?? strength[node.id] ?? 1),
        })),
        links: exploreData.links,
      };
    }

    if (graphMode === "focus" && focusCompany !== "ALL") {
      const nodesById: Record<string, GraphNode> = {
        [focusCompany]: {
          id: focusCompany,
          label: focusCompany,
          type: "Company",
          strength: 30,
        },
      };
      const links: GraphLink[] = [];

      focusedCoMentions.slice(0, 10).forEach((edge) => {
        const peer = edge.source === focusCompany ? edge.target : edge.source;
        nodesById[peer] = {
          id: peer,
          label: peer,
          type: "Company",
          strength: edge.weight,
        };
        links.push({
          source: focusCompany,
          target: peer,
          type: "CO_MENTION",
          weight: edge.weight,
        });
      });

      exploreData.nodes
        .filter(
          (node) =>
            node.type &&
            node.type !== "Event" &&
            node.type !== "Topic" &&
            node.type !== "Company"
        )
        .forEach((node) => {
          nodesById[node.id] = {
            ...node,
            strength: 8,
          };
        });

      exploreData.links
        .filter((link) => {
          const source = nodeKey(link.source);
          const target = nodeKey(link.target);
          const sourceNode = nodesById[source];
          const targetNode = nodesById[target];

          return (
            link.type !== "MENZIONATA_IN" &&
            link.type !== "TRENDING_WITH" &&
            sourceNode?.type !== "Event" &&
            targetNode?.type !== "Event" &&
            sourceNode?.type !== "Topic" &&
            targetNode?.type !== "Topic"
          );
        })
        .forEach((link) => {
          links.push({ ...link, weight: Number(link.weight || 1) });
        });

      if (focusSummary.eventCount) {
        const eventClusterId = `events:${focusCompany}`;
        nodesById[eventClusterId] = {
          id: eventClusterId,
          label: `${focusSummary.eventCount} events`,
          type: "EventCluster",
          count: focusSummary.eventCount,
          strength: focusSummary.eventCount,
        };
        links.push({
          source: eventClusterId,
          target: focusCompany,
          type: "MENTION_EVENTS",
          weight: focusSummary.eventCount,
        });
      }

      if (focusSummary.topicCount) {
        const topicClusterId = `topics:${focusCompany}`;
        nodesById[topicClusterId] = {
          id: topicClusterId,
          label: `${focusSummary.topicCount} topics`,
          type: "TopicCluster",
          count: focusSummary.topicCount,
          strength: focusSummary.topicCount,
        };
        links.push({
          source: focusCompany,
          target: topicClusterId,
          type: "TRENDING_TOPICS",
          weight: focusSummary.topicCount,
        });
      }

      return {
        nodes: Object.values(nodesById),
        links,
      };
    }

    const strength: Record<string, number> = {};

    focusedCoMentions.forEach((edge) => {
      strength[edge.source] =
        (strength[edge.source] || 0) + Number(edge.weight || 0);
      strength[edge.target] =
        (strength[edge.target] || 0) + Number(edge.weight || 0);
    });

    const nodes = Object.keys(strength).map((id) => ({
      id,
      label: id,
      type: "Company",
      strength: strength[id],
    }));

    const links = focusedCoMentions.map((edge) => ({
      source: edge.source,
      target: edge.target,
      type: "CO_MENTION",
      weight: Number(edge.weight || 0),
    }));

    return { nodes, links };
  }, [graphMode, exploreData, focusedCoMentions, focusCompany, focusSummary]);

  const maxWeight = Math.max(
    1,
    ...graphData.links.map((edge) => Number(edge.weight || 1))
  );

  useEffect(() => {
    if (!graphRef.current) return;

    graphRef.current
      .d3Force("charge")
      ?.strength(graphMode === "raw" ? -900 : -650);

    graphRef.current.d3Force("link")?.distance((link: GraphLink) => {
      if (graphMode === "overview") return 150;
      if (graphMode === "focus" && link.type === "CO_MENTION") return 190;
      if (graphMode === "focus") return 145;

      const sourceType =
        typeof link.source === "string" ? undefined : link.source?.type;
      const targetType =
        typeof link.target === "string" ? undefined : link.target?.type;

      if (sourceType === "Event" || targetType === "Event") return 190;
      return 135;
    });

    const timer = window.setTimeout(() => {
      graphRef.current?.zoomToFit?.(700, 90);
    }, 900);

    return () => window.clearTimeout(timer);
  }, [graphData, graphMode]);

  const graphTitle =
    graphMode === "overview"
      ? "Co-mention network"
      : graphMode === "focus"
      ? "Company focus"
      : "Raw Neo4j graph";

  const graphSubtitle =
    graphMode === "overview"
      ? "Relazioni aggregate tra aziende citate negli stessi eventi."
      : graphMode === "focus"
      ? "Azienda selezionata, categorie, topic, co-mentions e volume eventi aggregato."
      : "Vista tecnica dei nodi Neo4j reali, inclusi gli eventi singoli.";

  return (
    <div className="grid grid-cols-12 gap-4">
      <Panel className="col-span-12 p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <SectionTitle
            title="Graph Explorer"
            subtitle="Analisi delle relazioni Neo4j senza mostrare ogni evento come nodo principale."
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
        <MetricTile
          label="Neo4j nodes"
          value={schemaTotals.nodes.toLocaleString()}
          tone="primary"
        />
        <MetricTile
          label="Neo4j relationships"
          value={schemaTotals.links.toLocaleString()}
          tone="tertiary"
        />
        {nodeCounts.slice(0, 3).map((node: any, index: number) => (
          <MetricTile
            key={node.label || index}
            label={node.label}
            value={Number(node.count || 0).toLocaleString()}
            tone={index % 2 ? "tertiary" : "primary"}
          />
        ))}
      </section>

      <Panel className="col-span-12 p-5 lg:col-span-9">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <SectionTitle title={graphTitle} subtitle={graphSubtitle} />

          <select
            value={topN}
            onChange={(event) => setTopN(Number(event.target.value))}
            className="rounded-xl border border-outline-variant bg-surface-low px-3 py-2 text-sm text-on-surface"
          >
            <option value={10}>Top 10</option>
            <option value={20}>Top 20</option>
            <option value={40}>Top 40</option>
            <option value={80}>Top 80</option>
          </select>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          {[
            ["overview", "Overview"],
            ["focus", "Company focus"],
            ["raw", "Raw graph"],
          ].map(([mode, label]) => (
            <button
              key={mode}
              onClick={() => setGraphMode(mode as GraphMode)}
              className={`rounded-full border px-3 py-1.5 text-xs font-black ${
                graphMode === mode
                  ? "border-cyan bg-cyan text-[#001f24]"
                  : "border-outline-variant bg-surface-high text-on-variant"
              }`}
            >
              {label}
            </button>
          ))}
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

          {companies.map((company) => (
            <button
              key={company}
              onClick={() => {
                setFocusCompany(company);
                if (graphMode === "overview") setGraphMode("focus");
              }}
              className={`rounded-full border px-3 py-1.5 text-xs font-black ${
                focusCompany === company
                  ? "border-cyan bg-cyan text-[#001f24]"
                  : "border-outline-variant bg-surface-high text-on-variant hover:border-primary hover:text-primary"
              }`}
            >
              {company}
            </button>
          ))}
        </div>

        <div className="mt-5 flex flex-col gap-2 rounded-xl border border-outline-variant/20 bg-surface-low p-3 md:flex-row md:items-center md:justify-between">
          <div className="flex items-center gap-2 text-tertiary">
            <Network size={18} />
            <span className="text-sm font-bold">
              {focusCompany === "ALL" ? "Market graph" : focusCompany}
            </span>
          </div>

          <span className="text-xs text-on-variant">
            {graphMode === "raw"
              ? "Eventi singoli visibili solo in Raw graph."
              : "Eventi aggregati, relazioni principali in evidenza."}
          </span>
        </div>

        <div className="relative mt-4 h-[680px] overflow-hidden rounded-2xl border border-outline-variant/25 bg-surface-lowest">
          <div className="absolute inset-0 data-grid-bg opacity-60" />

          {(hoveredNode || hoveredLink) && (
            <div className="absolute left-4 top-4 z-10 max-w-[420px] rounded-xl border border-primary/30 bg-surface-container px-4 py-3 text-sm shadow-glow">
              {hoveredNode && (
                <>
                  <p className="font-black text-primary">
                    {compactLabel(displayLabel(hoveredNode), 58)}
                  </p>
                  <p className="mt-1 text-xs text-on-variant">
                    Type:{" "}
                    <span className="font-mono text-tertiary">
                      {hoveredNode.type || "Node"}
                    </span>
                  </p>
                  <p className="mt-1 text-xs text-on-variant">
                    Weight:{" "}
                    <span className="font-mono text-tertiary">
                      {hoveredNode.count ?? hoveredNode.strength ?? 0}
                    </span>
                  </p>
                </>
              )}

              {hoveredLink && (
                <>
                  <p className="font-black text-primary">
                    {compactLabel(displayLabel(hoveredLink.source), 30)} -{" "}
                    {compactLabel(displayLabel(hoveredLink.target), 30)}
                  </p>
                  <p className="mt-1 text-xs text-on-variant">
                    Relation:{" "}
                    <span className="font-mono text-tertiary">
                      {hoveredLink.type || "RELATED"}
                    </span>
                  </p>
                  <p className="mt-1 text-xs text-on-variant">
                    Weight:{" "}
                    <span className="font-mono text-tertiary">
                      {hoveredLink.weight ?? 1}
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
            enableNodeDrag={true}
            enableZoomInteraction={true}
            enablePanInteraction={true}
            nodeRelSize={5}
            d3VelocityDecay={0.25}
            d3AlphaDecay={0.015}
            warmupTicks={100}
            cooldownTicks={180}
            linkDirectionalParticles={(link: GraphLink) =>
              graphMode === "raw" && link.type === "MENZIONATA_IN" ? 0 : 1
            }
            linkDirectionalParticleWidth={(link: GraphLink) =>
              Math.max(1, (Number(link.weight || 1) / maxWeight) * 4)
            }
            linkDirectionalParticleSpeed={0.004}
            linkWidth={(link: GraphLink) => {
              if (graphMode === "raw" && link.type === "MENZIONATA_IN") {
                return 0.7;
              }
              if (link.type === "MENTION_EVENTS") return 3;
              return 1 + (Number(link.weight || 0) / maxWeight) * 6;
            }}
            linkColor={(link: GraphLink) => {
              const source = nodeKey(link.source);
              const target = nodeKey(link.target);

              if (
                focusCompany !== "ALL" &&
                (source === focusCompany || target === focusCompany)
              ) {
                return "rgba(245, 158, 11, 0.85)";
              }

              if (link.type === "MENZIONATA_IN") return "rgba(167, 139, 250, 0.18)";
              if (link.type === "MENTION_EVENTS") return "rgba(167, 139, 250, 0.55)";
              if (link.type === "TRENDING_TOPICS") return "rgba(245, 158, 11, 0.6)";
              if (link.type === "BELONGS_TO") return "rgba(52, 211, 153, 0.55)";
              if (link.type === "PART_OF") return "rgba(248, 113, 113, 0.5)";
              if (link.type === "TRENDING_WITH") return "rgba(245, 158, 11, 0.55)";
              return "rgba(34, 211, 238, 0.45)";
            }}
            linkLabel={(link: GraphLink) =>
              `${displayLabel(link.source)} - ${displayLabel(link.target)} - ${
                link.type || "RELATED"
              }`
            }
            nodeLabel={(node: GraphNode) =>
              `${displayLabel(node)} - ${node.type || "Node"} - weight ${
                node.count ?? node.strength ?? 0
              }`
            }
            onNodeHover={(node: GraphNode | null) => {
              setHoveredNode(node || null);
              if (node) setHoveredLink(null);
            }}
            onLinkHover={(link: GraphLink | null) => {
              setHoveredLink(link || null);
              if (link) setHoveredNode(null);
            }}
            onNodeClick={(node: GraphNode) => {
              if (node.type && node.type !== "Company") return;
              setFocusCompany((prev) => (prev === node.id ? "ALL" : node.id));
              if (node.type === "Company") setGraphMode("focus");
            }}
            nodeCanvasObject={(
              node: GraphNode,
              ctx: CanvasRenderingContext2D,
              globalScale: number
            ) => {
              if (typeof node.x !== "number" || typeof node.y !== "number") {
                return;
              }

              const nodeType = node.type || "Company";
              const isEvent = nodeType === "Event";
              const isFocus = focusCompany === node.id;
              const isHovered = hoveredNode?.id === node.id;
              const showLabel =
                graphMode !== "raw" ||
                !isEvent ||
                isHovered ||
                (focusCompany !== "ALL" && globalScale > 2.6);
              const radius =
                (nodeType === "Company" ? 9 : isEvent ? 3.5 : 7) +
                Math.min(
                  isEvent ? 4 : 18,
                  Number(node.count ?? node.strength ?? 0) /
                    (isEvent ? 12 : 18)
                );

              ctx.beginPath();
              ctx.arc(
                node.x,
                node.y,
                isFocus ? radius + 5 : radius,
                0,
                2 * Math.PI,
                false
              );
              ctx.fillStyle = isFocus
                ? "rgba(245, 158, 11, 0.95)"
                : nodeColor(nodeType, isEvent ? 0.5 : 0.85);
              ctx.fill();

              ctx.lineWidth = isFocus ? 3 : isEvent ? 0.7 : 1.5;
              ctx.strokeStyle = isFocus
                ? "rgba(245, 158, 11, 1)"
                : nodeColor(nodeType, isEvent ? 0.45 : 0.9);
              ctx.stroke();

              if (!showLabel) return;

              const label = compactLabel(
                displayLabel(node),
                isEvent
                  ? 42
                  : nodeType === "EventCluster" || nodeType === "TopicCluster"
                  ? 18
                  : 24
              );
              const fontSize = Math.max(9, (isEvent ? 11 : 13) / globalScale);

              drawBadge(
                ctx,
                node,
                label,
                nodeColor(nodeType, 0.85),
                fontSize,
                isFocus
              );
            }}
          />

          {!graphData.links.length && (
            <div className="absolute inset-0 grid place-items-center text-center text-on-variant">
              Nessuna relazione disponibile per il filtro selezionato.
            </div>
          )}
        </div>
      </Panel>

      <Panel className="col-span-12 p-5 lg:col-span-3">
        <SectionTitle title="Neo4j schema" />

        <div className="mt-4 space-y-3">
          {nodeCounts.map((node: any) => (
            <div
              key={node.label}
              className="flex items-center justify-between rounded-xl border border-outline-variant/20 bg-surface-low p-3"
            >
              <span className="text-sm text-on-surface">{node.label}</span>
              <span className="font-mono text-sm font-bold text-tertiary">
                {Number(node.count).toLocaleString()}
              </span>
            </div>
          ))}
        </div>

        {focusCompany !== "ALL" && (
          <div className="mt-8 border-t border-outline-variant/20 pt-5">
            <SectionTitle title={`${focusCompany} focus`} />

            <div className="mt-4 grid grid-cols-2 gap-2">
              <div className="rounded-xl bg-surface-low p-3">
                <p className="text-xs text-on-variant">Events</p>
                <p className="mt-1 font-mono text-lg font-black text-primary">
                  {focusSummary.eventCount.toLocaleString()}
                </p>
              </div>
              <div className="rounded-xl bg-surface-low p-3">
                <p className="text-xs text-on-variant">Topics</p>
                <p className="mt-1 font-mono text-lg font-black text-tertiary">
                  {focusSummary.topicCount.toLocaleString()}
                </p>
              </div>
            </div>

            <div className="mt-4 space-y-2">
              {focusSummary.categoryNodes.slice(0, 4).map((node) => (
                <div
                  key={node.id}
                  className="rounded-xl bg-surface-low p-3 text-sm"
                >
                  <p className="text-xs font-bold uppercase text-on-variant">
                    {node.type}
                  </p>
                  <p className="mt-1 text-on-surface">{displayLabel(node)}</p>
                </div>
              ))}
            </div>

            {!!focusSummary.topicNodes.length && (
              <div className="mt-4 space-y-2">
                <p className="text-xs font-bold uppercase tracking-wider text-on-variant">
                  Topics
                </p>
                {focusSummary.topicNodes.slice(0, 8).map((node) => (
                  <div
                    key={node.id}
                    className="truncate rounded-xl bg-surface-low px-3 py-2 text-sm text-on-surface"
                  >
                    {displayLabel(node)}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        <div className="mt-8 border-t border-outline-variant/20 pt-5">
          <SectionTitle title="Relationship types" />

          <div className="mt-4 space-y-3">
            {relCounts.map((rel: any) => (
              <div
                key={rel.type}
                className="flex items-center justify-between rounded-xl border border-outline-variant/20 bg-surface-low p-3"
              >
                <span className="text-sm text-on-surface">{rel.type}</span>
                <span className="font-mono text-sm font-bold text-tertiary">
                  {Number(rel.count).toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-8 border-t border-outline-variant/20 pt-5">
          <SectionTitle title="Strongest co-mentions" />

          <div className="mt-4 space-y-2">
            {focusedCoMentions.slice(0, 12).map((edge, index) => (
              <button
                key={`${edge.source}-${edge.target}-${index}`}
                onMouseEnter={() =>
                  setHoveredLink({
                    source: edge.source,
                    target: edge.target,
                    type: "CO_MENTION",
                    weight: edge.weight,
                  })
                }
                onMouseLeave={() => setHoveredLink(null)}
                onClick={() => {
                  setFocusCompany(
                    focusCompany === edge.source ? edge.target : edge.source
                  );
                  setGraphMode("focus");
                }}
                className="flex w-full justify-between gap-3 rounded-xl bg-surface-low p-3 text-left text-sm transition hover:bg-surface-high"
              >
                <span className="truncate text-on-surface">
                  {edge.source} - {edge.target}
                </span>

                <span className="font-mono text-primary">{edge.weight}</span>
              </button>
            ))}
          </div>
        </div>
      </Panel>
    </div>
  );
}
