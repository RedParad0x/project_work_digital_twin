import { ReactNode } from "react";

export function Panel({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`glass-panel panel-highlight ${className}`}>
      {children}
    </section>
  );
}

export function SectionTitle({
  title,
  subtitle,
  right,
}: {
  title: string;
  subtitle?: string;
  right?: ReactNode;
}) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div>
        <h2 className="text-[11px] font-bold uppercase tracking-[0.20em] text-on-variant">{title}</h2>
        {subtitle && <p className="mt-1 text-xs text-outline">{subtitle}</p>}
      </div>
      {right}
    </div>
  );
}

export function MetricTile({
  label,
  value,
  detail,
  tone = "primary",
}: {
  label: string;
  value: string | number;
  detail?: string;
  tone?: "primary" | "tertiary" | "error" | "secondary" | "amber";
}) {
  const toneClass = {
    primary: "text-primary",
    tertiary: "text-tertiary",
    error: "text-error",
    secondary: "text-secondary",
    amber: "text-amber",
  }[tone];

  return (
    <div className="rounded-xl border border-outline-variant/40 bg-surface-low p-4">
      <p className="mb-1 text-[10px] font-bold uppercase tracking-wider text-on-variant">{label}</p>
      <p className={`text-2xl font-bold ${toneClass}`}>{value}</p>
      {detail && <p className="mt-1 text-[10px] text-on-variant/70">{detail}</p>}
    </div>
  );
}

export function StatusChip({ status }: { status?: string }) {
  const s = status || "idle";
  const cls =
    s === "running" || s === "active"
      ? "bg-tertiary/10 text-tertiary border-tertiary/20"
      : s === "success" || s === "ok"
      ? "bg-teal/10 text-teal border-teal/20"
      : s === "failed" || s === "error"
      ? "bg-error/10 text-error border-error/20"
      : "bg-surface-high text-on-variant border-outline-variant";

  return (
    <span className={`inline-flex rounded-full border px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider ${cls}`}>
      {s}
    </span>
  );
}

export function SmoothAreaChart() {
  return (
    <svg className="h-full w-full" preserveAspectRatio="none" viewBox="0 0 100 100">
      <defs>
        <linearGradient id="intelChartGradient" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="#9cf0ff" stopOpacity="0.32" />
          <stop offset="100%" stopColor="#9cf0ff" stopOpacity="0" />
        </linearGradient>
      </defs>
      <path
        className="chart-path"
        d="M0,78 C12,72 20,46 32,45 S43,22 53,27 S64,60 74,54 S84,14 93,18 L100,22 L100,100 L0,100 Z"
        fill="url(#intelChartGradient)"
      />
      <path
        className="chart-path"
        d="M0,78 C12,72 20,46 32,45 S43,22 53,27 S64,60 74,54 S84,14 93,18 L100,22"
        fill="none"
        stroke="#00daf3"
        strokeWidth="2.2"
      />
      <circle cx="32" cy="45" r="2.5" fill="#00e5ff" />
      <circle cx="53" cy="27" r="2.5" fill="#00e5ff" />
      <circle cx="93" cy="18" r="2.5" fill="#00e5ff" />
    </svg>
  );
}
