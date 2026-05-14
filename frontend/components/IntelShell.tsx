"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Activity, Database, GitCompare, Network, Settings, Workflow, Building2 } from "lucide-react";

const nav = [
  { href: "/overview", label: "Overview", icon: Activity },
  { href: "/graph", label: "Graph Explorer", icon: Network },
  { href: "/mongodb", label: "Document Lab", icon: Database },
];

export function IntelShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [time, setTime] = useState("SYNC");

useEffect(() => {
  const update = () => setTime(new Date().toLocaleTimeString());
  update();
  const id = setInterval(update, 1000);
  return () => clearInterval(id);
}, []);

  return (
    <div className="min-h-screen data-grid-bg">
      <header className="fixed left-0 right-0 top-0 z-50 flex h-16 items-center justify-between border-b border-outline-variant/30 bg-surface-lowest/85 px-6 backdrop-blur-md">
        <div className="flex items-center gap-8">
          <Link href="/overview" className="flex items-center gap-3">
            <div className="grid h-9 w-9 place-items-center rounded-xl bg-cyan text-[#001f24] shadow-glow">
              <Activity size={18} strokeWidth={3} />
            </div>
            <div className="flex items-baseline gap-2">
              <h1 className="text-xl font-bold tracking-tight text-primary">Media Twin</h1>
              <span className="text-xs text-on-variant/60">v3.0</span>
            </div>
          </Link>

          <nav className="hidden items-center gap-6 xl:flex">
            {nav.map((item) => {
              const active = pathname?.startsWith(item.href);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`border-b-2 pb-1 text-xs font-semibold uppercase tracking-wider transition ${
                    active
                      ? "border-primary text-primary"
                      : "border-transparent text-on-variant hover:text-primary"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>

        <div className="flex items-center gap-4">
          <div className="flex items-center gap-3 rounded-xl border border-outline-variant bg-surface-low px-3 py-1.5">
            <div className="flex items-center gap-1.5">
              <span className="h-2 w-2 animate-pulse rounded-full bg-teal shadow-[0_0_8px_#62fae3]" />
              <span className="text-[10px] font-bold uppercase tracking-widest text-on-variant">Live uplink</span>
            </div>
            <div className="h-4 w-px bg-outline-variant" />
            <span className="font-mono text-[11px] text-primary">{time}</span>
          </div>

          <button className="grid h-10 w-10 place-items-center rounded-xl border border-outline-variant text-on-variant transition hover:bg-surface-high hover:text-primary">
            <Settings size={18} />
          </button>
        </div>
      </header>

      <main className="mx-auto max-w-[1600px] px-6 pb-8 pt-24">
        {children}
      </main>
    </div>
  );
}
