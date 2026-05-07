import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}", "./lib/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        background: "#111317",
        surface: "#111317",
        "surface-lowest": "#0c0e12",
        "surface-low": "#1a1c20",
        "surface-container": "#1e2024",
        "surface-high": "#282a2e",
        "surface-highest": "#333539",
        "on-surface": "#e2e2e8",
        "on-variant": "#bac9cc",
        outline: "#849396",
        "outline-variant": "#3b494c",
        primary: "#c3f5ff",
        cyan: "#00e5ff",
        "cyan-dim": "#00daf3",
        tertiary: "#98ffed",
        teal: "#62fae3",
        secondary: "#b9c7e0",
        error: "#ffb4ab",
        danger: "#ffb4ab",
        good: "#62fae3",
        amber: "#f7c66a",
        void: "#0c0e12",
        panel: "#1e2024",
        panel2: "#282a2e",
        line: "#3b494c",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        display: ["Inter", "system-ui", "sans-serif"],
        mono: ["Inter", "ui-monospace", "SFMono-Regular", "monospace"],
      },
      boxShadow: {
        glow: "0 0 50px rgba(0, 229, 255, 0.10)",
        "glow-strong": "0 0 60px rgba(0, 229, 255, 0.18)",
      },
      borderRadius: {
        xl: "0.75rem",
        "2xl": "1rem",
        "3xl": "1.5rem",
      },
    },
  },
  plugins: [],
};

export default config;
