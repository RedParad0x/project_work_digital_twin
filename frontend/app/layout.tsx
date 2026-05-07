import "./globals.css";

export const metadata = {
  title: "Media Twin",
  description: "Media Digital Twin Intelligence Cockpit",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="it" className="dark">
      <body>{children}</body>
    </html>
  );
}
