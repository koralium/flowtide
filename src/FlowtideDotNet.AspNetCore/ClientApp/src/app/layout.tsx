import type { Metadata } from "next";
import "./globals.css";
import { PrometheusProvider } from "./contexts/PrometheusProvider";
import { TimespanProvider } from "./contexts/TimespanProvider";

export const metadata: Metadata = {
  title: "Flowtide",
  description: "",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <PrometheusProvider baseUrl="/v1/api" url={process.env.NEXT_PUBLIC_ROOT_PATH as any}>
        <TimespanProvider relativeTimeMs={1 * 5 * 60 * 1000} relativeRefreshRateMs={5000}>
          <body>{children}</body>
        </TimespanProvider>
      </PrometheusProvider>
    </html>
  );
}
