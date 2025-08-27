import React from "react";

export const metadata = {
  title: "MarketScreening",
  description: "Protótipo BI financeiro",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  );
}
