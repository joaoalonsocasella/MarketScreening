# MarketScreening
Projeto de fazer screening das empresas de capital aberto brasileiras.


Como isso vai funcionar no seu projeto

 - Você escreve o código (Next.js) no repo MarketScreening.

 - Isso define suas páginas (/market/pulse, /company/...) e APIs (/api/...).

 - O Vercel detecta quando você faz git push.

 - Ele instala as dependências (npm install).

 - Faz build (npm run build).

 - Publica em https://market-screening.vercel.app (ou parecido).

 - Esse código se conecta ao Supabase (onde estão os dados).

 - O app busca as views mv_market_pulse_daily, mv_latest_facts etc.

 - Renderiza no navegador como um BI simples.
 