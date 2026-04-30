'use strict';
// ============================================================
//  HsHub Frontend Server — sert uniquement le visuel du site
//  Proxifie les appels /api/* vers le serveur data (Codespaces)
//
//  Usage :
//    node frontend-server.js <url-serveur-data>
//
//  Exemples :
//    node frontend-server.js http://localhost:7331
//    node frontend-server.js https://monnom-7331.app.github.dev
// ============================================================
const express = require('express');
const http    = require('http');
const https   = require('https');
const path    = require('path');

const PORT     = process.env.PORT || 3000;
const API_BASE = (process.argv[2] || process.env.API_URL || 'http://localhost:7331').replace(/\/$/, '');

if (!process.argv[2] && !process.env.API_URL) {
  console.warn(`\n  ⚠  Aucune URL data fournie — utilisation de ${API_BASE} par défaut`);
  console.warn(`  Usage : node frontend-server.js https://monnom-7331.app.github.dev\n`);
}

const app = express();

// ── Proxy /api/* → serveur data ───────────────────────────────────────────────
app.use('/api', (req, res) => {
  let targetUrl;
  try { targetUrl = new URL(req.originalUrl, API_BASE); }
  catch(e) { return res.status(500).json({ error: `URL data invalide : ${API_BASE}` }); }

  const lib     = targetUrl.protocol === 'https:' ? https : http;
  const options = {
    hostname : targetUrl.hostname,
    port     : targetUrl.port || (targetUrl.protocol === 'https:' ? 443 : 80),
    path     : targetUrl.pathname + targetUrl.search,
    method   : req.method,
    headers  : { ...req.headers, host: targetUrl.host }
  };

  const proxy = lib.request(options, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });

  proxy.on('error', err => {
    console.error(`[proxy] Erreur : ${err.message}`);
    if (!res.headersSent)
      res.status(502).json({ error: `Serveur data inaccessible (${API_BASE}) : ${err.message}` });
  });

  req.pipe(proxy, { end: true });
});

// ── Fichiers statiques (CSS, JS inline dans le HTML, etc.) ───────────────────
app.use((req, res, next) => {
  if (/\.(bin|csv)$/i.test(req.path)) return res.status(403).end();
  next();
});
app.use(express.static(path.join(__dirname)));

// ── Page principale ───────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'hshub_v5.html')));

// ── Démarrage ─────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  const bar = '═'.repeat(44);
  console.log(`\n╔${bar}╗`);
  console.log(`║  ✓  HsHub Frontend  →  http://localhost:${PORT}       ║`);
  console.log(`║     API proxy       →  ${API_BASE.slice(0,38).padEnd(38)}  ║`);
  console.log(`╚${bar}╝\n`);
});
