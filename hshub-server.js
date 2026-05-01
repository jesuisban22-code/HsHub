'use strict';
// ============================================================
//  HsHub Server  —  node hshub-server.js
//  Sert le frontend + API REST
//  Aucun buffer persistant — fichiers lus uniquement pendant la recherche
// ============================================================
const express = require('express');
const fs      = require('fs');
const path    = require('path');

// better-sqlite3 — optionnel : si absent le serveur tombe en mode scan
let BetterSQLite = null;
try { BetterSQLite = require('better-sqlite3'); } catch(_) {}

const DIR  = process.cwd();
const PORT = (() => {
  const i = process.argv.indexOf('--port');
  return (i >= 0 ? parseInt(process.argv[i+1], 10) : 0) || 7331;
})();

// ── State ─────────────────────────────────────────────────────────────────────
const W = {
  dbs:  [],
  // Fichiers .bin — PAS de buffer en mémoire persistant
  files:       [],    // [{path, rowBase, rowCount}]
  rowBuf:      null,  // Uint8Array  — index dans files[] pour chaque ligne
  rowPos:      null,  // Uint32Array — offset dans le fichier (après le champ dbi)
  rowDbi:      null,  // Uint16Array — valeur dbi par ligne
  binRowCount: 0,
  db:          null,  // better-sqlite3 Database — index inversé (si index.sqlite présent)
  prev: [],
  deleted:     new Set(),
  totalActive: 0,
  loaded:      false,
  loading:     false,
  loadError:   null
};

const F = { dbi:0, ln:1, fn:2, bd:3, st:4, sn:5, pc:6, cy:7, co:8, ni:9, em:10, ph:11, ib:12, ip:13 };

// ── Helpers ───────────────────────────────────────────────────────────────────
const _AC  = {'à':'a','á':'a','â':'a','ä':'a','ã':'a','å':'a','æ':'ae','è':'e','é':'e','ê':'e','ë':'e','ì':'i','í':'i','î':'i','ï':'i','ò':'o','ó':'o','ô':'o','ö':'o','õ':'o','ø':'o','ù':'u','ú':'u','û':'u','ü':'u','ý':'y','ÿ':'y','ñ':'n','ç':'c','ß':'ss','œ':'oe'};
const NnF  = s => !s ? '' : String(s).toLowerCase().replace(/[àáâäãåæèéêëìíîïòóôöõøùúûüýÿñçßœ]/g, c => _AC[c]||c).replace(/\s+/g,' ').trim();
const ND   = s => !s ? '' : String(s).replace(/[-\/.]/g,'');
const fmtB = n => n >= 1e9 ? (n/1e9).toFixed(1)+'GB' : n >= 1e6 ? (n/1e6).toFixed(0)+'MB' : (n/1e3).toFixed(0)+'KB';
const fmtN = n => n >= 1e9 ? (n/1e9).toFixed(2)+'B'  : n >= 1e6 ? (n/1e6).toFixed(1)+'M'  : n >= 1e3 ? (n/1e3).toFixed(0)+'K' : String(n);

const tick = () => new Promise(r => setImmediate(r));

// ── Charge les buffers des fichiers .bin à la demande (libérés après usage) ───
async function loadBufs() {
  return Promise.all(W.files.map(fi => fs.promises.readFile(fi.path)));
}

// ── Binary loader — lecture par morceaux, JAMAIS de readFile(145MB) ──────────
// Alloue max 4MB à la fois → RSS reste bas après démarrage
const SCAN_CHUNK = 4 * 1024 * 1024; // fenêtre glissante 4MB

async function loadBinFile(filePath) {
  const label = path.basename(filePath);
  const size  = (await fs.promises.stat(filePath)).size;
  console.log(`\n[•] ${label} (${fmtB(size)})`);

  const fd    = await fs.promises.open(filePath, 'r');
  const chunk = Buffer.allocUnsafe(SCAN_CHUNK);
  let cBase   = 0;  // offset fichier de chunk[0]
  let cLen    = 0;  // octets valides dans chunk
  let cPos    = 0;  // curseur dans chunk

  // Garantit au moins `need` octets lisibles depuis cPos
  const ensure = async (need) => {
    if (cLen - cPos >= need) return;
    const rem = cLen - cPos;
    if (rem > 0) chunk.copy(chunk, 0, cPos, cLen);
    cBase += cPos; cPos = 0; cLen = rem;
    const { bytesRead } = await fd.read(chunk, cLen, SCAN_CHUNK - cLen, cBase + cLen);
    cLen += bytesRead;
  };

  // Helpers de lecture dans le chunk
  const u8   = ()  => chunk[cPos++];
  const u16  = ()  => { const v = chunk.readUInt16LE(cPos); cPos += 2; return v; };
  const u32  = ()  => { const v = chunk.readUInt32LE(cPos); cPos += 4; return v; };
  const s8   = ()  => { const l = u8();  const s = chunk.toString('utf8', cPos, cPos+l); cPos += l; return s; };
  const s16  = ()  => { const l = u16(); const s = chunk.toString('utf8', cPos, cPos+l); cPos += l; return s; };
  const sk16 = ()  => { cPos += 2 + chunk.readUInt16LE(cPos); }; // skip string16

  // Chargement initial
  await ensure(SCAN_CHUNK);

  // — Magic & header —
  const magic = chunk.toString('ascii', 0, 4); cPos = 4;
  if (magic !== 'HSHB') { await fd.close(); throw new Error(`${label}: magic invalide ("${magic}")`); }
  u16(); // version
  const nb_dbs  = u32();
  const nb_rows = u32();
  const nb_idx  = u32();
  console.log(`    → ${nb_rows.toLocaleString()} lignes, ${nb_dbs} base(s), ${nb_idx} champs indexés`);

  const dbiBase = W.dbs.length;

  // — DBs —
  for (let i = 0; i < nb_dbs; i++) {
    const name  = s16();
    const count = u32();
    W.dbs.push({ id: `preloaded_${dbiBase+i}`, name, count });
  }

  // Tableaux typés — position + dbi seulement (~26MB total pour 3.75M lignes)
  const positions = new Uint32Array(nb_rows);
  const dbis      = new Uint16Array(nb_rows);
  const t0        = Date.now();
  const YIELD     = 100_000;

  for (let i = 0; i < nb_rows; i++) {
    // Worst-case row : 2 + 13*(2+255) = 3343 octets — 4096 est sûr
    if (cLen - cPos < 4096) await ensure(4096);

    positions[i] = cBase + cPos + 2; // offset fichier absolu du 1er champ string (après dbi u16)
    dbis[i]      = u16() + dbiBase;
    for (let f = 0; f < 13; f++) sk16();

    W.totalActive++;
    if ((i+1) % YIELD === 0) {
      const spd = Math.round((i+1) / ((Date.now()-t0) / 1000));
      process.stdout.write(`\r    Lignes: ${(i+1).toLocaleString()}/${nb_rows.toLocaleString()} — ${(spd/1000).toFixed(0)}k/s   `);
      await tick();
    }
  }
  process.stdout.write('\n');

  await fd.close(); // chunk de 4MB libéré ici — index section ignorée

  // Enregistrer le fichier
  W.files.push({ path: filePath, rowBase: W.binRowCount, rowCount: nb_rows });

  // Ouvrir l'index SQLite compagnon si disponible
  if (!W.db && BetterSQLite) {
    const sqlitePath = filePath.replace(/\.bin$/i, '') + '.sqlite';
    if (fs.existsSync(sqlitePath)) {
      try {
        W.db = BetterSQLite(sqlitePath, { readonly: true });
        console.log(`    → Index SQLite chargé (recherche rapide activée)`);
      } catch(e) {
        console.warn(`    ⚠  SQLite non chargé: ${e.message}`);
      }
    }
  }

  // Fusionner dans les tableaux globaux
  const prevCount = W.binRowCount;
  const newCount  = prevCount + nb_rows;
  const newBuf    = new Uint8Array(newCount);
  const newPos    = new Uint32Array(newCount);
  const newDbi    = new Uint16Array(newCount);
  if (prevCount > 0) { newBuf.set(W.rowBuf); newPos.set(W.rowPos); newDbi.set(W.rowDbi); }
  newBuf.fill(W.files.length - 1, prevCount);
  newPos.set(positions, prevCount);
  newDbi.set(dbis, prevCount);
  W.rowBuf = newBuf; W.rowPos = newPos; W.rowDbi = newDbi;
  W.binRowCount = newCount;

  const elapsed = ((Date.now()-t0)/1000).toFixed(1);
  const mem     = Math.round(process.memoryUsage().heapUsed / 1e6);
  console.log(`    ✓  Positions indexées en ${elapsed}s  —  heap: ${mem} MB`);
}

// ── Lecture d'une seule ligne depuis le fichier (pour detail/preview) ─────────
async function rowReadBinSingle(id) {
  const fi  = W.files[W.rowBuf[id]];
  const pos = W.rowPos[id];
  const chunk = Buffer.allocUnsafe(4096);
  const fh  = await fs.promises.open(fi.path, 'r');
  try {
    await fh.read(chunk, 0, 4096, pos);
  } finally {
    await fh.close();
  }
  let p = 0;
  const rs = () => { const l=chunk.readUInt16LE(p); p+=2; const s=chunk.toString('utf8',p,p+l); p+=l; return s; };
  const ln=rs(),fn=rs(),bd=rs(),st=rs(),sn=rs(),pc=rs(),cy=rs(),co=rs(),ni=rs(),em=rs(),ph=rs(),ib=rs(),ip=rs();
  return [W.rowDbi[id], ln, fn, bd, st, sn, pc, cy, co, ni, em, ph, ib, ip];
}

// ── Lecture depuis un buffer déjà chargé (scan / export / fam) ───────────────
function rowReadBin(id, bufs) {
  const b = bufs[W.rowBuf[id]];
  let p = W.rowPos[id];
  const rs = () => { const l=b.readUInt16LE(p); p+=2; const s=b.toString('utf8',p,p+l); p+=l; return s; };
  const ln=rs(),fn=rs(),bd=rs(),st=rs(),sn=rs(),pc=rs(),cy=rs(),co=rs(),ni=rs(),em=rs(),ph=rs(),ib=rs(),ip=rs();
  return [W.rowDbi[id], ln, fn, bd, st, sn, pc, cy, co, ni, em, ph, ib, ip];
}

function rowGet(id, bufs) {
  if (id < W.binRowCount) return rowReadBin(id, bufs);
  const p = W.prev[id - W.binRowCount]; return p || null;
}

function rowPreview(id, bufs) {
  const p = rowGet(id, bufs); if (!p) return null;
  const db = W.dbs[p[F.dbi]]; if (!db) return null;
  return { id, dbName: db.name, ln: p[F.ln], fn: p[F.fn], bd: p[F.bd], pc: p[F.pc], cy: p[F.cy] };
}

function rowDetail(id, bufs) {
  const p = rowGet(id, bufs); if (!p) return null;
  const db = W.dbs[p[F.dbi]]; if (!db) return null;
  const all = {};
  const add = (k,v) => { if (v && v !== '0') all[k] = v; };
  add('Nom', p[F.ln]); add('Prénom', p[F.fn]); add('Date naissance', p[F.bd]);
  add('Rue', p[F.st]); add('Numéro', p[F.sn]); add('Code postal', p[F.pc]);
  add('Ville', p[F.cy]); add('Pays', p[F.co]); add('ID National', p[F.ni]);
  add('Email', p[F.em]); add('Téléphone', p[F.ph]); add('IBAN', p[F.ib]); add('IP', p[F.ip]);
  return {
    ln: p[F.ln], fn: p[F.fn], bd: p[F.bd],
    st: [p[F.st], p[F.sn]].filter(Boolean).join(' '),
    pc: p[F.pc], cy: p[F.cy], co: p[F.co], ni: p[F.ni],
    em: p[F.em], ph: p[F.ph], ib: p[F.ib], ip: p[F.ip],
    _all: all
  };
}

// ── Search — scan séquentiel dans les buffers temporaires ────────────────────
function mt(raw, term) {
  if (!term) return true;
  if (term.endsWith('*')) return raw.startsWith(term.slice(0, -1));
  return raw === term;
}

async function scanSearch(q, bufs) {
  const qLN   = q.qLN     ? NnF(q.qLN)                                : null;
  const qFN   = q.qFN     ? NnF(q.qFN)                                : null;
  const qBF   = q.qBF     ? ND(q.qBF)                                 : null;
  const qYear  = !qBF && q.qYear  ? q.qYear.slice(0, 4)               : null;
  const qMonth = !qBF && q.qMonth ? q.qMonth                          : null;
  const qDay   = !qBF && q.qDay   ? q.qDay                            : null;
  const qST   = q.qStreet  ? NnF(q.qStreet)                           : null;
  const qPC   = q.qPostal  ? q.qPostal.trim()                         : null;
  const qCY   = q.qCity    ? NnF(q.qCity)                             : null;
  const qCO   = q.qCountry ? NnF(q.qCountry)                          : null;
  const qNI   = q.qNID     ? q.qNID.replace(/[\s\-.]/g, '')           : null;
  const qEM   = q.qEmail   ? NnF(q.qEmail)                            : null;
  const qPH   = q.qPhone   ? q.qPhone.replace(/\D/g, '')              : null;
  const qIB   = q.qIban    ? q.qIban.replace(/\s/g,'').toLowerCase()  : null;
  const qIP   = q.qIp      ? q.qIp.trim()                             : null;

  const allIds = [];
  const seen   = new Set();
  const total  = W.binRowCount + W.prev.length;
  const YIELD  = 5_000;

  for (let id = 0; id < total; id++) {
    if (id > 0 && id % YIELD === 0) await tick();
    if (W.deleted.has(id)) continue;

    if (id < W.binRowCount) {
      const b  = bufs[W.rowBuf[id]];
      let   bp = W.rowPos[id];
      const nF = () => { const l=b.readUInt16LE(bp); bp+=2; const s=b.toString('utf8',bp,bp+l); bp+=l; return s; };
      const sF = () => { bp += 2 + b.readUInt16LE(bp); };

      const rawLN = nF();
      if (qLN && !mt(NnF(rawLN), qLN)) continue;
      const rawFN = nF();
      if (qFN && !mt(NnF(rawFN), qFN)) continue;
      const rawBD = nF();
      if (qBF   && !mt(ND(rawBD), qBF))    continue;
      if (qYear  && !rawBD.includes(qYear))  continue;
      if (qMonth && !rawBD.includes(qMonth)) continue;
      if (qDay   && !rawBD.includes(qDay))   continue;
      const rawST = qST ? nF() : (sF(), '');
      if (qST && !mt(NnF(rawST), qST)) continue;
      sF(); // sn — jamais filtré
      const rawPC = nF();
      if (qPC && !mt(rawPC, qPC)) continue;
      const rawCY = qCY ? nF() : (sF(), '');
      if (qCY && !mt(NnF(rawCY), qCY)) continue;
      const rawCO = qCO ? nF() : (sF(), '');
      if (qCO && !mt(NnF(rawCO), qCO)) continue;
      const rawNI = qNI ? nF() : (sF(), '');
      if (qNI && !mt(rawNI.replace(/[\s\-.]/g,''), qNI)) continue;
      const rawEM = qEM ? nF() : (sF(), '');
      if (qEM && !mt(NnF(rawEM), qEM)) continue;
      const rawPH = qPH ? nF() : (sF(), '');
      if (qPH && !mt(rawPH.replace(/\D/g,''), qPH)) continue;
      const rawIB = qIB ? nF() : (sF(), '');
      if (qIB && !mt(rawIB.replace(/\s/g,'').toLowerCase(), qIB)) continue;
      if (qIP) { const rawIP = nF(); if (!mt(rawIP.trim(), qIP)) continue; }

      const key = NnF(rawLN)+'|'+NnF(rawFN)+'|'+ND(rawBD)+'|'+rawPC;
      if (seen.has(key)) continue;
      seen.add(key); allIds.push(id);

    } else {
      const p = W.prev[id - W.binRowCount]; if (!p) continue;
      if (qLN && !mt(p[F.ln], qLN)) continue;
      if (qFN && !mt(p[F.fn], qFN)) continue;
      if (qBF   && !mt(ND(p[F.bd]), qBF))     continue;
      if (qYear  && !p[F.bd].includes(qYear))  continue;
      if (qMonth && !p[F.bd].includes(qMonth)) continue;
      if (qDay   && !p[F.bd].includes(qDay))   continue;
      if (qST && !mt(p[F.st], qST)) continue;
      if (qPC && !mt(p[F.pc], qPC)) continue;
      if (qCY && !mt(p[F.cy], qCY)) continue;
      if (qCO && !mt(p[F.co], qCO)) continue;
      if (qNI && !mt(p[F.ni], qNI)) continue;
      if (qEM && !mt(p[F.em], qEM)) continue;
      if (qPH && !mt(p[F.ph], qPH)) continue;
      if (qIB && !mt(p[F.ib], qIB)) continue;
      if (qIP && !mt(p[F.ip], qIP)) continue;
      const key = p[F.ln]+'|'+p[F.fn]+'|'+p[F.bd]+'|'+p[F.pc];
      if (seen.has(key)) continue;
      seen.add(key); allIds.push(id);
    }
  }
  return allIds;
}

// ── Recherche SQLite — index inversé, O(résultats) pas O(total) ──────────────

// Intersecte deux Uint32Array triées en O(a+b)
function intersect(a, b) {
  const r = new Uint32Array(Math.min(a.length, b.length));
  let i = 0, j = 0, n = 0;
  while (i < a.length && j < b.length) {
    if      (a[i] === b[j]) { r[n++] = a[i]; i++; j++; }
    else if (a[i]  <  b[j]) i++;
    else                     j++;
  }
  return r.subarray(0, n);
}

// Union + tri de plusieurs BLOBs d'IDs (pour les requêtes préfixe/wildcard)
function mergeBlobs(blobs) {
  let total = 0;
  for (const b of blobs) total += b.byteLength >> 2;
  const all = new Uint32Array(total);
  let off = 0;
  for (const b of blobs) {
    const arr = new Uint32Array(b.buffer, b.byteOffset, b.byteLength >> 2);
    all.set(arr, off); off += arr.length;
  }
  all.sort();
  return all;
}

// Récupère les IDs pour un champ/clé depuis SQLite (supporte wildcard '*')
const _stmtCache = new Map();
function getStmt(table, wildcard) {
  const key = table + (wildcard ? '*' : '=');
  if (!_stmtCache.has(key)) {
    const sql = wildcard
      ? `SELECT ids FROM "${table}" WHERE key LIKE ? ESCAPE '\\'`
      : `SELECT ids FROM "${table}" WHERE key=?`;
    _stmtCache.set(key, W.db.prepare(sql));
  }
  return _stmtCache.get(key);
}

function sqliteIds(table, rawKey) {
  if (!rawKey) return null;
  if (rawKey.endsWith('*')) {
    const prefix = rawKey.slice(0, -1).replace(/[%_\\]/g, c => '\\' + c);
    const rows = getStmt(table, true).all(prefix + '%');
    if (!rows.length) return new Uint32Array(0);
    return mergeBlobs(rows.map(r => r.ids));
  }
  const row = getStmt(table, false).get(rawKey);
  if (!row) return new Uint32Array(0);
  return new Uint32Array(row.ids.buffer, row.ids.byteOffset, row.ids.byteLength >> 2);
}

// narrow : intersecte le résultat courant avec un nouveau jeu d'IDs
// Retourne false si résultat vide (permet de court-circuiter)
function narrow(result, ids) {
  if (ids === null) return result;           // champ non renseigné → pas de filtre
  if (result === null) return ids;           // premier filtre
  const r = intersect(result, ids);
  return r;
}

function sqliteSearch(q) {
  let result = null;

  const n = (table, key) => {
    if (!key) return true;
    const ids = sqliteIds(table, key);
    result = narrow(result, ids);
    return result === null || result.length > 0;
  };

  if (!n('lastName',  q.qLN    ? NnF(q.qLN)                              : null)) return [];
  if (!n('firstName', q.qFN    ? NnF(q.qFN)                              : null)) return [];
  if (q.qBF) {
    if (!n('birthFull', ND(q.qBF))) return [];
  } else {
    if (!n('birthYear',  q.qYear  ? q.qYear.slice(0, 4)                  : null)) return [];
    if (!n('birthMonth', q.qMonth ? q.qMonth                             : null)) return [];
    if (!n('birthDay',   q.qDay   ? q.qDay                               : null)) return [];
  }
  if (!n('street',    q.qStreet  ? NnF(q.qStreet)                        : null)) return [];
  if (!n('postal',    q.qPostal  ? q.qPostal.trim()                      : null)) return [];
  if (!n('city',      q.qCity    ? NnF(q.qCity)                          : null)) return [];
  if (!n('country',   q.qCountry ? NnF(q.qCountry)                       : null)) return [];
  if (!n('nationalId',q.qNID     ? q.qNID.replace(/[\s\-.]/g,'')         : null)) return [];
  if (!n('email',     q.qEmail   ? NnF(q.qEmail)                         : null)) return [];
  if (!n('phone',     q.qPhone   ? q.qPhone.replace(/\D/g,'')            : null)) return [];
  if (!n('iban',      q.qIban    ? q.qIban.replace(/\s/g,'').toLowerCase(): null)) return [];
  if (!n('ip',        q.qIp      ? q.qIp.trim()                          : null)) return [];

  if (result === null) return []; // aucun champ renseigné
  return Array.from(result);
}

// Lecture groupée de N lignes depuis le .bin — 1 open/close par fichier
async function rowReadBatch(ids) {
  const byFile = new Map();
  for (const id of ids) {
    if (id >= W.binRowCount) continue;
    const fi = W.rowBuf[id];
    if (!byFile.has(fi)) byFile.set(fi, []);
    byFile.get(fi).push(id);
  }
  const result = new Map();
  for (const [fi, fileIds] of byFile) {
    const fh    = await fs.promises.open(W.files[fi].path, 'r');
    const chunk = Buffer.allocUnsafe(4096);
    try {
      for (const id of fileIds) {
        await fh.read(chunk, 0, 4096, W.rowPos[id]);
        let p = 0;
        const rs = () => { const l=chunk.readUInt16LE(p); p+=2; const s=chunk.toString('utf8',p,p+l); p+=l; return s; };
        const ln=rs(),fn=rs(),bd=rs(),st=rs(),sn=rs(),pc=rs(),cy=rs(),co=rs(),ni=rs(),em=rs(),ph=rs(),ib=rs(),ip=rs();
        result.set(id, [W.rowDbi[id], ln,fn,bd,st,sn,pc,cy,co,ni,em,ph,ib,ip]);
      }
    } finally { await fh.close(); }
  }
  return result;
}

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));
// CORS — autorise le frontend-server à appeler l'API depuis un autre port/domaine
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin',  '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});
app.use((req, res, next) => {
  if (/\.(bin|csv)$/i.test(req.path)) return res.status(403).end();
  next();
});
app.use(express.static(DIR));
app.get('/', (req, res) => res.sendFile(path.join(DIR, 'hshub_v5.html')));

// Status
app.get('/api/status', (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    loaded: W.loaded, loading: W.loading, error: W.loadError,
    totalActive: W.totalActive,
    dbs: W.dbs.map(d => ({ id: d.id, name: d.name, count: d.count })),
    memory: { heapUsed: mem.heapUsed, heapTotal: mem.heapTotal, rss: mem.rss }
  });
});

// Recherche — SQLite (rapide) ou scan (fallback)
app.post('/api/search', async (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const { query: q = {}, ts } = req.body;
  const t0 = Date.now();
  try {
    let allIds;
    let mode;

    if (W.db) {
      // ── Mode SQLite : index inversé, pas de lecture fichier ──────────
      allIds = sqliteSearch(q);
      mode   = 'sqlite';
    } else {
      // ── Mode scan : charge .bin temporairement, scanne, libère ───────
      const bufs = await loadBufs();
      allIds = await scanSearch(q, bufs);
      mode   = 'scan';
    }

    // srcStats via W.rowDbi (déjà en mémoire — pas de lecture fichier)
    const srcStats = {};
    for (const id of allIds) {
      const dbi = id < W.binRowCount ? W.rowDbi[id] : (W.prev[id - W.binRowCount]?.[F.dbi] ?? -1);
      const db  = W.dbs[dbi];
      if (db) srcStats[db.name] = (srcStats[db.name] || 0) + 1;
    }

    // Top 200 previews — 1 open/close par fichier .bin (groupé)
    const top200  = allIds.slice(0, 200);
    const binPart = top200.filter(id => id < W.binRowCount);
    const rowMap  = await rowReadBatch(binPart);
    const rows    = top200.map(id => {
      let p;
      if (id < W.binRowCount) p = rowMap.get(id);
      else p = W.prev[id - W.binRowCount] || null;
      if (!p) return null;
      const db = W.dbs[p[F.dbi]];
      return { id, dbName: db ? db.name : '', ln: p[F.ln], fn: p[F.fn], bd: p[F.bd], pc: p[F.pc], cy: p[F.cy] };
    }).filter(Boolean);

    const elapsed = Date.now() - t0;
    console.log(`[search:${mode}] ${elapsed}ms → ${allIds.length} résultats`);
    res.json({ total: allIds.length, allIds, rows, ts: ts || Date.now(), srcStats, serverMs: elapsed });
  } catch(e) {
    console.error('[search] erreur:', e);
    res.status(500).json({ error: e.message });
  }
});

// Détail — lecture partielle du fichier (une seule ligne, 4KB)
app.get('/api/detail/:id', async (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const id = parseInt(req.params.id, 10);
  if (isNaN(id) || id < 0) return res.status(400).json({ error: 'ID invalide' });
  try {
    let p;
    if (id < W.binRowCount) {
      p = await rowReadBinSingle(id);
    } else {
      p = W.prev[id - W.binRowCount] || null;
    }
    if (!p) return res.status(404).json({ error: 'Introuvable' });
    const db  = W.dbs[p[F.dbi]];
    const all = {};
    const add = (k,v) => { if (v && v !== '0') all[k] = v; };
    add('Nom', p[F.ln]); add('Prénom', p[F.fn]); add('Date naissance', p[F.bd]);
    add('Rue', p[F.st]); add('Numéro', p[F.sn]); add('Code postal', p[F.pc]);
    add('Ville', p[F.cy]); add('Pays', p[F.co]); add('ID National', p[F.ni]);
    add('Email', p[F.em]); add('Téléphone', p[F.ph]); add('IBAN', p[F.ib]); add('IP', p[F.ip]);
    const row = {
      ln: p[F.ln], fn: p[F.fn], bd: p[F.bd],
      st: [p[F.st], p[F.sn]].filter(Boolean).join(' '),
      pc: p[F.pc], cy: p[F.cy], co: p[F.co], ni: p[F.ni],
      em: p[F.em], ph: p[F.ph], ib: p[F.ib], ip: p[F.ip],
      _all: all
    };
    res.json({ id, dbName: db ? db.name : '', row });
  } catch(e) {
    console.error('[detail] erreur:', e);
    res.status(500).json({ error: e.message });
  }
});

// Batch preview by IDs
app.post('/api/search_by_ids', async (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const { ids = [] } = req.body;
  try {
    const top     = ids.slice(0, 200);
    const binPart = top.filter(id => id < W.binRowCount);
    const rowMap  = await rowReadBatch(binPart);
    const rows = top.map(id => {
      let p;
      if (id < W.binRowCount) p = rowMap.get(id);
      else p = W.prev[id - W.binRowCount] || null;
      if (!p) return null;
      const db = W.dbs[p[F.dbi]];
      return { id, dbName: db ? db.name : '', ln: p[F.ln], fn: p[F.fn], bd: p[F.bd], pc: p[F.pc], cy: p[F.cy] };
    }).filter(Boolean);
    res.json({ rows });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Export CSV/JSON
app.post('/api/export', async (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const { allIds = [] } = req.body;
  try {
    const bufs = await loadBufs();
    const rows = allIds.slice(0, 100_000).map(id => {
      const p = rowGet(id, bufs); if (!p) return null;
      const db = W.dbs[p[F.dbi]];
      return {
        source: db ? db.name : '', nom: p[F.ln], prenom: p[F.fn], dateNaissance: p[F.bd],
        adresse: [p[F.st], p[F.sn]].filter(Boolean).join(' '),
        codePostal: p[F.pc], ville: p[F.cy], pays: p[F.co],
        idNational: p[F.ni], email: p[F.em], telephone: p[F.ph], iban: p[F.ib], ip: p[F.ip]
      };
    }).filter(Boolean);
    res.json({ rows });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Famille — scan séquentiel avec buffers temporaires
app.get('/api/fam/:id', async (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const id = parseInt(req.params.id, 10);
  try {
    let refP;
    if (id < W.binRowCount) {
      refP = await rowReadBinSingle(id);
    } else {
      refP = W.prev[id - W.binRowCount] || null;
    }
    if (!refP) return res.json({ id, results: [] });

    const ln = NnF(refP[F.ln]), pc = refP[F.pc], st = NnF(refP[F.st]);
    const sc    = new Map();
    const total = W.binRowCount + W.prev.length;
    const YIELD = 5_000;
    const bufs  = await loadBufs();

    for (let rid = 0; rid < total; rid++) {
      if (rid > 0 && rid % YIELD === 0) await tick();
      if (rid === id || W.deleted.has(rid)) continue;
      const r = rowGet(rid, bufs); if (!r) continue;
      let pts = 0; const rs = new Set();
      if (ln && NnF(r[F.ln]) === ln) { pts += 3; rs.add('Même nom'); }
      if (pc && r[F.pc] === pc)      { pts += 2; rs.add('Même CP'); }
      if (st && NnF(r[F.st]) === st) { pts += 4; rs.add('Même adresse'); }
      if (pts >= 3) {
        const e = sc.get(rid) || { pts: 0, rs: new Set() };
        e.pts += pts; rs.forEach(x => e.rs.add(x)); sc.set(rid, e);
      }
    }
    // bufs libérés ici

    const seen = new Set(), results = [];
    for (const [rid, { pts, rs }] of [...sc].sort((a,b) => b[1].pts - a[1].pts)) {
      if (results.length >= 25) break;
      let fp;
      if (rid < W.binRowCount) { fp = await rowReadBinSingle(rid); }
      else { fp = W.prev[rid - W.binRowCount] || null; }
      if (!fp) continue;
      const k = fp[F.ln]+'|'+fp[F.fn]+'|'+fp[F.bd];
      if (!seen.has(k)) {
        seen.add(k);
        results.push({ rid, rs: [...rs], ln: fp[F.ln], fn: fp[F.fn], bd: fp[F.bd], pc: fp[F.pc], cy: fp[F.cy] });
      }
    }
    res.json({ id, results });
  } catch(e) {
    console.error('[fam] erreur:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── CSV loader ────────────────────────────────────────────────────────────────
const PAT_CSV = {
  lastName:  /\b(naam|^nom$|lastname|surname|familyname|lname)\b/i,
  firstName: /\b(voornamen|prenom|firstname|givenname|fname)\b/i,
  birthDate: /\b(geboortedatum|birth_?date|date_?naiss|dob|birthdate)\b/i,
  email:     /email|e[-_]?mail/i,
  phone:     /\b(tel(eph)?|phone|gsm|mobile|cel)\b/i,
  iban:      /\biban\b/i,
  ip:        /^ip(_addr(ess)?)?$/i,
  street:    /\b(straat|rue|street|adresse)\b/i,
  streetNum: /\b(nummer|numero|housenr|streetnr)\b/i,
  postal:    /\b(post_?code|zip|plz|cp|code_?postal)\b/i,
  city:      /\b(plaats|ville|city|stad|gemeente)\b/i,
  country:   /^(land|pays|country)$/i,
  nationalId:/rijksregister|ssn|niss|national_?id|id_?nat/i
};

function detectCsv(headers) {
  const m = {};
  for (const [f, p] of Object.entries(PAT_CSV))
    for (const h of headers) if (p.test(h)) { m[f] = h; break; }
  return m;
}

function loadCsvFile(filePath) {
  const rl    = require('readline');
  const label = path.basename(filePath);
  const size  = fs.statSync(filePath).size;
  console.log(`\n[•] ${label}  (CSV · ${fmtB(size)})`);

  const dbiIdx  = W.dbs.length;
  const dbEntry = { id: `csv_${dbiIdx}`, name: label, count: 0 };
  W.dbs.push(dbEntry);

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    const iface  = rl.createInterface({ input: stream, crlfDelay: Infinity });
    let headers = null, mapping = null, delim = ',', rowCount = 0;
    const t0 = Date.now();

    iface.on('line', raw => {
      const line = raw.trim();
      if (!line) return;
      if (!headers) {
        const cnt = { ',':0, ';':0, '|':0, '\t':0 };
        for (const c of line) if (c in cnt) cnt[c]++;
        delim   = Object.entries(cnt).sort((a,b) => b[1]-a[1])[0][0];
        headers = line.split(delim).map(h => h.trim().replace(/^["']|["']$/g,''));
        mapping = detectCsv(headers);
        return;
      }
      const vals = line.split(delim).map(v => v.trim().replace(/^["']|["']$/g,''));
      const row  = {};
      headers.forEach((h, i) => { row[h] = vals[i] || ''; });
      const g  = key => mapping[key] ? String(row[mapping[key]] || '') : '';
      W.prev.push([
        dbiIdx,
        NnF(g('lastName')), NnF(g('firstName')), ND(g('birthDate')),
        NnF(g('street')),   NnF(g('streetNum')),
        String(row[mapping.postal]     ||'').trim(),
        NnF(g('city')),     NnF(g('country')),
        String(row[mapping.nationalId] ||'').replace(/[\s\-.]/g,''),
        NnF(g('email')),
        String(row[mapping.phone]      ||'').replace(/\D/g,''),
        String(row[mapping.iban]       ||'').replace(/\s/g,'').toLowerCase(),
        String(row[mapping.ip]         ||'').trim()
      ]);
      dbEntry.count++;
      W.totalActive++;
      rowCount++;
      if (rowCount % 500000 === 0) {
        const spd = Math.round(rowCount / ((Date.now()-t0) / 1000));
        process.stdout.write(`\r    ${rowCount.toLocaleString()} lignes — ${(spd/1000).toFixed(0)}k/s   `);
      }
    });

    iface.on('close', () => {
      process.stdout.write('\n');
      const elapsed = ((Date.now()-t0)/1000).toFixed(1);
      const mem     = Math.round(process.memoryUsage().heapUsed / 1e6);
      console.log(`    ✓  ${rowCount.toLocaleString()} lignes en ${elapsed}s  —  heap: ${mem} MB`);
      resolve();
    });
    iface.on('error', reject);
    stream.on('error', reject);
  });
}

// ── Démarrage ─────────────────────────────────────────────────────────────────
async function main() {
  await new Promise(resolve => {
    app.listen(PORT, () => {
      console.log(`\n[•] HsHub Server → http://localhost:${PORT}  (chargement en cours…)\n`);
      resolve();
    });
  });

  let binFiles = process.argv.slice(2)
    .filter(a => !a.startsWith('-') && /\.bin$/i.test(a) && fs.existsSync(a))
    .map(a => path.resolve(a));

  if (!binFiles.length) {
    binFiles = fs.readdirSync(DIR)
      .filter(f => /\.bin$/i.test(f))
      .map(f => path.join(DIR, f))
      .filter(f => fs.statSync(f).isFile())
      .sort();
  }

  if (binFiles.length) {
    W.loading = true;
    try {
      for (const f of binFiles) await loadBinFile(f);
      W.loaded = true;
    } catch(e) {
      W.loadError = e.message;
      console.error('\n[!] Erreur chargement .bin :', e.message);
    }
    W.loading = false;
  } else {
    const csvFiles = fs.readdirSync(DIR)
      .filter(f => /\.csv$/i.test(f))
      .map(f => path.join(DIR, f))
      .filter(f => fs.statSync(f).isFile())
      .sort();

    if (csvFiles.length) {
      console.log(`\n[CSV] ${csvFiles.length} fichier(s) trouvé(s) — indexation en cours…`);
      W.loading = true;
      try {
        for (const f of csvFiles) await loadCsvFile(f);
        W.loaded = true;
      } catch(e) {
        W.loadError = e.message;
        console.error('\n[!] Erreur CSV :', e.message);
      }
      W.loading = false;
    } else {
      console.log('\n[!] Aucun fichier .bin ou .csv trouvé dans', DIR);
      W.loadError = 'Aucun fichier .bin ou .csv trouvé';
    }
  }

  const mem = process.memoryUsage();
  const bar = '═'.repeat(46);
  console.log(`\n╔${bar}╗`);
  console.log(`║  ✓  HsHub Server  →  http://localhost:${PORT}           ║`);
  console.log(`║     ${fmtN(W.totalActive).padEnd(12)} entrées  ·  ${W.dbs.length} base(s)              ║`);
  console.log(`║     Heap: ${fmtB(mem.heapUsed).padEnd(8)}  RSS: ${fmtB(mem.rss).padEnd(10)}            ║`);
  console.log(`╚${bar}╝\n`);
  if (!W.loaded)
    console.log('  ⚠  Aucune donnée chargée.\n');
}

main().catch(e => console.error('Fatal:', e));
