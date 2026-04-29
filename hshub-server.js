'use strict';
// ============================================================
//  HsHub Server  —  node --max-old-space-size=8192 hshub-server.js
//  Sert le frontend + API REST pour des milliards de lignes
//  Support multi-fichiers .bin (fusion automatique des index)
// ============================================================
const express = require('express');
const fs      = require('fs');
const path    = require('path');

const DIR  = process.cwd();
const PORT = (() => {
  const i = process.argv.indexOf('--port');
  return (i >= 0 ? parseInt(process.argv[i+1], 10) : 0) || 7331;
})();

// ── State ─────────────────────────────────────────────────────────────────────
const W = {
  dbs:  [],
  prev: [],   // [dbi, ln, fn, bd, st, sn, pc, cy, co, ni, em, ph, ib, ip]
  idx: {
    lastName:  new Map(), firstName: new Map(),
    birthFull: new Map(), birthYear: new Map(), birthMonth: new Map(), birthDay: new Map(),
    email:     new Map(), phone:     new Map(), iban:      new Map(), ip:       new Map(),
    postal:    new Map(), city:      new Map(), country:   new Map(),
    nationalId:new Map(), street:    new Map()
  },
  keys:        {},
  deleted:     new Set(),
  totalActive: 0,
  loaded:      false,
  loading:     false,
  loadError:   null
};

const F = { dbi:0, ln:1, fn:2, bd:3, st:4, sn:5, pc:6, cy:7, co:8, ni:9, em:10, ph:11, ib:12, ip:13 };

// ── Helpers ───────────────────────────────────────────────────────────────────
const Nn   = s => !s ? '' : String(s).toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g,'').replace(/\s+/g,' ').trim();
const ND   = s => !s ? '' : String(s).replace(/[-\/.]/g,'');
const fmtB = n => n >= 1e9 ? (n/1e9).toFixed(1)+'GB' : n >= 1e6 ? (n/1e6).toFixed(0)+'MB' : (n/1e3).toFixed(0)+'KB';
const fmtN = n => n >= 1e9 ? (n/1e9).toFixed(2)+'B'  : n >= 1e6 ? (n/1e6).toFixed(1)+'M'  : n >= 1e3 ? (n/1e3).toFixed(0)+'K' : String(n);

const tick = () => new Promise(r => setImmediate(r));

async function buildKeys() {
  for (const [name, map] of Object.entries(W.idx)) {
    W.keys[name] = Array.from(map.keys()).sort();
    await tick(); // libère l'event loop entre chaque champ
  }
}

// ── Binary loader async (yield régulier pour ne pas bloquer l'event loop) ────
async function loadBinFile(filePath) {
  const label  = path.basename(filePath);
  const size   = fs.statSync(filePath).size;
  console.log(`\n[•] ${label} (${fmtB(size)})`);

  if (size > 32 * 1024 * 1024 * 1024)
    console.warn(`    ⚠  Fichier > 32GB — assurez-vous d'avoir assez de RAM`);

  const buf = fs.readFileSync(filePath);
  let pos = 0;

  const readU8    = ()     => buf[pos++];
  const readU16   = ()     => { const v = buf.readUInt16LE(pos); pos += 2; return v; };
  const readU32   = ()     => { const v = buf.readUInt32LE(pos); pos += 4; return v; };
  const readStr16 = ()     => { const l = readU16(); const s = buf.toString('utf8', pos, pos+l); pos += l; return s; };
  const readStr8  = ()     => { const l = readU8();  const s = buf.toString('utf8', pos, pos+l); pos += l; return s; };

  // — Magic & header —
  const magic = buf.toString('ascii', 0, 4); pos = 4;
  if (magic !== 'HSHB') throw new Error(`${label}: fichier invalide (magic="${magic}")`);

  readU16(); // version
  const nb_dbs  = readU32();
  const nb_rows = readU32();
  const nb_idx  = readU32();
  console.log(`    → ${nb_rows.toLocaleString()} lignes, ${nb_dbs} base(s), ${nb_idx} champs indexés`);

  const dbiBase = W.dbs.length;
  const rowBase = W.prev.length;

  // — DBs —
  for (let i = 0; i < nb_dbs; i++) {
    const name  = readStr16();
    const count = readU32();
    W.dbs.push({ id: `preloaded_${dbiBase+i}`, name, count });
  }

  // — Rows — yield tous les 100k pour laisser passer les requêtes HTTP
  const t0    = Date.now();
  const YIELD = 100_000;
  for (let i = 0; i < nb_rows; i++) {
    const dbi = readU16() + dbiBase;
    const ln  = readStr16(), fn = readStr16(), bd = readStr16();
    const st  = readStr16(), sn = readStr16(), pc = readStr16(), cy = readStr16(), co = readStr16();
    const ni  = readStr16(), em = readStr16(), ph = readStr16(), ib = readStr16(), ip = readStr16();
    W.prev.push([dbi, ln, fn, bd, st, sn, pc, cy, co, ni, em, ph, ib, ip]);
    W.totalActive++;
    if ((i+1) % YIELD === 0) {
      const spd = Math.round((i+1) / ((Date.now()-t0) / 1000));
      process.stdout.write(`\r    Lignes: ${(i+1).toLocaleString()}/${nb_rows.toLocaleString()} — ${(spd/1000).toFixed(0)}k/s   `);
      await tick(); // yield → les requêtes /api/status peuvent passer ici
    }
  }
  process.stdout.write('\n');

  // — Index — yield entre chaque champ indexé
  for (let i = 0; i < nb_idx; i++) {
    const name    = readStr8();
    const nb_keys = readU32();
    if (!W.idx[name]) W.idx[name] = new Map();
    const m = W.idx[name];
    for (let k = 0; k < nb_keys; k++) {
      const key    = readStr16();
      const nb_ids = readU32();
      if (!m.has(key)) m.set(key, []);
      const arr = m.get(key);
      for (let j = 0; j < nb_ids; j++) arr.push(readU32() + rowBase);
    }
    await tick(); // yield entre chaque champ
  }

  const elapsed = ((Date.now()-t0)/1000).toFixed(1);
  const mem     = Math.round(process.memoryUsage().heapUsed / 1e6);
  console.log(`    ✓  Chargé en ${elapsed}s  —  heap: ${mem} MB`);
}

// ── Search ────────────────────────────────────────────────────────────────────
function ps(idxN, q) {
  if (!q) return null;
  const isWild = q.endsWith('*');
  const prefix = isWild ? q.slice(0,-1) : q;
  const keys   = W.keys[idxN] || [];
  const map    = W.idx[idxN];
  const res    = new Set();
  let lo = 0, hi = keys.length-1;
  while (lo <= hi) { const mid = (lo+hi)>>1; if (keys[mid] < prefix) lo = mid+1; else hi = mid-1; }
  let i = lo;
  while (i < keys.length && keys[i].startsWith(prefix)) {
    const ids = map.get(keys[i]); if (ids) ids.forEach(x => res.add(x)); i++;
  }
  return res;
}

function intersect(sets) {
  const active = sets.filter(s => s !== null);
  if (!active.length) return [];
  active.sort((a,b) => a.size - b.size);
  let r = active[0];
  for (let i = 1; i < active.length; i++) {
    const n = new Set();
    for (const x of r) if (active[i].has(x)) n.add(x);
    r = n;
    if (!r.size) break;
  }
  return [...r].filter(id => !W.deleted.has(id));
}

function rowPreview(id) {
  const p = W.prev[id]; if (!p) return null;
  const db = W.dbs[p[F.dbi]]; if (!db) return null;
  return { id, dbName: db.name, ln: p[F.ln], fn: p[F.fn], bd: p[F.bd], pc: p[F.pc], cy: p[F.cy] };
}

function rowDetail(id) {
  const p = W.prev[id]; if (!p) return null;
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

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));
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

// Recherche
app.post('/api/search', (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const { query: q = {}, ts } = req.body;
  const t0 = Date.now();

  const sets = [];
  if (q.qLN)      sets.push(ps('lastName',   Nn(q.qLN)));
  if (q.qFN)      sets.push(ps('firstName',  Nn(q.qFN)));
  if (q.qNID)     sets.push(ps('nationalId', q.qNID.replace(/[\s\-.]/g,'')));
  if (q.qEmail)   sets.push(ps('email',      Nn(q.qEmail)));
  if (q.qPhone)   sets.push(ps('phone',      q.qPhone.replace(/\D/g,'')));
  if (q.qIban)    sets.push(ps('iban',       q.qIban.replace(/\s/g,'').toLowerCase()));
  if (q.qIp)      sets.push(ps('ip',         q.qIp.trim()));
  if (q.qStreet)  sets.push(ps('street',     Nn(q.qStreet)));
  if (q.qPostal)  sets.push(ps('postal',     q.qPostal.trim()));
  if (q.qCity)    sets.push(ps('city',       Nn(q.qCity)));
  if (q.qCountry) sets.push(ps('country',   Nn(q.qCountry)));
  if (q.qBF)      sets.push(ps('birthFull',  ND(q.qBF)));
  else {
    if (q.qYear)  sets.push(ps('birthYear',  q.qYear.slice(0,4)));
    if (q.qMonth) sets.push(ps('birthMonth', q.qMonth));
    if (q.qDay)   sets.push(ps('birthDay',   q.qDay));
  }

  const ids = intersect(sets);
  const seen = new Set(), uniq = [];
  for (const id of ids) {
    const p = W.prev[id]; if (!p) continue;
    const k = p[F.ln]+'|'+p[F.fn]+'|'+p[F.bd]+'|'+p[F.pc];
    if (!seen.has(k)) { seen.add(k); uniq.push(id); }
  }
  const srcStats = {};
  uniq.forEach(id => {
    const p = W.prev[id]; if (!p) return;
    const db = W.dbs[p[F.dbi]]; if (!db) return;
    srcStats[db.name] = (srcStats[db.name] || 0) + 1;
  });

  const rows    = uniq.slice(0, 200).map(rowPreview).filter(Boolean);
  const elapsed = Date.now() - t0;
  console.log(`[search] ${elapsed}ms → ${uniq.length} résultats (${fmtN(W.totalActive)} total)`);
  res.json({ total: uniq.length, allIds: uniq, rows, ts: ts || Date.now(), srcStats, serverMs: elapsed });
});

// Détail
app.get('/api/detail/:id', (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const id  = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).json({ error: 'ID invalide' });
  const row = rowDetail(id);
  if (!row) return res.status(404).json({ error: 'Introuvable' });
  const p  = W.prev[id];
  const db = W.dbs[p[F.dbi]];
  res.json({ id, dbName: db ? db.name : '', row });
});

// Batch row preview by IDs (for "load more" in server mode)
app.post('/api/search_by_ids', (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const { ids = [] } = req.body;
  const rows = ids.slice(0, 200).map(id => rowPreview(id)).filter(Boolean);
  res.json({ rows });
});

// Export CSV/JSON
app.post('/api/export', (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const { allIds = [] } = req.body;
  const rows = allIds.slice(0, 100_000).map(id => {
    const p = W.prev[id]; if (!p) return null;
    const db = W.dbs[p[F.dbi]];
    return {
      source: db ? db.name : '', nom: p[F.ln], prenom: p[F.fn], dateNaissance: p[F.bd],
      adresse: [p[F.st], p[F.sn]].filter(Boolean).join(' '),
      codePostal: p[F.pc], ville: p[F.cy], pays: p[F.co],
      idNational: p[F.ni], email: p[F.em], telephone: p[F.ph], iban: p[F.ib], ip: p[F.ip]
    };
  }).filter(Boolean);
  res.json({ rows });
});

// Famille
app.get('/api/fam/:id', (req, res) => {
  if (!W.loaded) return res.status(503).json({ error: 'Index non chargé' });
  const id = parseInt(req.params.id, 10);
  const p  = W.prev[id]; if (!p) return res.json({ id, results: [] });
  const ln = Nn(p[F.ln]), pc = p[F.pc], st = Nn(p[F.st]);
  const sc = new Map();
  const add = (rid, pts, reason) => {
    if (rid === id || W.deleted.has(rid)) return;
    const e = sc.get(rid) || { pts: 0, rs: new Set() };
    e.pts += pts; e.rs.add(reason); sc.set(rid, e);
  };
  if (ln) ps('lastName', ln)?.forEach(x => add(x, 3, 'Même nom'));
  if (pc) { const a = W.idx.postal.get(pc); if (a) a.forEach(x => add(x, 2, 'Même CP')); }
  if (st) { const a = W.idx.street.get(st); if (a) a.forEach(x => add(x, 4, 'Même adresse')); }
  const seen = new Set(), results = [];
  for (const [rid, { pts, rs }] of [...sc].sort((a,b) => b[1].pts - a[1].pts)) {
    if (pts < 3) continue;
    const fp  = W.prev[rid]; if (!fp) continue;
    const fdb = W.dbs[fp[F.dbi]]; if (!fdb) continue;
    const k   = fp[F.ln]+'|'+fp[F.fn]+'|'+fp[F.bd];
    if (!seen.has(k)) {
      seen.add(k);
      results.push({ rid, rs: [...rs], ln: fp[F.ln], fn: fp[F.fn], bd: fp[F.bd], pc: fp[F.pc], cy: fp[F.cy] });
    }
  }
  res.json({ id, results: results.slice(0, 25) });
});

// ── CSV loader (utilisé quand aucun .bin n'est disponible) ───────────────────
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

function addIdx(name, key, id) {
  if (!key) return;
  const m = W.idx[name];
  if (!m.has(key)) m.set(key, []);
  m.get(key).push(id);
}

function indexCsvRow(id, row, mp) {
  const g  = key => mp[key] ? String(row[mp[key]] || '') : '';
  const ln = Nn(g('lastName')),  fn = Nn(g('firstName'));
  if (ln) addIdx('lastName',  ln,  id);
  if (fn) addIdx('firstName', fn,  id);
  const bd = ND(g('birthDate'));
  if (bd) {
    if (bd.length >= 8) {
      addIdx('birthYear', bd.slice(0,4), id);
      addIdx('birthMonth',bd.slice(4,6), id);
      addIdx('birthDay',  bd.slice(6,8), id);
    } else {
      addIdx('birthFull', bd, id);
      if (bd.length >= 4) addIdx('birthYear',  bd.slice(0,4), id);
      if (bd.length >= 6) addIdx('birthMonth', bd.slice(4,6), id);
    }
  }
  const em  = Nn(g('email'));
  if (em) addIdx('email', em, id);
  const ph  = String(row[mp.phone]     ||'').replace(/\D/g,'');
  if (ph && ph !== '0') addIdx('phone', ph, id);
  const ib  = String(row[mp.iban]      ||'').replace(/\s/g,'').toLowerCase();
  if (ib) addIdx('iban', ib, id);
  const ip  = String(row[mp.ip]        ||'').trim();
  if (ip) addIdx('ip', ip, id);
  const pc  = String(row[mp.postal]    ||'').trim();
  if (pc) addIdx('postal', pc, id);
  const cy  = Nn(g('city'));    if (cy) addIdx('city',      cy,  id);
  const co  = Nn(g('country')); if (co) addIdx('country',   co,  id);
  const nid = String(row[mp.nationalId]||'').replace(/[\s\-.]/g,'');
  if (nid && nid !== '0') addIdx('nationalId', nid, id);
  const st  = Nn(g('street')); if (st) addIdx('street', st, id);
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
      const id = W.prev.length;
      const g  = key => mapping[key] ? String(row[mapping[key]] || '') : '';
      W.prev.push([
        dbiIdx,
        Nn(g('lastName')), Nn(g('firstName')), ND(g('birthDate')),
        Nn(g('street')),   Nn(g('streetNum')),
        String(row[mapping.postal]     ||'').trim(),
        Nn(g('city')),     Nn(g('country')),
        String(row[mapping.nationalId] ||'').replace(/[\s\-.]/g,''),
        Nn(g('email')),
        String(row[mapping.phone]      ||'').replace(/\D/g,''),
        String(row[mapping.iban]       ||'').replace(/\s/g,'').toLowerCase(),
        String(row[mapping.ip]         ||'').trim()
      ]);
      indexCsvRow(id, row, mapping);
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
  // Start HTTP server immediately so /api/status can report loading=true
  await new Promise(resolve => {
    app.listen(PORT, () => {
      console.log(`\n[•] HsHub Server → http://localhost:${PORT}  (chargement en cours…)\n`);
      resolve();
    });
  });

  // 1. Cherche les .bin
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
      await buildKeys();
      W.loaded = true;
    } catch(e) {
      W.loadError = e.message;
      console.error('\n[!] Erreur chargement .bin :', e.message);
    }
    W.loading = false;
  } else {
    // 2. Pas de .bin — charger les CSV directement
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
        await buildKeys();
        W.loaded = true;
      } catch(e) {
        W.loadError = e.message;
        console.error('\n[!] Erreur CSV :', e.message);
      }
      W.loading = false;
    } else {
      console.log('\n[!] Aucun fichier .bin ou .csv trouvé dans', DIR);
      console.log('    Créez un index : node hshub-index.js *.csv -o index.bin\n');
      W.loadError = 'Aucun fichier .bin ou .csv trouvé dans le dossier du serveur';
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
