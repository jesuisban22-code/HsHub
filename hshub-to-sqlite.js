#!/usr/bin/env node
/**
 * hshub-to-sqlite.js
 * Extrait l'index inversé embarqué dans index.bin et le sauvegarde dans index.sqlite
 * Usage : node hshub-to-sqlite.js [index.bin]
 */
'use strict';
const fs   = require('fs');
const path = require('path');

const binFile    = process.argv[2] || 'index.bin';
const sqliteFile = binFile.replace(/\.bin$/i, '') + '.sqlite';

if (!fs.existsSync(binFile)) {
  console.error(`❌  Fichier introuvable : ${binFile}`);
  process.exit(1);
}

let BetterSQLite;
try { BetterSQLite = require('better-sqlite3'); }
catch(e) {
  console.error('❌  better-sqlite3 non installé. Lancez : npm install');
  process.exit(1);
}

const fmtB = n => n >= 1e9 ? (n/1e9).toFixed(1)+'GB' : n >= 1e6 ? (n/1e6).toFixed(0)+'MB' : (n/1e3).toFixed(0)+'KB';
const size  = fs.statSync(binFile).size;
console.log(`\n[•] ${binFile}  (${fmtB(size)})`);

// ── Lecteur par fenêtre glissante 4MB ─────────────────────────────────────────
const CHUNK = 4 * 1024 * 1024;
const fd    = fs.openSync(binFile, 'r');
const chunk = Buffer.allocUnsafe(CHUNK);
let cBase   = 0;
let cLen    = 0;
let cPos    = 0;

// Garantit N octets lisibles depuis cPos (N doit être <= CHUNK)
function ensure(need) {
  if (cLen - cPos >= need) return;
  const rem = Math.max(0, cLen - cPos);
  if (rem > 0) chunk.copy(chunk, 0, cPos, cLen);
  cBase += cPos; cPos = 0; cLen = rem;
  const n = fs.readSync(fd, chunk, cLen, CHUNK - cLen, cBase + cLen);
  cLen += n;
}

// Lit exactement `count` octets dans un Buffer dédié (taille arbitraire)
function readBytes(count) {
  const out = Buffer.allocUnsafe(count);
  let done  = 0;
  while (done < count) {
    if (cLen - cPos === 0) {
      cBase += cPos; cPos = 0; cLen = 0;
      cLen = fs.readSync(fd, chunk, 0, CHUNK, cBase);
      if (cLen === 0) break;
    }
    const take = Math.min(count - done, cLen - cPos);
    chunk.copy(out, done, cPos, cPos + take);
    cPos += take; done += take;
  }
  return out;
}

// Saute exactement `count` octets sans les lire
function skipBytes(count) {
  let done = 0;
  while (done < count) {
    if (cLen - cPos === 0) {
      cBase += cPos; cPos = 0; cLen = 0;
      cLen = fs.readSync(fd, chunk, 0, CHUNK, cBase);
      if (cLen === 0) break;
    }
    const take = Math.min(count - done, cLen - cPos);
    cPos += take; done += take;
  }
}

function u8()  { ensure(1); return chunk[cPos++]; }
function u16() { ensure(2); const v=chunk.readUInt16LE(cPos); cPos+=2; return v; }
function u32() { ensure(4); const v=chunk.readUInt32LE(cPos); cPos+=4; return v; }
function s8()  { const l=u8();  return readBytes(l).toString('utf8'); }
function s16() { const l=u16(); return readBytes(l).toString('utf8'); }

// Chargement initial
ensure(CHUNK);

// ── Header ────────────────────────────────────────────────────────────────────
const magic = chunk.toString('ascii', 0, 4); cPos = 4;
if (magic !== 'HSHB') { console.error(`❌  Format invalide (magic="${magic}")`); process.exit(1); }

u16();                   // version
const nb_dbs  = u32();
const nb_rows = u32();
const nb_idx  = u32();

console.log(`    → ${nb_rows.toLocaleString()} lignes, ${nb_dbs} base(s), ${nb_idx} champ(s) indexé(s)`);

if (nb_idx === 0) {
  console.log(`\n⚠️  Ce fichier .bin ne contient pas d'index embarqué (nb_idx=0).`);
  console.log(`   Régénérez index.bin + index.sqlite avec :`);
  console.log(`   node hshub-index.js *.csv -o index.bin\n`);
  fs.closeSync(fd);
  process.exit(0);
}

// ── DBs ───────────────────────────────────────────────────────────────────────
for (let i = 0; i < nb_dbs; i++) { s16(); u32(); }

// ── Rows — skip pour atteindre la section index ───────────────────────────────
process.stdout.write(`    Parcours des lignes…`);
const t0 = Date.now();
for (let i = 0; i < nb_rows; i++) {
  ensure(4);
  cPos += 2; // dbi u16
  for (let f = 0; f < 13; f++) {
    ensure(2);
    const l = chunk.readUInt16LE(cPos); cPos += 2;
    skipBytes(l);
  }
  if ((i+1) % 500_000 === 0) process.stdout.write('.');
}
console.log(` ✓  (${((Date.now()-t0)/1000).toFixed(1)}s)`);

// ── Index inversé → SQLite ────────────────────────────────────────────────────
if (fs.existsSync(sqliteFile)) fs.unlinkSync(sqliteFile);
const DB = BetterSQLite(sqliteFile);
DB.pragma('journal_mode=WAL');
DB.pragma('synchronous=OFF');
DB.pragma('page_size=8192');
DB.pragma('cache_size=-131072'); // 128MB cache SQLite

DB.exec(`CREATE TABLE _meta(k TEXT PRIMARY KEY, v TEXT)`);
DB.prepare(`INSERT OR REPLACE INTO _meta VALUES('totalRows',?)`).run(String(nb_rows));

process.stdout.write(`    Écriture SQLite`);
const t1    = Date.now();
let totalKeys = 0;
const BATCH   = 2000; // inserts par transaction

for (let i = 0; i < nb_idx; i++) {
  const name = s8();
  const nk   = u32();

  // Créer la table pour ce champ
  DB.exec(`CREATE TABLE IF NOT EXISTS "${name}"(key TEXT PRIMARY KEY, ids BLOB) WITHOUT ROWID`);
  const stmt = DB.prepare(
    `INSERT INTO "${name}"(key,ids) VALUES(?,?)` +
    ` ON CONFLICT(key) DO UPDATE SET ids=ids||excluded.ids`
  );
  const tx = DB.transaction(entries => { for (const [k,v] of entries) stmt.run(k,v); });

  let entries = [];
  for (let k = 0; k < nk; k++) {
    const key = s16();
    const ni  = u32();
    const ids = readBytes(ni * 4); // lecture directe, taille arbitraire
    entries.push([key, ids]);
    totalKeys++;
    if (entries.length >= BATCH) { tx(entries); entries = []; }
  }
  if (entries.length) tx(entries);
  process.stdout.write('.');
}

DB.close();
fs.closeSync(fd);

const sqliteSize = fs.statSync(sqliteFile).size;
const elapsed    = ((Date.now()-t1)/1000).toFixed(1);
console.log(` ✓  (${elapsed}s)`);
console.log(`\n✅  ${sqliteFile}  —  ${fmtB(sqliteSize)}  (${totalKeys.toLocaleString()} clés)`);
console.log(`\nRelancez le serveur : node hshub-server.js\n`);
