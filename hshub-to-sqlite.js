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
const CHUNK   = 4 * 1024 * 1024;
const fd      = fs.openSync(binFile, 'r');
const chunk   = Buffer.allocUnsafe(CHUNK);
let   cBase   = 0;   // offset fichier de chunk[0]
let   cLen    = 0;   // octets valides
let   cPos    = 0;   // curseur

function ensure(need) {
  if (cLen - cPos >= need) return;
  const rem = cLen - cPos;
  if (rem > 0) chunk.copy(chunk, 0, cPos, cLen);
  cBase += cPos; cPos = 0; cLen = rem;
  const n = fs.readSync(fd, chunk, cLen, CHUNK - cLen, cBase + cLen);
  cLen += n;
}

function u8()  { ensure(1); return chunk[cPos++]; }
function u16() { ensure(2); const v=chunk.readUInt16LE(cPos); cPos+=2; return v; }
function u32() { ensure(4); const v=chunk.readUInt32LE(cPos); cPos+=4; return v; }
function s8()  { const l=u8();  ensure(l); const s=chunk.toString('utf8',cPos,cPos+l); cPos+=l; return s; }
function s16() { const l=u16(); ensure(l); const s=chunk.toString('utf8',cPos,cPos+l); cPos+=l; return s; }
function sk16(){ ensure(2); const l=chunk.readUInt16LE(cPos); cPos+=2+l; }

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
for (let i = 0; i < nb_dbs; i++) { s16(); u32(); } // skip name + count

// ── Rows — on skip juste pour avancer le curseur ─────────────────────────────
process.stdout.write(`    Parcours des lignes…`);
const t0 = Date.now();
for (let i = 0; i < nb_rows; i++) {
  if (cLen - cPos < 4096) ensure(4096);
  cPos += 2; // dbi
  for (let f = 0; f < 13; f++) sk16();
  if ((i+1) % 500_000 === 0) process.stdout.write('.');
}
console.log(` ✓  (${((Date.now()-t0)/1000).toFixed(1)}s)`);

// ── Index inversé → SQLite ────────────────────────────────────────────────────
if (fs.existsSync(sqliteFile)) fs.unlinkSync(sqliteFile);
const DB = BetterSQLite(sqliteFile);
DB.pragma('journal_mode=WAL');
DB.pragma('synchronous=OFF');
DB.pragma('page_size=8192');
DB.pragma('cache_size=-131072');

// Lire les noms des champs d'abord (peek)
const fieldNames = [];
const savedBase = cBase, savedPos = cPos, savedLen = cLen;
for (let i = 0; i < nb_idx; i++) {
  ensure(64);
  const nl = u8(); ensure(nl);
  const name = chunk.toString('ascii', cPos, cPos+nl); cPos += nl;
  fieldNames.push(name);
  const nk = u32();
  for (let k = 0; k < nk; k++) {
    ensure(2); const kl = chunk.readUInt16LE(cPos); cPos += 2 + kl;
    ensure(4); const ni = chunk.readUInt32LE(cPos); cPos += 4 + ni*4;
  }
}

// Revenir au début de la section index
cBase = savedBase; cPos = savedPos; cLen = savedLen;
ensure(CHUNK);

// Créer les tables
for (const name of fieldNames) {
  DB.exec(`CREATE TABLE "${name}"(key TEXT PRIMARY KEY, ids BLOB) WITHOUT ROWID`);
}
DB.exec(`CREATE TABLE _meta(k TEXT PRIMARY KEY, v TEXT)`);

// Préparer les statements
const stmts = {};
for (const name of fieldNames) {
  stmts[name] = DB.prepare(`INSERT INTO "${name}"(key,ids) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET ids=ids||excluded.ids`);
}

process.stdout.write(`    Écriture SQLite…`);
const t1 = Date.now();

const writeTx = DB.transaction((name, entries) => {
  const stmt = stmts[name];
  for (const [key, ids] of entries) stmt.run(key, ids);
});

let totalKeys = 0;
for (let i = 0; i < nb_idx; i++) {
  ensure(64);
  const nl  = u8(); ensure(nl);
  const name = chunk.toString('ascii', cPos, cPos+nl); cPos += nl;
  const nk  = u32();

  const entries = [];
  for (let k = 0; k < nk; k++) {
    ensure(2); const kl = chunk.readUInt16LE(cPos); cPos += 2;
    ensure(kl); const key = chunk.toString('utf8', cPos, cPos+kl); cPos += kl;
    ensure(4);  const ni  = chunk.readUInt32LE(cPos); cPos += 4;
    ensure(ni*4);
    const ids = Buffer.allocUnsafe(ni*4);
    chunk.copy(ids, 0, cPos, cPos+ni*4); cPos += ni*4;
    entries.push([key, ids]);
    totalKeys++;
  }
  writeTx(name, entries);
  process.stdout.write('.');
}

DB.close();
fs.closeSync(fd);

const sqliteSize = fs.statSync(sqliteFile).size;
console.log(` ✓  (${((Date.now()-t1)/1000).toFixed(1)}s)`);
console.log(`\n✅  ${sqliteFile}  —  ${fmtB(sqliteSize)}  (${totalKeys.toLocaleString()} clés)`);
console.log(`\nRelancez le serveur : node hshub-server.js\n`);
