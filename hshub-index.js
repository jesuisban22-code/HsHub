#!/usr/bin/env node
/**
 * HsHub Pre-Indexer — version streaming + chunked (sans crash mémoire)
 */
'use strict';
const fs=require('fs'),path=require('path'),readline=require('readline'),os=require('os');
const args=process.argv.slice(2);
if(!args.length||args.includes('--help')||args.includes('-h')){console.log(`
HsHub Pre-Indexer (streaming + chunked)
Usage: node hshub-index.js [fichiers...] [options]
  -o, --output <f>    Fichier de sortie (défaut: index.bin)
  -d, --dir <d>       Indexer un dossier
  --ext <.csv,...>    Extensions à inclure
  --exclude <f,...>   Fichiers à exclure
  --chunk <N>         Lignes par chunk (défaut: 1500000)
`);process.exit(0);}
let outputFile='index.bin',inputFiles=[],filterExts=null,excludeList=[],CHUNK_SIZE=1_500_000;
for(let i=0;i<args.length;i++){
  if((args[i]==='-o'||args[i]==='--output')&&args[i+1]){outputFile=args[++i];}
  else if((args[i]==='-d'||args[i]==='--dir')&&args[i+1]){const dir=args[++i];fs.readdirSync(dir).forEach(f=>{const full=path.join(dir,f);if(fs.statSync(full).isFile())inputFiles.push(full);});}
  else if(args[i]==='--ext'&&args[i+1]){filterExts=args[++i].split(',').map(e=>e.trim().toLowerCase().replace(/^\./,''));}
  else if(args[i]==='--exclude'&&args[i+1]){excludeList=args[++i].split(',').map(e=>e.trim().toLowerCase());}
  else if(args[i]==='--chunk'&&args[i+1]){CHUNK_SIZE=parseInt(args[++i])||CHUNK_SIZE;}
  else if(!args[i].startsWith('-')){inputFiles.push(args[i]);}
}
if(filterExts)inputFiles=inputFiles.filter(f=>filterExts.includes(path.extname(f).slice(1).toLowerCase()));
if(excludeList.length)inputFiles=inputFiles.filter(f=>!excludeList.includes(path.basename(f).toLowerCase()));
inputFiles=inputFiles.filter(f=>path.resolve(f)!==path.resolve(outputFile));
if(!inputFiles.length){console.error('❌  Aucun fichier à indexer.');process.exit(1);}
const PAT={
  lastName:/\b(df_naam|ts_naam|rq_naam|naam|^nom$|lastname|surname|familyname)\b/i,
  firstName:/\b(df_voornamen|ts_voornamen|voornamen|prenom|firstname|givenname)\b/i,
  birthDate:/\b(df_geboortedatum|geboortedatum|birth.?date|date.?naiss|dob|birthdate|date_naissance)\b/i,
  email:/email|e.mail|courriel/i,
  phone:/\b(tel|phone|gsm|mobile|telephone)\b/i,
  iban:/\biban\b/i,
  ip:/^ip(_addr(ess)?)?$/i,
  street:/\b(df_straat|ts_straat|straat|rue|street|voie)\b/i,
  streetNum:/\b(df_nummer|ts_nummer|numero\b|housenr)\b/i,
  postal:/\b(df_post_code|ts_post_code|post.?code|zip|cp\b|code.?postal|code_postal)\b/i,
  city:/\b(df_plaats|ts_plaats|plaats|ville|city|stad|commune)\b/i,
  country:/^(df_land|ts_land|land|pays|country)$/i,
  nationalId:/rijksregister|ssn|niss|nin|id.nat|national.id|matricule/i
};
function detect(headers){const m={};for(const[f,p]of Object.entries(PAT))for(const h of headers)if(p.test(h)){m[f]=h;break;}return m;}
const Nn=s=>!s?'':String(s).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim();
const ND=s=>!s?'':String(s).replace(/[-\/.]/g,'');
const enc=s=>Buffer.from(s||'','utf8');
const TMP_ROWS=path.join(os.tmpdir(),'hshub_rows_'+Date.now()+'.tmp');
const TMP_IDX=path.join(os.tmpdir(),'hshub_idx_'+Date.now()+'.tmp');
let rowsStream=fs.createWriteStream(TMP_ROWS);
let idxStream=fs.createWriteStream(TMP_IDX);
let curIdx=makeEmptyIdx(),globalRowId=0,chunkRowId=0,chunkStart=0;
function makeEmptyIdx(){return{lastName:new Map(),firstName:new Map(),birthFull:new Map(),birthYear:new Map(),birthMonth:new Map(),birthDay:new Map(),email:new Map(),phone:new Map(),iban:new Map(),ip:new Map(),postal:new Map(),city:new Map(),country:new Map(),nationalId:new Map(),street:new Map()};}
function ai(name,key,id){if(!key)return;const m=curIdx[name];if(!m.has(key))m.set(key,[]);m.get(key).push(id);}
function flushIdx(){
  if(chunkRowId===0)return;
  const hdr=Buffer.allocUnsafe(8);hdr.writeUInt32LE(chunkStart,0);hdr.writeUInt32LE(chunkRowId,4);idxStream.write(hdr);
  for(const name of Object.keys(curIdx)){
    const map=curIdx[name];const nb=Buffer.from(name,'ascii');
    const h2=Buffer.allocUnsafe(1+nb.length+4);h2.writeUInt8(nb.length,0);nb.copy(h2,1);h2.writeUInt32LE(map.size,1+nb.length);idxStream.write(h2);
    for(const[key,ids]of map.entries()){
      const kb=enc(key);const entry=Buffer.allocUnsafe(2+kb.length+4+ids.length*4);let p=0;
      entry.writeUInt16LE(kb.length,p);p+=2;kb.copy(entry,p);p+=kb.length;
      entry.writeUInt32LE(ids.length,p);p+=4;
      for(let i=0;i<ids.length;i++){entry.writeUInt32LE(ids[i],p);p+=4;}
      idxStream.write(entry);
    }
  }
  curIdx=makeEmptyIdx();chunkStart+=chunkRowId;chunkRowId=0;
}
function writeRow(dbi,fields){
  const bufs=fields.map(s=>enc(s));
  const tl=2+bufs.reduce((a,b)=>a+2+b.length,0);
  const buf=Buffer.allocUnsafe(tl);let pos=0;
  buf.writeUInt16LE(dbi,pos);pos+=2;
  for(const b of bufs){buf.writeUInt16LE(b.length,pos);pos+=2;b.copy(buf,pos);pos+=b.length;}
  rowsStream.write(buf);
}
function processRow(dbi,row,mp){
  const id=globalRowId;
  const g=f=>mp[f]?String(row[mp[f]]||''):'';
  const ln=g('lastName'),fn=g('firstName'),bd=g('birthDate');
  const st=g('street'),sn=g('streetNum'),pc=g('postal');
  const cy=g('city'),co=g('country'),ni=g('nationalId');
  const em=g('email'),ph=String(row[mp.phone]||'').replace(/\D/g,'');
  const ib=String(row[mp.iban]||'').replace(/\s/g,'').toLowerCase();
  const ip=String(row[mp.ip]||'').trim();
  writeRow(dbi,[ln,fn,bd,st,sn,pc,cy,co,ni,em,ph,ib,ip]);
  const lid=chunkRowId;
  const lnN=Nn(ln);if(lnN)ai('lastName',lnN,lid);
  const fnN=Nn(fn);if(fnN)ai('firstName',fnN,lid);
  const bdN=ND(bd);
  if(bdN){if(bdN.length>=8){ai('birthFull',bdN.slice(0,8),lid);ai('birthYear',bdN.slice(0,4),lid);ai('birthMonth',bdN.slice(4,6),lid);ai('birthDay',bdN.slice(6,8),lid);}else{ai('birthFull',bdN,lid);if(bdN.length>=4)ai('birthYear',bdN.slice(0,4),lid);if(bdN.length>=6)ai('birthMonth',bdN.slice(4,6),lid);}}
  const emN=Nn(em);if(emN)ai('email',emN,lid);
  if(ph&&ph!=='0')ai('phone',ph,lid);
  if(ib)ai('iban',ib,lid);
  if(ip)ai('ip',ip,lid);
  if(pc)ai('postal',pc.trim(),lid);
  const cyN=Nn(cy);if(cyN)ai('city',cyN,lid);
  const coN=Nn(co);if(coN)ai('country',coN,lid);
  const niN=String(ni||'').replace(/[\s\-.]/g,'');if(niN&&niN!=='0')ai('nationalId',niN,lid);
  const stN=Nn(st);if(stN)ai('street',stN,lid);
  globalRowId++;chunkRowId++;
  if(chunkRowId>=CHUNK_SIZE)flushIdx();
}
function guessDelim(line){const c={',':0,';':0,'|':0,'\t':0};for(const ch of line)if(ch in c)c[ch]++;return Object.entries(c).sort((a,b)=>b[1]-a[1])[0][0];}
async function loadCsvTsv(filePath,dbi,istsv){
  return new Promise((resolve,reject)=>{
    const stream=fs.createReadStream(filePath,{encoding:'latin1'});
    const rl=readline.createInterface({input:stream,crlfDelay:Infinity});
    let headers=null,mapping=null,autoDelim=istsv?'\t':null,count=0;
    rl.on('line',line=>{
      if(!line.trim())return;
      if(!headers){if(!autoDelim)autoDelim=guessDelim(line);headers=line.split(autoDelim).map(h=>h.trim().replace(/^["']|["']$/g,'').replace(/^\uFEFF/,''));mapping=detect(headers);mapping._headers=headers;return;}
      const vals=line.split(autoDelim).map(v=>v.trim().replace(/^["']|["']$/g,''));
      const row={};headers.forEach((h,i)=>row[h]=vals[i]||'');
      processRow(dbi,row,mapping);count++;
    });
    rl.on('close',()=>resolve(count));rl.on('error',reject);stream.on('error',reject);
  });
}
async function loadJson(filePath,dbi){
  const fd=fs.openSync(filePath,'r');const peek=Buffer.alloc(256);fs.readSync(fd,peek,0,256,0);fs.closeSync(fd);
  const firstChar=peek.toString('utf8').trimStart()[0];
  return new Promise((resolve,reject)=>{
    const stream=fs.createReadStream(filePath,{encoding:'utf8'});
    let mapping=null,count=0;
    if(firstChar==='['){
      let depth=0,objBuf='',inStr=false,esc=false;
      stream.on('data',chunk=>{for(let i=0;i<chunk.length;i++){const c=chunk[i];if(esc){esc=false;if(depth>0)objBuf+=c;continue}if(inStr){if(c==='\\')esc=true;else if(c==='"')inStr=false;if(depth>0)objBuf+=c;continue}if(c==='"'){inStr=true;if(depth>0)objBuf+=c;continue}if(c==='{'){depth++;objBuf+=c}else if(c==='}'){depth--;objBuf+=c;if(depth===0){try{const r=JSON.parse(objBuf);if(!mapping)mapping=detect(Object.keys(r));processRow(dbi,r,mapping);count++;}catch(e){}objBuf=''}}else if(depth>0){objBuf+=c}}});
      stream.on('end',()=>resolve(count));stream.on('error',reject);
    }else{
      const rl=readline.createInterface({input:stream,crlfDelay:Infinity});
      rl.on('line',line=>{const t=line.trim();if(!t||t[0]!=='{')return;try{const r=JSON.parse(t);if(!mapping)mapping=detect(Object.keys(r));processRow(dbi,r,mapping);count++;}catch(e){}});
      rl.on('close',()=>resolve(count));rl.on('error',reject);stream.on('error',reject);
    }
  });
}
const dbs=[];
async function assembleBin(){
  flushIdx();
  await Promise.all([new Promise(r=>rowsStream.end(r)),new Promise(r=>idxStream.end(r))]);
  console.log(`\n💾 Assemblage du fichier .bin…`);
  process.stdout.write('  ↳ Fusion des index…');
  const globalIdx=makeEmptyIdx();
  const idxNames=Object.keys(globalIdx);
  const idxFd=fs.openSync(TMP_IDX,'r');
  const idxSize=fs.statSync(TMP_IDX).size;
  let idxPos=0;
  function readExact(fd,pos,len){const buf=Buffer.allocUnsafe(len);let got=0;while(got<len){const n=fs.readSync(fd,buf,got,len-got,pos+got);if(n===0)break;got+=n;}return buf;}
  while(idxPos<idxSize){
    const hdr=readExact(idxFd,idxPos,8);const cs=hdr.readUInt32LE(0);idxPos+=8;
    for(const name of idxNames){
      const nb=readExact(idxFd,idxPos,1);const nl=nb.readUInt8(0);idxPos+=1+nl;
      const nkb=readExact(idxFd,idxPos,4);const nk=nkb.readUInt32LE(0);idxPos+=4;
      const gm=globalIdx[name];
      for(let k=0;k<nk;k++){
        const klb=readExact(idxFd,idxPos,2);const kl=klb.readUInt16LE(0);idxPos+=2;
        const kb=readExact(idxFd,idxPos,kl);idxPos+=kl;const key=kb.toString('utf8');
        const nib=readExact(idxFd,idxPos,4);const ni=nib.readUInt32LE(0);idxPos+=4;
        const ib=readExact(idxFd,idxPos,ni*4);idxPos+=ni*4;
        if(!gm.has(key))gm.set(key,[]);const arr=gm.get(key);
        for(let i=0;i<ni;i++)arr.push(cs+ib.readUInt32LE(i*4));
      }
    }
  }
  fs.closeSync(idxFd);
  console.log(' ✓');
  const outFd=fs.openSync(outputFile,'w');let outPos=0;
  const wb=buf=>{fs.writeSync(outFd,buf,0,buf.length,outPos);outPos+=buf.length;};
  const u8=v=>{const b=Buffer.allocUnsafe(1);b.writeUInt8(v,0);return b;};
  const u16=v=>{const b=Buffer.allocUnsafe(2);b.writeUInt16LE(v,0);return b;};
  const u32=v=>{const b=Buffer.allocUnsafe(4);b.writeUInt32LE(v,0);return b;};
  wb(Buffer.from('HSHB','ascii'));wb(u16(1));wb(u32(dbs.length));wb(u32(globalRowId));wb(u32(idxNames.length));
  for(const db of dbs){const nb=enc(db.name);wb(u16(nb.length));wb(nb);wb(u32(db.count));}
  process.stdout.write('  ↳ Écriture des lignes…');
  const rowFd=fs.openSync(TMP_ROWS,'r');const rowSize=fs.statSync(TMP_ROWS).size;
  const RBUF=Buffer.allocUnsafe(8*1024*1024);let rPos=0,dots=0;
  while(rPos<rowSize){const n=fs.readSync(rowFd,RBUF,0,RBUF.length,rPos);if(n===0)break;fs.writeSync(outFd,RBUF,0,n,outPos);outPos+=n;rPos+=n;if(++dots%64===0)process.stdout.write('.');}
  fs.closeSync(rowFd);console.log(' ✓');
  process.stdout.write('  ↳ Écriture de l\'index…');
  for(const name of idxNames){
    const map=globalIdx[name];const nb=Buffer.from(name,'ascii');
    wb(u8(nb.length));wb(nb);wb(u32(map.size));
    for(const[key,ids]of map.entries()){
      const kb=enc(key);wb(u16(kb.length));wb(kb);wb(u32(ids.length));
      const ibuf=Buffer.allocUnsafe(ids.length*4);for(let i=0;i<ids.length;i++)ibuf.writeUInt32LE(ids[i],i*4);wb(ibuf);
    }
  }
  console.log(' ✓');
  fs.closeSync(outFd);
  try{fs.unlinkSync(TMP_ROWS);}catch(_){}try{fs.unlinkSync(TMP_IDX);}catch(_){}
  const outSize=fs.statSync(outputFile).size;
  console.log(`\n✅ ${outputFile} — ${(outSize/1024/1024).toFixed(1)} Mo`);
  console.log(`Pour utiliser : placez index.bin à côté de hshub_v5.html\n`);
}
async function main(){
  console.log(`\nHsHub Pre-Indexer (streaming + chunked)`);
  console.log(`${'─'.repeat(50)}`);
  console.log(`Fichiers : ${inputFiles.length}   Chunk : ${CHUNK_SIZE.toLocaleString()} lignes`);
  console.log(`Sortie   : ${outputFile}\n`);
  const t0=Date.now();
  for(const filePath of inputFiles){
    if(!fs.existsSync(filePath)){console.warn(`  ⚠  Introuvable: ${filePath}`);continue;}
    const size=fs.statSync(filePath).size;
    const ext=path.extname(filePath).slice(1).toLowerCase();
    const name=path.basename(filePath);
    const dbi=dbs.length;dbs.push({name,count:0});
    const t1=Date.now();
    process.stdout.write(`  ⏳ ${name} (${(size/1024/1024).toFixed(1)} Mo)… `);
    let count=0;
    try{
      if(ext==='csv')count=await loadCsvTsv(filePath,dbi,false);
      else if(ext==='tsv'||ext==='tab')count=await loadCsvTsv(filePath,dbi,true);
      else if(ext==='json'||ext==='ndjson'||ext==='jsonl')count=await loadJson(filePath,dbi);
      else count=await loadCsvTsv(filePath,dbi,false);
    }catch(e){console.error(`\n  ❌ ${e.message}`);dbs.pop();continue;}
    dbs[dbi].count=count;
    console.log(`✓ ${count.toLocaleString()} entrées (${((Date.now()-t1)/1000).toFixed(1)}s)`);
  }
  console.log(`\n📊 Total : ${globalRowId.toLocaleString()} lignes`);
  await assembleBin();
  console.log(`⏱  Temps total : ${((Date.now()-t0)/1000).toFixed(1)}s`);
}
main().catch(e=>{console.error('❌ Erreur fatale:',e);try{fs.unlinkSync(TMP_ROWS);}catch(_){}try{fs.unlinkSync(TMP_IDX);}catch(_){}process.exit(1);});
