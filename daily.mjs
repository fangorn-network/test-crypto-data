#!/usr/bin/env node
// Daily upload of a market-data recorder's tape (books-<d>.jsonl.gz, events-<d>.jsonl.gz in TAPE_DIR) to the public
// Fangorn repo test-crypto-data, as the wallet in ~/.fangorn/config.json.
// Per completed UTC day, two files pinned through Fangorn's storage (the publisher's quota) + one `file` record each:
//   books-<d>.jsonl.gz  as recorded: {"t", "cb": {coin: [bid, bidsz, ask, asksz]}, "kr": {...}} once a second
//   pm-<d>.jsonl.gz     events-<d> cut to Polymarket rows (pmd, pmt, pmc), fields allowlisted (Coinbase matches dropped)
// Only the allowlisted market-data fields are published. Days already in the repo are skipped, so a missed run catches up.
//   node daily.mjs [--dry-run]       config: .env (see .env.example); cron daily, after the UTC day closes
import { createReadStream, createWriteStream, readdirSync, readFileSync, writeFileSync, statSync, mkdirSync } from "node:fs";
import { createGunzip, createGzip } from "node:zlib";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { execFileSync } from "node:child_process";
import { homedir } from "node:os";
import { join, dirname, basename } from "node:path";
import { pipeline } from "node:stream/promises";

const REPO = "test-crypto-data";
const HERE = dirname(new URL(import.meta.url).pathname);
try { process.loadEnvFile(join(HERE, ".env")); } catch {}    // gitignored; real env vars win
const SRC = process.env.TAPE_DIR;                              // where the recorder writes its daily files
if (!SRC) { console.error("TAPE_DIR is not set: copy .env.example to .env"); process.exit(1); }
const STAGE = join(HERE, "stage");                             // gitignored; staged files are pinned copies, safe to delete
const FANGORN = execFileSync("which", ["fangorn"]).toString().trim();
const SDK = join(dirname(execFileSync("readlink", ["-f", FANGORN]).toString().trim()), "..", "..");
const DRY = process.argv.includes("--dry-run");
const KEEP = { pmd: ["k", "r", "s", "b", "a"], pmt: ["k", "r", "s", "px", "q", "tt", "tk"], pmc: ["k", "r"] };   // allowlist
const TK = ["side", "intent", "outcomeSide", "action"];
const COINS = "BTC, ETH, SOL, XRP";

const lines = (f) => createInterface({ input: createReadStream(f).pipe(createGunzip()), crlfDelay: Infinity });
const sha = async (f) => { const h = createHash("sha256"); for await (const c of createReadStream(f)) h.update(c); return h.digest("hex"); };
const iso = (t) => new Date(t * 1000).toISOString().slice(0, 19) + "Z";
const fangorn = (...a) => execFileSync(FANGORN, a, { cwd: HERE, encoding: "utf-8" });

function published() {                                         // record ids already on chain
    try { return new Set(JSON.parse(fangorn("read", REPO)).records.map((r) => r.id)); }
    catch (e) { if (DRY) return new Set(); throw e; }
}

async function books(day) {                                    // published as recorded
    const f = join(SRC, `books-${day}.jsonl.gz`); let n = 0, t0 = null, t1 = null;
    for await (const l of lines(f)) { if (!l.trim()) continue; const t = JSON.parse(l).t; t0 ??= t; t1 = t; n++; }
    return { f, rows: n, counts: { books: n }, t0, t1 };
}

async function pm(day) {                                       // events → Polymarket rows only, allowlisted fields
    const f = join(STAGE, `pm-${day}.jsonl.gz`), counts = { pmd: 0, pmt: 0, pmc: 0 }, slugs = new Set(); let t0 = null, t1 = null;
    const out = createGzip(); const done = pipeline(out, createWriteStream(f));
    for await (const l of lines(join(SRC, `events-${day}.jsonl.gz`))) {
        if (!l.trim()) continue; const d = JSON.parse(l); if (!KEEP[d.k]) continue;
        const o = Object.fromEntries(KEEP[d.k].filter((k) => k in d).map((k) => [k, d[k]]));
        if (o.tk) o.tk = Object.fromEntries(TK.filter((k) => k in o.tk).map((k) => [k, o.tk[k]]));
        if (!out.write(JSON.stringify(o) + "\n")) await new Promise((r) => out.once("drain", r));
        counts[d.k]++; if (o.s) slugs.add(o.s); t0 ??= o.r; t1 = o.r;
    }
    out.end(); await done;
    return { f, rows: counts.pmd + counts.pmt + counts.pmc, counts, t0, t1, markets: slugs.size };
}

const SCHEMA = {
    books: '{"t": epoch s, "cb": {coin: [bid, bid size, ask, ask size]}, "kr": {coin: [...]}} once a second; cb = Coinbase level2, kr = Kraken book (may be empty)',
    pm: '{"k":"pmt","r":recv epoch s,"s":market slug,"px","q","tt":trade time,"tk":{side,intent,outcomeSide,action}} every trade; {"k":"pmd","r","s","b":[[px,qty]x10],"a":[[px,qty]x10]} book on change (YES price); {"k":"pmc","r"} reconnect, gap before it',
};
const PROSE = {
    books: (d, s) => `Coinbase spot top-of-book (best bid/ask and sizes) once a second for ${COINS} against USD, ${d} UTC, ${iso(s.t0).slice(11, 16)}–${iso(s.t1).slice(11, 16)} covered, ${s.rows.toLocaleString("en")} snapshots.`,
    pm: (d, s) => `Polymarket US crypto up/down markets (${COINS}; 5-minute, 15-minute and 1-hour windows) on ${d} UTC, ${iso(s.t0).slice(11, 16)}–${iso(s.t1).slice(11, 16)} covered: every trade (${s.counts.pmt.toLocaleString("en")}) and every top-10 order-book change (${s.counts.pmd.toLocaleString("en")}) across ${s.markets} markets, as received.`,
};

async function record(kind, day, s, pin) {
    const d = `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6)}`, size = statSync(s.f).size, sum = await sha(s.f);
    const cid = pin ? await pin(s.f) : "<dry-run>";
    return { id: `${kind}-${day}`, type: "file", payload: {
        file: `${GATEWAY}/ipfs/${cid}`, cid, mime: "application/gzip", size, sha256: sum, title: basename(s.f),
        kind, date: d, first_t: iso(s.t0), last_t: iso(s.t1), hours: +((s.t1 - s.t0) / 3600).toFixed(2), rows: s.rows, counts: s.counts,
        ...(s.markets ? { markets: s.markets } : {}), coins: COINS, format: "gzip JSON lines", schema: SCHEMA[kind], text: PROSE[kind](d, s),
    } };
}

const CFG = JSON.parse(readFileSync(join(homedir(), ".fangorn", "config.json"), "utf-8"));
const GATEWAY = "https://ipfs-test.fangorn.network";          // Fangorn's public gateway (files sit on Fangorn's storage, not ours)
const { privateKeyToAccount } = await import(join(SDK, "node_modules", "viem", "_esm", "accounts", "index.js"));
const ACCOUNT = privateKeyToAccount(CFG.privateKey);           // the publisher: the repo is ACCOUNT.address/REPO
async function pinner() {                                      // same path as the CLI with no Pinata JWT: Fangorn's storage worker,
    const { SignedUrlBackend } = await import(join(SDK, "lib", "providers", "storage", "signed-url.js"));   // charged to the publisher's quota
    const a = ACCOUNT, signer = { address: a.address, signMessage: (message) => a.signMessage({ message }) };
    const s = new SignedUrlBackend(CFG.signedUrlWorkerUrl, signer, GATEWAY);   // blank worker URL → the network's default
    return async (f) => s.putFile(readFileSync(f), basename(f));
}

mkdirSync(STAGE, { recursive: true });
// .fangorn/ is gitignored local state: rebuild it from the chain tip every run (also catches pushes made elsewhere,
// e.g. fangorn card or the hub, which don't move the local HEAD)
fangorn("clone", ACCOUNT.address, REPO);
const today = new Date().toISOString().slice(0, 10).replaceAll("-", ""), have = published();
const days = readdirSync(SRC).map((n) => n.match(/^books-(\d{8})\.jsonl\.gz$/)?.[1]).filter((d) => d && d < today).sort()
    .filter((d) => !have.has(`books-${d}`) || !have.has(`pm-${d}`));
if (!days.length) { console.log(`${new Date().toISOString()} nothing new`); process.exit(0); }
const pin = DRY ? null : await pinner(), recs = [];
for (const day of days) {
    if (!have.has(`books-${day}`)) recs.push(await record("books", day, await books(day), pin));
    if (!have.has(`pm-${day}`)) recs.push(await record("pm", day, await pm(day), pin));
    console.log(`${new Date().toISOString()} ${day} staged`);
}
const cf = join(STAGE, `commit-${today}.json`); writeFileSync(cf, JSON.stringify({ records: recs }, null, 1));
if (DRY) { console.log(JSON.stringify(recs, null, 1)); process.exit(0); }
console.log(fangorn("commit", cf)); console.log(fangorn("push"));
