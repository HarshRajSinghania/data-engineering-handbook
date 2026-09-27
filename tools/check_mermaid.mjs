// Parse every ```mermaid block in docs/ with the real Mermaid parser, so a broken diagram fails CI.
// Usage: npm install --no-save --prefix tools mermaid jsdom && node tools/check_mermaid.mjs
import { JSDOM } from "jsdom";
import fs from "fs";
import path from "path";
const dom = new JSDOM("<!DOCTYPE html><body></body>");
globalThis.window = dom.window; globalThis.document = dom.window.document;
const mermaid = (await import("mermaid")).default;
const root = process.argv[2] || "docs";
const files = [];
(function walk(d){ for (const f of fs.readdirSync(d)) { const p = path.join(d,f); fs.statSync(p).isDirectory() ? walk(p) : p.endsWith(".md") && files.push(p); } })(root);
let n=0, bad=0;
for (const f of files) {
  const t = fs.readFileSync(f,"utf8");
  for (const m of t.matchAll(/```mermaid\n([\s\S]*?)```/g)) {
    n++;
    try { await mermaid.parse(m[1]); } catch (e) { bad++; console.log("FAIL", f, String(e.message||e).split("\n").slice(0,3).join(" | ")); }
  }
}
console.log(`parsed ${n} mermaid blocks, ${bad} failed`);
