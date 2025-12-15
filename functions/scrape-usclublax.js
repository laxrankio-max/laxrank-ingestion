// netlify/functions/scrape-usclublax.js
// CommonJS Netlify Function — stable deployment
// POST { urls: ["https://usclublax.com/team_info/?y=2024&t=105218"] }
// Header: Authorization: Bearer <SCRAPE_API_TOKEN>

const crypto = require("crypto");
const cheerio = require("cheerio");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SCRAPE_API_TOKEN = process.env.SCRAPE_API_TOKEN;

function must(name, val) {
  if (!val) throw new Error(`Missing env var: ${name}`);
}

function bearerToken(headers = {}) {
  const auth = headers.authorization || headers.Authorization || "";
  const m = auth.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

const sha256 = (s) => crypto.createHash("sha256").update(s).digest("hex");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function parseExternalTeamId(url) {
  try {
    const u = new URL(url);
    return u.searchParams.get("t");
  } catch {
    return null;
  }
}

function tryParseDateMMDDYYYY(s) {
  const m = (s || "").trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!m) return null;
  const mm = m[1].padStart(2, "0");
  const dd = m[2].padStart(2, "0");
  return `${m[3]}-${mm}-${dd}`;
}

function extractTeamName($) {
  return (
    $("h2").first().text().trim() ||
    $("h1").first().text().trim() ||
    $("title").text().trim() ||
    "Unknown Team"
  );
}

function extractGradYear(name) {
  const m = (name || "").match(/\((\d{4})\)/);
  return m ? parseInt(m[1], 10) : null;
}

function parseResultScore(text) {
  const clean = (text || "").replace(/\s+/g, " ").trim();
  const r = clean.match(/\b([WLT])\b/i);
  const result = r ? r[1].toUpperCase() : null;
  const s = clean.match(/(\d+)\s*-\s*(\d+)/);
  return {
    result,
    teamScore: s ? parseInt(s[1], 10) : null,
    oppScore: s ? parseInt(s[2], 10) : null,
  };
}

function findGamesRows($) {
  const byId = $("#games_table");
  if (byId.length) {
    const rows = byId.find("tbody tr").toArray();
    if (rows.length) return rows;
  }

  let rows = [];
  $("

