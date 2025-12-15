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
  $("table").each((_, tbl) => {
    if (rows.length) return;
    const headers = $(tbl)
      .find("thead th")
      .toArray()
      .map((th) => $(th).text().trim().toLowerCase());
    const hasOpponent = headers.some((h) => h.includes("opponent"));
    const hasResult =
      headers.some((h) => h.includes("result")) ||
      headers.some((h) => h.includes("score"));
    if (hasOpponent && hasResult) rows = $(tbl).find("tbody tr").toArray();
  });

  return rows;
}

function parseGames($) {
  const rows = findGamesRows($);
  const games = [];

  rows.forEach((row, rowIndex) => {
    const cells = $(row)
      .find("td")
      .toArray()
      .map((td) => $(td).text().replace(/\s+/g, " ").trim())
      .filter(Boolean);

    if (cells.length < 3) return;

    const dateText = cells.find((c) => tryParseDateMMDDYYYY(c));
    const gameDate = dateText ? tryParseDateMMDDYYYY(dateText) : null;

    const resultCell = cells.find(
      (c) => /\b[WLT]\b/i.test(c) && /\d+\s*-\s*\d+/.test(c)
    );
    const { result, teamScore, oppScore } = parseResultScore(resultCell || "");
    if (!result) return;

    const remaining = cells.filter((c) => c !== dateText && c !== resultCell);
    remaining.sort((a, b) => b.length - a.length);
    const opponentName = remaining[0] || "Unknown Opponent";
    const eventName = remaining[1] || "Unknown Event";

    games.push({
      row_index: rowIndex,
      opponent_name: opponentName,
      event_name: eventName,
      game_date: gameDate,
      result,
      team_score: teamScore,
      opponent_score: oppScore,
      raw_cells: cells,
    });
  });

  return games;
}

async function getOrCreateExternalSourceId(supabase) {
  const { data, error } = await supabase
    .from("external_sources")
    .select("id")
    .ilike("name", "usclublax")
    .limit(1);

  if (error) throw error;
  if (data?.length) return data[0].id;

  const { data: created, error: e2 } = await supabase
    .from("external_sources")
    .insert({ name: "usclublax", base_url: "https://usclublax.com" })
    .select("id")
    .single();

  if (e2) throw e2;
  return created.id;
}

async function upsertTeamViaExternalLink(
  supabase,
  sourceId,
  externalId,
  externalUrl,
  teamName,
  gradYear
) {
  const { data: links, error: e1 } = await supabase
    .from("external_entity_links")
    .select("entity_id")
    .eq("source_id", sourceId)
    .eq("entity_type", "team")
    .eq("external_id", externalId)
    .limit(1);

  if (e1) throw e1;

  let teamId = links?.length ? links[0].entity_id : null;

  if (!teamId) {
    const { data: teamRow, error: e2 } = await supabase
      .from("teams")
      .insert({
        name: teamName,
        grad_year: gradYear,
        is_active: true,
      })
      .select("id")
      .single();
    if (e2) throw e2;

    teamId = teamRow.id;

    const { error: e3 } = await supabase.from("external_entity_links").insert({
      source_id: sourceId,
      entity_type: "team",
      entity_id: teamId,
      external_id: externalId,
      external_url: externalUrl,
      last_synced_at: new Date().toISOString(),
    });
    if (e3) throw e3;
  } else {
    const { error: e4 } = await supabase
      .from("teams")
      .update({ name: teamName, grad_year: gradYear })
      .eq("id", teamId);
    if (e4) throw e4;
  }

  return teamId;
}

async function upsertEvent(supabase, name, startDate) {
  const payload = { name, start_date: startDate || null };
  const { data, error } = await supabase
    .from("events")
    .upsert(payload, { onConflict: "name,start_date" })
    .select("id")
    .single();

  if (!error) return data.id;

  // fallback if your events unique constraint differs
  const { data: found } = await supabase
    .from("events")
    .select("id")
    .ilike("name", name)
    .limit(1);

  if (found?.length) return found[0].id;
  throw error;
}

async function upsertGames(supabase, teamId, externalTeamId, games) {
  let upserted = 0;

  for (const g of games) {
    const eventId = await upsertEvent(
      supabase,
      g.event_name || "Unknown Event",
      g.game_date
    );

    const key = sha256(
      [
        externalTeamId,
        g.game_date || "",
        g.event_name || "",
        g.opponent_name || "",
        g.team_score ?? "",
        g.opponent_score ?? "",
        g.result || "",
        g.row_index,
      ].join("|")
    );

    const payload = {
      source: "usclublax",
      source_game_key: key,
      team_id: teamId,
      event_id: eventId,
      opponent_name: g.opponent_name,
      game_date: g.game_date,
      result: g.result,
      team_score: g.team_score,
      opponent_score: g.opponent_score,
      raw_json: g,
    };

    const { error } = await supabase
      .from("games")
      .upsert(payload, { onConflict: "source,source_game_key" });

    if (error) throw error;

    upserted += 1;
    await sleep(100);
  }

  return upserted;
}

async function touchQueueRow(supabase, url, status, lastError) {
  const { data: existing } = await supabase
    .from("scrape_queue")
    .select("id")
    .eq("url", url)
    .limit(1);

  if (existing?.length) {
    await supabase
      .from("scrape_queue")
      .update({
        status,
        last_run_at: new Date().toISOString(),
        last_error: lastError || null,
      })
      .eq("id", existing[0].id);
    return;
  }

  await supabase.from("scrape_queue").insert({
    url,
    source: "usclublax",
    status,
    last_run_at: new Date().toISOString(),
    last_error: lastError || null,
  });
}

module.exports.handler = async (event) => {
  try {
    must("SUPABASE_URL", SUPABASE_URL);
    must("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY);
    must("SCRAPE_API_TOKEN", SCRAPE_API_TOKEN);

    const token = bearerToken(event.headers || {});
    if (!token || token !== SCRAPE_API_TOKEN) {
      return { statusCode: 401, body: JSON.stringify({ ok: false, error: "Unauthorized" }) };
    }

    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: JSON.stringify({ ok: false, error: "Use POST" }) };
    }

    const body = event.body ? JSON.parse(event.body) : {};
    const urls = Array.isArray(body.urls) ? body.urls : [];
    if (!urls.length) {
      return { statusCode: 400, body: JSON.stringify({ ok: false, error: "Provide { urls: [...] }" }) };
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
    });

    const sourceId = await getOrCreateExternalSourceId(supabase);
    const results = [];

    for (const url of urls.slice(0, 10)) {
      await touchQueueRow(supabase, url, "processing", null);

      try {
        const externalTeamId = parseExternalTeamId(url);
        if (!externalTeamId) throw new Error("Missing t=... in URL");

        const res = await fetch(url, {
          headers: { "user-agent": "Mozilla/5.0 (LaxRankIngestion/1.0)" },
        });
        if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);

        const html = await res.text();
        const $ = cheerio.load(html);

        const teamName = extractTeamName($);
        const gradYear = extractGradYear(teamName);
        const games = parseGames($);

        const teamId = await upsertTeamViaExternalLink(
          supabase,
          sourceId,
          externalTeamId,
          url,
          teamName,
          gradYear
        );

        const gamesUpserted = await upsertGames(supabase, teamId, externalTeamId, games);

        await touchQueueRow(supabase, url, "completed", null);

        results.push({
          url,
          ok: true,
          external_team_id: externalTeamId,
          team_id: teamId,
          team_name: teamName,
          games_found: games.length,
          games_upserted: gamesUpserted,
        });
      } catch (err) {
        const msg = err?.message || String(err);
        await touchQueueRow(supabase, url, "failed", msg);
        results.push({ url, ok: false, error: msg });
      }

      await sleep(500);
    }

    return { statusCode: 200, body: JSON.stringify({ ok: true, results }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: err?.message || String(err) }) };
  }
};
