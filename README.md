# LaxRank Ingestion (Netlify Functions)

This repo deploys a serverless ingestion endpoint on Netlify that:
- Accepts one or more USClubLax *team* URLs
- Scrapes team name + games/results (best-effort)
- Upserts into Supabase tables (teams, events, games, external_entity_links)
- Updates scrape_queue status

## Deploy (Netlify)
1. Push this repo to GitHub
2. In Netlify: **Add new site → Import from GitHub**
3. Set environment variables (Site settings → Environment variables):
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SCRAPE_API_TOKEN`

## Endpoint
After deploy, call:

`https://YOUR-SITE.netlify.app/.netlify/functions/scrape-usclublax`

## Test (curl)
```bash
curl -X POST "https://YOUR-SITE.netlify.app/.netlify/functions/scrape-usclublax" \
  -H "Authorization: Bearer $SCRAPE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"urls":["https://usclublax.com/team_info/?y=2024&t=105218"]}'
```

## Notes
- This function uses your Supabase **Service Role Key** (server-side only).
- Rate limiting is included to reduce blocking.
- Data mapping is durable via:
  - `external_sources` (seeded/created as needed)
  - `external_entity_links` linking external team IDs to internal team UUIDs
  - `games` idempotency via `source` + `source_game_key`

