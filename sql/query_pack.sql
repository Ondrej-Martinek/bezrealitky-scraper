-- Bezrealitky scraper: SQL query pack
-- Replace :scope_key placeholders in your SQL client.
-- Example scope_key: hostivice:pronajem:bytu,domu

-- 1) Latest successful runs
SELECT
  id,
  started_at,
  finished_at,
  status,
  found_urls,
  scraped_count,
  error_count,
  metadata
FROM scrape_runs
WHERE scope_key = :scope_key
ORDER BY id DESC
LIMIT 20;

-- 2) Currently active listings in scope (latest first)
SELECT
  l.listing_id,
  l.url,
  l.price_czk,
  l.total_monthly_czk,
  l.surface_m2,
  l.address,
  l.available_from_utc,
  s.first_seen_at,
  s.last_seen_at
FROM listing_scope_state s
JOIN listings l ON l.listing_id = s.listing_id
WHERE s.scope_key = :scope_key
  AND s.is_active = TRUE
ORDER BY s.last_seen_at DESC;

-- 3) New listings in last 7 days
SELECT
  h.created_at,
  l.listing_id,
  l.url,
  l.price_czk,
  l.address
FROM listing_history h
JOIN listings l ON l.listing_id = h.listing_id
WHERE h.scope_key = :scope_key
  AND h.event_type = 'NEW'
  AND h.created_at >= NOW() - INTERVAL '7 days'
ORDER BY h.created_at DESC;

-- 4) Price drops in last 7 days (from UPDATED events)
SELECT
  h.created_at,
  l.listing_id,
  l.url,
  (h.old_values->>'price_czk')::numeric AS old_price_czk,
  (h.new_values->>'price_czk')::numeric AS new_price_czk,
  ((h.old_values->>'price_czk')::numeric - (h.new_values->>'price_czk')::numeric) AS drop_czk,
  l.address
FROM listing_history h
JOIN listings l ON l.listing_id = h.listing_id
WHERE h.scope_key = :scope_key
  AND h.event_type = 'UPDATED'
  AND h.old_values ? 'price_czk'
  AND h.new_values ? 'price_czk'
  AND (h.new_values->>'price_czk')::numeric < (h.old_values->>'price_czk')::numeric
  AND h.created_at >= NOW() - INTERVAL '7 days'
ORDER BY drop_czk DESC, h.created_at DESC;

-- 5) Removed listings in last 7 days
SELECT
  h.created_at,
  l.listing_id,
  l.url,
  l.address,
  l.price_czk
FROM listing_history h
JOIN listings l ON l.listing_id = h.listing_id
WHERE h.scope_key = :scope_key
  AND h.event_type = 'REMOVED'
  AND h.created_at >= NOW() - INTERVAL '7 days'
ORDER BY h.created_at DESC;

-- 6) Listings with latest known price under threshold
-- Adjust value as needed.
SELECT
  l.listing_id,
  l.url,
  l.price_czk,
  l.charges_czk,
  l.total_monthly_czk,
  l.surface_m2,
  l.address
FROM listing_scope_state s
JOIN listings l ON l.listing_id = s.listing_id
WHERE s.scope_key = :scope_key
  AND s.is_active = TRUE
  AND l.price_czk <= 20000
ORDER BY l.price_czk ASC;

-- 7) Latest snapshot + previous price for each active listing
WITH latest_price_change AS (
  SELECT DISTINCT ON (h.listing_id)
    h.listing_id,
    (h.old_values->>'price_czk')::numeric AS previous_price_czk,
    (h.new_values->>'price_czk')::numeric AS current_price_czk,
    h.created_at AS changed_at
  FROM listing_history h
  WHERE h.scope_key = :scope_key
    AND h.event_type = 'UPDATED'
    AND h.old_values ? 'price_czk'
    AND h.new_values ? 'price_czk'
  ORDER BY h.listing_id, h.created_at DESC
)
SELECT
  l.listing_id,
  l.url,
  l.address,
  l.price_czk,
  pc.previous_price_czk,
  pc.changed_at
FROM listing_scope_state s
JOIN listings l ON l.listing_id = s.listing_id
LEFT JOIN latest_price_change pc ON pc.listing_id = l.listing_id
WHERE s.scope_key = :scope_key
  AND s.is_active = TRUE
ORDER BY l.last_seen_at DESC;

-- 8) Daily run quality summary (last 14 days)
SELECT
  date_trunc('day', started_at) AS day,
  COUNT(*) AS runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
  AVG(scraped_count)::numeric(10,2) AS avg_scraped,
  SUM(error_count) AS total_errors
FROM scrape_runs
WHERE scope_key = :scope_key
  AND started_at >= NOW() - INTERVAL '14 days'
GROUP BY 1
ORDER BY 1 DESC;

-- 9) Top listings by number of updates
SELECT
  l.listing_id,
  l.url,
  l.address,
  COUNT(*) AS update_events
FROM listing_history h
JOIN listings l ON l.listing_id = h.listing_id
WHERE h.scope_key = :scope_key
  AND h.event_type = 'UPDATED'
GROUP BY l.listing_id, l.url, l.address
ORDER BY update_events DESC
LIMIT 20;

-- 10) Notifications log (latest 50)
SELECT
  id,
  dedupe_key,
  channel,
  target,
  sent_at,
  payload
FROM notifications
ORDER BY sent_at DESC
LIMIT 50;
