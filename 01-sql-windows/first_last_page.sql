-- Author: MaksMaidaanov
-- Query: First and last page per user for 2017-08-01
WITH page_views AS (
  SELECT
    fullVisitorId AS user_id,
    TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime + hit.time/1000 AS INT64)) AS hit_time,
    hit.page.pagePath AS page_location,
    ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY hit.time ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY hit.time DESC) AS rn_desc
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`,
    UNNEST(hits) AS hit
  WHERE
    hit.type = 'PAGE'
)
SELECT
  user_id,
  MAX(IF(rn_asc = 1, page_location, NULL)) AS first_page,
  MAX(IF(rn_desc = 1, page_location, NULL)) AS last_page,
  MIN(hit_time) AS first_time,
  MAX(hit_time) AS last_time,
  COUNT(*) AS page_views_count
FROM
  page_views
GROUP BY
  user_id
LIMIT 10;