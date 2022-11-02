
# Simple queue pop for emews_queue_OUT

# https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/

sql <<EOF
DELETE FROM emews_queue_OUT
WHERE eq_id = (
  SELECT eq_id
  FROM emews_queue_OUT
  ORDER BY eq_id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
EOF
