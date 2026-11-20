-- Chạy: docker exec -i <postgres> psql -U postgres -d txn -f - < scripts/queries.sql
SELECT status, count(*) FROM transfer GROUP BY status ORDER BY 2 DESC;
SELECT count(*) AS outbox_pending FROM outbox_event WHERE sent = false;
SELECT now() - min(created_at) AS oldest_pending_age FROM outbox_event WHERE sent = false;
