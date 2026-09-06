ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET track_commit_timestamp = on;
ALTER SYSTEM SET max_replication_slots = 100;