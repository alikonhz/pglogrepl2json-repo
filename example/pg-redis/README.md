# pg2redis E-commerce Beta Example

This example shows Postgres as the source of truth and Redis as a replicated
read model for a small shop.

The API writes to Postgres. `pg2redis` consumes logical replication and keeps
Redis hashes, sets, sorted sets, streams, and pub/sub notifications current.

## What Is Included

- Postgres schema for `customers`, `products`, `orders`, and `order_items`
- Seed data for a tiny product catalog
- Go HTTP API for product updates and order creation
- `pg2redis` config that maps database changes into Redis keys
- Docker Compose stack for Postgres, Redis, the `alikpgwalk/pg2redis` image,
  and the API

## Run It

Start the stack:

```bash
docker compose up --build
```

Compose pulls `alikpgwalk/pg2redis:0.4.2` from Docker Hub and builds only the
small example API image locally.

The API is available at `http://localhost:8080`.

If pg2redis exits with `license check failed`, the API, Postgres, and Redis can
still start, but Redis will not receive replicated data until the license value
is set and the `pg2redis` service is restarted.

## Try The Flow

List products from Postgres:

```bash
curl -s http://localhost:8080/products
```

Read the Redis projection for product `1`:

```bash
curl -s http://localhost:8080/cache/products/1
```

Change product stock in Postgres through the API:

```bash
curl -s -X PATCH \
  -H 'Content-Type: application/json' \
  -d '{"stock_quantity":37}' \
  http://localhost:8080/products/1
```

Check Redis again:

```bash
curl -s http://localhost:8080/cache/products/1
```

Create an order. This updates `orders`, `order_items`, and product inventory in
one Postgres transaction:

```bash
curl -s -X POST \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":1,"items":[{"product_id":1,"quantity":2},{"product_id":2,"quantity":1}]}' \
  http://localhost:8080/orders
```

The response includes the new order `id`. Use that returned ID in the cache and
status examples below; the commands show `1` for a freshly initialized demo
database.

Inspect order cache data:

```bash
curl -s http://localhost:8080/cache/orders/1
```

List cached orders for customer `1` from Redis:

```bash
curl -s http://localhost:8080/cache/customers/1/orders
```

Update order status with an optimistic concurrency check:

```bash
curl -s -X PATCH \
  -H 'Content-Type: application/json' \
  -d '{"expected_status":"pending","status":"paid"}' \
  http://localhost:8080/orders/1/status
```

Look directly at Redis:

```bash
docker compose exec redis redis-cli HGETALL product:1
docker compose exec redis redis-cli ZRANGE products:stock 0 -1 WITHSCORES
docker compose exec redis redis-cli XRANGE order_events - +
```

## Persisted Volumes

The demo uses named Docker volumes for both data stores:

- `pgdata` stores Postgres data, including shop tables, the publication, the
  replication slot, and `pgwalk.snapshot_state`.
- `redisdata` stores Redis data under `/data`, including pg2redis projections
  and the saved snapshot LSN.

Regular restarts keep both stores:

```bash
docker compose down
docker compose up --build
```

This keeps Postgres snapshot state and Redis projections in sync across demo
restarts. To start from an empty database and empty Redis cache, remove the
volumes too:

Because `pgdata` persists the `orders` table and its sequence, order IDs keep
increasing across restarts. Removing volumes resets the demo data, including
order IDs.

```bash
docker compose down -v
docker compose up --build
```

## Key Redis Projections

- `product:{id}`: product row as a Redis hash
- `products:all`: set of all product IDs
- `products:active`: set of active product IDs
- `products:stock`: sorted set scored by `stock_quantity`
- `customer:{id}`: customer row as a Redis hash
- `customer:{id}:orders`: set of a customer's order IDs
- `order:{id}`: order row as a Redis hash
- `order:{id}:items`: order item JSON values keyed by item ID
- `order_events`: Redis stream for order inserts and status updates
- `order_updates`: pub/sub channel for order changes
