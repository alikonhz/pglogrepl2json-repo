package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type server struct {
	db    *pgxpool.Pool
	redis *redis.Client
}

var errOrderCacheEntryNotFound = errors.New("order cache entry not found")

type product struct {
	ID            int64     `json:"id"`
	SKU           string    `json:"sku"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	PriceCents    int       `json:"price_cents"`
	Currency      string    `json:"currency"`
	StockQuantity int       `json:"stock_quantity"`
	Active        bool      `json:"active"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type createProductRequest struct {
	SKU           string `json:"sku"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	PriceCents    int    `json:"price_cents"`
	Currency      string `json:"currency"`
	StockQuantity int    `json:"stock_quantity"`
	Active        *bool  `json:"active"`
}

type patchProductRequest struct {
	Name          *string `json:"name"`
	Description   *string `json:"description"`
	PriceCents    *int    `json:"price_cents"`
	StockQuantity *int    `json:"stock_quantity"`
	Active        *bool   `json:"active"`
}

type order struct {
	ID         int64       `json:"id"`
	CustomerID int64       `json:"customer_id"`
	Status     string      `json:"status"`
	TotalCents int         `json:"total_cents"`
	Currency   string      `json:"currency"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
	Items      []orderItem `json:"items,omitempty"`
}

type orderItem struct {
	ID             int64  `json:"id"`
	OrderID        int64  `json:"order_id"`
	ProductID      int64  `json:"product_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int    `json:"unit_price_cents"`
	LineTotalCents int    `json:"line_total_cents"`
}

type createOrderRequest struct {
	CustomerID int64 `json:"customer_id"`
	Items      []struct {
		ProductID int64 `json:"product_id"`
		Quantity  int   `json:"quantity"`
	} `json:"items"`
}

type patchOrderStatusRequest struct {
	Status         string `json:"status"`
	ExpectedStatus string `json:"expected_status"`
}

func main() {
	ctx := context.Background()
	databaseURL := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/shop?sslmode=disable")
	redisAddr := getenv("REDIS_ADDR", "localhost:16379")
	httpAddr := getenv("HTTP_ADDR", ":8080")

	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	s := &server{db: db, redis: rdb}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /products", s.listProducts)
	mux.HandleFunc("POST /products", s.createProduct)
	mux.HandleFunc("PATCH /products/{id}", s.patchProduct)
	mux.HandleFunc("POST /orders", s.createOrder)
	mux.HandleFunc("PATCH /orders/{id}/status", s.patchOrderStatus)
	mux.HandleFunc("GET /cache/products/{id}", s.cacheProduct)
	mux.HandleFunc("GET /cache/orders/{id}", s.cacheOrder)
	mux.HandleFunc("GET /cache/customers/{id}/orders", s.cacheCustomerOrders)
	mux.HandleFunc("GET /cache/keys", s.cacheKeys)

	log.Printf("shop API listening on %s", httpAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	result := map[string]string{"postgres": "ok", "redis": "ok"}
	status := http.StatusOK

	if err := s.db.Ping(ctx); err != nil {
		result["postgres"] = err.Error()
		status = http.StatusServiceUnavailable
	}
	if err := s.redis.Ping(ctx).Err(); err != nil {
		result["redis"] = err.Error()
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, result)
}

func (s *server) listProducts(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
SELECT id, sku, name, description, price_cents, currency, stock_quantity, active, updated_at
FROM products
ORDER BY id`)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	products, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (product, error) {
		return scanProduct(row)
	})
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, products)
}

func (s *server) createProduct(w http.ResponseWriter, r *http.Request) {
	var req createProductRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.SKU == "" || req.Name == "" || req.PriceCents < 0 || req.StockQuantity < 0 {
		httpError(w, http.StatusBadRequest, "sku, name, non-negative price_cents, and non-negative stock_quantity are required")
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	active := true
	if req.Active != nil {
		active = *req.Active
	}

	row := s.db.QueryRow(r.Context(), `
INSERT INTO products (sku, name, description, price_cents, currency, stock_quantity, active)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, sku, name, description, price_cents, currency, stock_quantity, active, updated_at`,
		req.SKU, req.Name, req.Description, req.PriceCents, req.Currency, req.StockQuantity, active)

	p, err := scanProduct(row)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, p)
}

func (s *server) patchProduct(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPathValue(w, r, "id")
	if !ok {
		return
	}

	var req patchProductRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.PriceCents != nil && *req.PriceCents < 0 {
		httpError(w, http.StatusBadRequest, "price_cents must be non-negative")
		return
	}
	if req.StockQuantity != nil && *req.StockQuantity < 0 {
		httpError(w, http.StatusBadRequest, "stock_quantity must be non-negative")
		return
	}

	row := s.db.QueryRow(r.Context(), `
UPDATE products
SET name = COALESCE($2, name),
    description = COALESCE($3, description),
    price_cents = COALESCE($4, price_cents),
    stock_quantity = COALESCE($5, stock_quantity),
    active = COALESCE($6, active)
WHERE id = $1
RETURNING id, sku, name, description, price_cents, currency, stock_quantity, active, updated_at`,
		id,
		nullableString(req.Name),
		nullableString(req.Description),
		nullableInt(req.PriceCents),
		nullableInt(req.StockQuantity),
		nullableBool(req.Active))

	p, err := scanProduct(row)
	if errors.Is(err, pgx.ErrNoRows) {
		httpError(w, http.StatusNotFound, "product not found")
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, p)
}

func (s *server) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.CustomerID <= 0 || len(req.Items) == 0 {
		httpError(w, http.StatusBadRequest, "customer_id and at least one item are required")
		return
	}

	ctx := r.Context()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback(ctx)

	var customerExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM customers WHERE id = $1)`, req.CustomerID).Scan(&customerExists); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !customerExists {
		httpError(w, http.StatusBadRequest, "customer does not exist")
		return
	}

	items := make([]orderItem, 0, len(req.Items))
	total := 0
	currency := "USD"

	for _, input := range req.Items {
		if input.ProductID <= 0 || input.Quantity <= 0 {
			httpError(w, http.StatusBadRequest, "product_id and positive quantity are required for every item")
			return
		}

		var sku, productCurrency string
		var price, stock int
		var active bool
		err := tx.QueryRow(ctx, `
SELECT sku, price_cents, currency, stock_quantity, active
FROM products
WHERE id = $1
FOR UPDATE`, input.ProductID).Scan(&sku, &price, &productCurrency, &stock, &active)
		if errors.Is(err, pgx.ErrNoRows) {
			httpError(w, http.StatusBadRequest, fmt.Sprintf("product %d does not exist", input.ProductID))
			return
		}
		if err != nil {
			httpError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !active {
			httpError(w, http.StatusConflict, fmt.Sprintf("product %s is inactive", sku))
			return
		}
		if stock < input.Quantity {
			httpError(w, http.StatusConflict, fmt.Sprintf("product %s has only %d item(s) left", sku, stock))
			return
		}
		if len(items) == 0 {
			currency = productCurrency
		} else if currency != productCurrency {
			httpError(w, http.StatusBadRequest, "all products in one order must use the same currency")
			return
		}

		if _, err := tx.Exec(ctx, `UPDATE products SET stock_quantity = stock_quantity - $1 WHERE id = $2`, input.Quantity, input.ProductID); err != nil {
			httpError(w, http.StatusInternalServerError, err.Error())
			return
		}

		lineTotal := price * input.Quantity
		total += lineTotal
		items = append(items, orderItem{
			ProductID:      input.ProductID,
			SKU:            sku,
			Quantity:       input.Quantity,
			UnitPriceCents: price,
			LineTotalCents: lineTotal,
		})
	}

	var created order
	err = tx.QueryRow(ctx, `
INSERT INTO orders (customer_id, status, total_cents, currency)
VALUES ($1, 'pending', $2, $3)
RETURNING id, customer_id, status::text, total_cents, currency, created_at, updated_at`,
		req.CustomerID, total, currency).Scan(
		&created.ID,
		&created.CustomerID,
		&created.Status,
		&created.TotalCents,
		&created.Currency,
		&created.CreatedAt,
		&created.UpdatedAt,
	)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	for i := range items {
		err = tx.QueryRow(ctx, `
INSERT INTO order_items (order_id, product_id, sku, quantity, unit_price_cents, line_total_cents)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, order_id`,
			created.ID,
			items[i].ProductID,
			items[i].SKU,
			items[i].Quantity,
			items[i].UnitPriceCents,
			items[i].LineTotalCents,
		).Scan(&items[i].ID, &items[i].OrderID)
		if err != nil {
			httpError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	created.Items = items
	writeJSON(w, http.StatusCreated, created)
}

func (s *server) patchOrderStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPathValue(w, r, "id")
	if !ok {
		return
	}

	var req patchOrderStatusRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if !validOrderStatus(req.Status) || !validOrderStatus(req.ExpectedStatus) {
		httpError(w, http.StatusBadRequest, "status and expected_status must be one of pending, paid, packed, shipped, cancelled")
		return
	}

	ctx := r.Context()
	var updated order
	err := s.db.QueryRow(ctx, `
UPDATE orders
SET status = $2
WHERE id = $1 AND status = $3
RETURNING id, customer_id, status::text, total_cents, currency, created_at, updated_at`,
		id, req.Status, req.ExpectedStatus).Scan(
		&updated.ID,
		&updated.CustomerID,
		&updated.Status,
		&updated.TotalCents,
		&updated.Currency,
		&updated.CreatedAt,
		&updated.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		var currentStatus string
		selectErr := s.db.QueryRow(ctx, `SELECT status::text FROM orders WHERE id = $1`, id).Scan(&currentStatus)
		if errors.Is(selectErr, pgx.ErrNoRows) {
			httpError(w, http.StatusNotFound, "order not found")
			return
		}
		if selectErr != nil {
			httpError(w, http.StatusInternalServerError, selectErr.Error())
			return
		}

		httpError(w, http.StatusConflict, fmt.Sprintf("order status is %s, expected %s", currentStatus, req.ExpectedStatus))
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, updated)
}

func (s *server) cacheProduct(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPathValue(w, r, "id")
	if !ok {
		return
	}

	ctx := r.Context()
	key := fmt.Sprintf("product:%d", id)
	hash, err := s.redis.HGetAll(ctx, key).Result()
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(hash) == 0 {
		httpError(w, http.StatusNotFound, "product cache entry not found")
		return
	}
	active, _ := s.redis.SIsMember(ctx, "products:active", strconv.FormatInt(id, 10)).Result()
	stockScore, _ := s.redis.ZScore(ctx, "products:stock", strconv.FormatInt(id, 10)).Result()

	writeJSON(w, http.StatusOK, map[string]any{
		"key":               key,
		"hash":              hash,
		"active_set_member": active,
		"stock_score":       stockScore,
	})
}

func (s *server) cacheOrder(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPathValue(w, r, "id")
	if !ok {
		return
	}

	ctx := r.Context()
	cached, err := s.readCachedOrder(ctx, strconv.FormatInt(id, 10))
	if errors.Is(err, errOrderCacheEntryNotFound) {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, cached)
}

func (s *server) cacheCustomerOrders(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPathValue(w, r, "id")
	if !ok {
		return
	}

	ctx := r.Context()
	ordersKey := fmt.Sprintf("customer:%d:orders", id)
	orderIDs, err := s.redis.SMembers(ctx, ordersKey).Result()
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	orderIDs, err = sortPositiveIntStrings(orderIDs)
	if err != nil {
		httpError(w, http.StatusInternalServerError, fmt.Sprintf("invalid order id in %s: %v", ordersKey, err))
		return
	}

	orders := make([]map[string]any, 0, len(orderIDs))
	for _, orderID := range orderIDs {
		cached, err := s.readCachedOrder(ctx, orderID)
		if errors.Is(err, errOrderCacheEntryNotFound) {
			httpError(w, http.StatusNotFound, err.Error())
			return
		}
		if err != nil {
			httpError(w, http.StatusInternalServerError, err.Error())
			return
		}
		orders = append(orders, cached)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"customer_id": id,
		"orders_key":  ordersKey,
		"order_ids":   orderIDs,
		"orders":      orders,
	})
}

func (s *server) readCachedOrder(ctx context.Context, orderID string) (map[string]any, error) {
	orderKey := fmt.Sprintf("order:%s", orderID)
	itemsKey := fmt.Sprintf("order:%s:items", orderID)
	orderHash, err := s.redis.HGetAll(ctx, orderKey).Result()
	if err != nil {
		return nil, err
	}
	if len(orderHash) == 0 {
		return nil, fmt.Errorf("%w: %s", errOrderCacheEntryNotFound, orderID)
	}
	itemsHash, err := s.redis.HGetAll(ctx, itemsKey).Result()
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"order_key": orderKey,
		"order":     orderHash,
		"items_key": itemsKey,
		"items":     itemsHash,
	}, nil
}

func sortPositiveIntStrings(values []string) ([]string, error) {
	ids := make([]int64, 0, len(values))
	for _, value := range values {
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid positive integer %q", value)
		}
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	sorted := make([]string, 0, len(ids))
	for _, id := range ids {
		sorted = append(sorted, strconv.FormatInt(id, 10))
	}

	return sorted, nil
}

func (s *server) cacheKeys(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}

	ctx := r.Context()
	keys := make([]string, 0)
	iter := s.redis.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= 200 {
			break
		}
	}
	if err := iter.Err(); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"pattern": pattern, "keys": keys})
}

type productScanner interface {
	Scan(dest ...any) error
}

func scanProduct(scanner productScanner) (product, error) {
	var p product
	err := scanner.Scan(
		&p.ID,
		&p.SKU,
		&p.Name,
		&p.Description,
		&p.PriceCents,
		&p.Currency,
		&p.StockQuantity,
		&p.Active,
		&p.UpdatedAt,
	)
	return p, err
}

func idFromPathValue(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := r.PathValue(name)
	if raw == "" {
		httpError(w, http.StatusNotFound, "not found")
		return 0, false
	}

	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		httpError(w, http.StatusBadRequest, "invalid id")
		return 0, false
	}

	return id, true
}

func decodeRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func httpError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func nullableString(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableInt(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableBool(v *bool) any {
	if v == nil {
		return nil
	}
	return *v
}

func validOrderStatus(status string) bool {
	switch status {
	case "pending", "paid", "packed", "shipped", "cancelled":
		return true
	default:
		return false
	}
}
