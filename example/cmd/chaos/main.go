package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func main() {

	gofakeit.Seed(0)

	host, port, db, user, password := getConnectionParams()

	pgConnStr := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s", user, password, host, port, db)
	pgConn, err := pgxpool.New(context.Background(), pgConnStr)

	if err != nil {
		panic(err)
	}

	defer pgConn.Close()

	if slices.Contains(os.Args, "--create") {
		if err := createTablesIfNotExist(pgConn); err != nil {
			panic(err)
		}

		if err := createSlotAndPub(pgConn); err != nil {
			panic(err)
		}

		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		envVal := os.Getenv("CHAOS_INTERVAL")
		if envVal == "" {
			envVal = "5000"
		}

		var milli int

		milliSec, err := strconv.Atoi(envVal)
		if err == nil {
			milli = milliSec
		} else {
			milli = 5000
		}

		for {
			if ctx.Err() != nil {
				fmt.Println("context canceled")
			}

			chaos(pgConn)

			time.Sleep(time.Millisecond * time.Duration(milli))
		}
	}()

	<-ctx.Done()

	//iteration := 0
	//var maxTxID uint32
	//for {
	//	iteration++
	//
	//	if iteration == 500 {
	//		break
	//	}
	//
	//	fmt.Printf("starting iteration %d\n", iteration)
	//	const maxConn = 20
	//	var wg sync.WaitGroup
	//	wg.Add(maxConn)
	//	for i := 0; i < maxConn; i++ {
	//		i := i
	//		go func() {
	//			defer wg.Done()
	//			err := insertData(pgConn, iteration, i)
	//			if err != nil {
	//				fmt.Printf("iteration %d, step %d, error: %v\n", iteration, i, err)
	//			}
	//		}()
	//	}
	//
	//	wg.Wait()
	//
	//	txId, _ := readTxID(pgConn)
	//	maxTxID = max(maxTxID, txId)
	//
	//	maxTxID -= 1
	//	fmt.Printf("done iteration %d at txid %d\n", iteration, txId)
	//}
	//
	//fmt.Printf("max txid: %d\n", maxTxID)
}

type Customer struct {
	ID           int64
	CustomerID   string
	RegisterDate time.Time
	FullName     string
	Email        string
}

type CustomerEvent struct {
	ID         int64
	CustomerID string
	Kind       string
	EventDate  time.Time
	FieldName  *string
	ValueFrom  *string
	ValueTo    *string
}

type OrderStatus string

const (
	OrderStatusNew       = "new"
	OrderStatusPaid      = "paid"
	OrderStatusSent      = "sent"
	OrderStatusDelivered = "delivered"
	OrderStatusCompleted = "completed"
	OrderStatusCancelled = "cancelled"
)

type Order struct {
	ID          int64
	OrderID     string
	CustomerID  string
	OrderDate   time.Time
	OrderStatus OrderStatus
}

type OrderEvent struct {
	ID        int64
	OrderID   string
	Kind      string
	EventDate time.Time
	FieldName *string
	ValueFrom *string
	ValueTo   *string
}

func chaos(conn *pgxpool.Pool) {
	fmt.Println("starting chaos")

	customers, err := readCustomers(conn)
	if err != nil {
		panic(err)
	}

	//if len(customers) > 0 {
	//	err = createOrdersForAllCustomers(conn, customers)
	//	if err != nil {
	//		panic(err)
	//	}
	//
	//	return
	//}

	fmt.Printf("customers in DB: %d\n", len(customers))

	customers, err = createOrUpdateCustomers(conn, customers)

	if err != nil {
		panic(err)
	}

	fmt.Printf("customers: %d\n", len(customers))

	err = createOrUpdateOrders(conn, customers)
	if err != nil {
		panic(err)
	}

	fmt.Println("chaos done")
}

func createTenOrdersForCustomers(ctx context.Context, conn *pgxpool.Pool, customers []*Customer) error {
	for _, customer := range customers {
		if err := createTenOrdersForCustomer(ctx, conn, customer); err != nil {
			return err
		}
	}

	return nil
}

func createTenOrdersForCustomer(ctx context.Context, conn *pgxpool.Pool, customer *Customer) error {
	fmt.Printf("creating 10 orders for customer %s\n", customer.CustomerID)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	for i := 0; i < 10; i++ {
		fmt.Printf("creating order %d for customer %s\n", i+1, customer.CustomerID)

		orderID, err := uuid.NewUUID()
		if err != nil {
			return fmt.Errorf("failed to create order ID: %w", err)
		}

		order := &Order{
			ID:          0,
			OrderID:     orderID.String(),
			CustomerID:  customer.CustomerID,
			OrderDate:   time.Now(),
			OrderStatus: OrderStatusNew,
		}

		row := tx.QueryRow(ctx,
			"insert into chaos.orders(order_id, customer_id, order_date, order_status) values ($1, $2, $3, $4) returning id",
			order.OrderID, order.CustomerID, order.OrderDate, order.OrderStatus)

		if err = row.Scan(&order.ID); err != nil {
			return fmt.Errorf("failed to insert new order: %w", err)
		}

		event := &OrderEvent{
			ID:        order.ID,
			OrderID:   order.OrderID,
			Kind:      "insert",
			EventDate: time.Now(),
			FieldName: nil,
			ValueFrom: nil,
			ValueTo:   nil,
		}

		if err = saveOrderEvent(ctx, tx, event); err != nil {
			return fmt.Errorf("failed to save order event: %w", err)
		}

		fmt.Printf("created order %d with ID %s for customer %s\n", i+1, order.OrderID, customer.CustomerID)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("done creating 10 orders for customer %s\n", customer.CustomerID)

	return nil
}

func createOrdersForAllCustomers(conn *pgxpool.Pool, customers []*Customer) error {
	var batches [][]*Customer
	var maxBatchSize = len(customers) / 4
	var currentBatch []*Customer

	for _, customer := range customers {
		currentBatch = append(currentBatch, customer)
		if len(currentBatch) == maxBatchSize {
			batches = append(batches, currentBatch)
			currentBatch = nil
		}
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	errGroup, ctx := errgroup.WithContext(context.Background())
	for _, batch := range batches {
		b := batch
		errGroup.Go(func() error {
			return createTenOrdersForCustomers(ctx, conn, b)
		})
	}

	if err := errGroup.Wait(); err != nil {
		return err
	}

	return nil
}

func createOrUpdateOrders(conn *pgxpool.Pool, customers []*Customer) error {
	errGroup, ctx := errgroup.WithContext(context.Background())
	errGroup.Go(func() error {
		return customersLoop(ctx, conn, customers, 0)
	})

	errGroup.Go(func() error {
		return customersLoop(ctx, conn, customers, 1)
	})

	if err := errGroup.Wait(); err != nil {
		return err
	}

	return nil
}

func customersLoop(ctx context.Context, conn *pgxpool.Pool, customers []*Customer, startIndex int) error {
	for i := startIndex; i < len(customers); i += 2 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := workWithOrder(ctx, conn, customers[i]); err != nil {
			return err
		}
	}

	return nil
}

func workWithOrder(ctx context.Context, conn *pgxpool.Pool, customer *Customer) error {
	orders, err := readOrders(ctx, conn, customer.CustomerID)
	if err != nil {
		return fmt.Errorf("failed to read orders: %w", err)
	}

	action := rand.Intn(10)
	switch action {
	case 0:
	case 1:
	case 2:
		// create order
		return saveNewOrder(ctx, conn, customer)
	case 9:
		// delete order
		if len(orders) == 0 {
			return saveNewOrder(ctx, conn, customer)
		}

		return deleteOrder(ctx, conn, orders[0])
	default:
		// update order
		if len(orders) == 0 {
			return saveNewOrder(ctx, conn, customer)
		}

		return updateNextOrder(ctx, conn, orders, customer)
	}

	return nil
}

func updateNextOrder(ctx context.Context, conn *pgxpool.Pool, orders []*Order, customer *Customer) error {
	tx, err := conn.Begin(ctx)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	var orderToUpdate *Order

	for _, order := range orders {
		if order.OrderStatus == OrderStatusCancelled || order.OrderStatus == OrderStatusCompleted {
			continue
		}

		orderToUpdate = order

		break
	}

	if orderToUpdate == nil {
		return saveNewOrder(ctx, conn, customer)
	}

	nextStatus := getNextOrderStatus(orderToUpdate.OrderStatus)

	_, err = tx.Exec(ctx, "update chaos.orders set order_status = $1 where order_id = $2",
		nextStatus, orderToUpdate.OrderID)

	if err != nil {
		return fmt.Errorf("failed to update order: %w", err)
	}

	fn, vf, vt := "order_status", string(orderToUpdate.OrderStatus), string(nextStatus)

	event := &OrderEvent{
		ID:        orderToUpdate.ID,
		OrderID:   orderToUpdate.OrderID,
		Kind:      "update",
		EventDate: time.Now(),
		FieldName: &fn,
		ValueFrom: &vf,
		ValueTo:   &vt,
	}

	if err = saveOrderEvent(ctx, tx, event); err != nil {
		return fmt.Errorf("failed to save order event: %w", err)
	}

	err = tx.Commit(ctx)

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func getNextOrderStatus(status OrderStatus) OrderStatus {
	r := rand.Intn(10)

	// 10% chance of order to be canceled
	if r == 9 {
		return OrderStatusCancelled
	}

	switch status {
	case OrderStatusNew:
		return OrderStatusPaid
	case OrderStatusPaid:
		return OrderStatusSent
	case OrderStatusSent:
		return OrderStatusDelivered
	case OrderStatusDelivered:
		return OrderStatusCompleted
	}

	return OrderStatusCancelled
}

func deleteOrder(ctx context.Context, conn *pgxpool.Pool, order *Order) error {
	fmt.Printf("deleting order %s from customer %s\n", order.OrderID, order.CustomerID)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "delete from chaos.orders where order_id = $1", order.OrderID)

	event := &OrderEvent{
		ID:        order.ID,
		OrderID:   order.OrderID,
		Kind:      "delete",
		EventDate: time.Now(),
		FieldName: nil,
		ValueFrom: nil,
		ValueTo:   nil,
	}

	if err = saveOrderEvent(ctx, tx, event); err != nil {
		return fmt.Errorf("failed to save order event: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("deleting order %s from customer %s\n", order.OrderID, order.CustomerID)

	return nil
}

func saveNewOrder(ctx context.Context, conn *pgxpool.Pool, customer *Customer) error {
	fmt.Printf("creating new order for customer %s\n", customer.CustomerID)

	orderID, err := uuid.NewUUID()
	if err != nil {
		return fmt.Errorf("failed to create order ID: %w", err)
	}

	order := &Order{
		ID:          0,
		OrderID:     orderID.String(),
		CustomerID:  customer.CustomerID,
		OrderDate:   time.Now(),
		OrderStatus: OrderStatusNew,
	}

	tx, err := conn.Begin(ctx)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, "insert into chaos.orders(order_id, customer_id, order_date, order_status) values ($1, $2, $3, $4) returning id",
		order.OrderID, order.CustomerID, order.OrderDate, order.OrderStatus)

	if err = row.Scan(&order.ID); err != nil {
		return fmt.Errorf("failed to insert new order: %w", err)
	}

	orderEvent := &OrderEvent{
		ID:        order.ID,
		OrderID:   order.OrderID,
		Kind:      "insert",
		EventDate: time.Now(),
		FieldName: nil,
		ValueFrom: nil,
		ValueTo:   nil,
	}

	if err = saveOrderEvent(ctx, tx, orderEvent); err != nil {
		return fmt.Errorf("failed to save order event: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("done creating new order for customer %s with ID %s\n", customer.CustomerID, order.OrderID)

	return nil
}

func saveOrderEvent(ctx context.Context, tx pgx.Tx, event *OrderEvent) error {
	_, err := tx.Exec(ctx,
		`
			insert into chaos.orders_events(id, order_id, kind, event_date, field_name, from_value, to_value) values ($1, $2, $3, $4, $5, $6, $7)
			`,
		event.ID, event.OrderID, event.Kind, event.EventDate, event.FieldName, event.ValueFrom, event.ValueTo)

	return err
}

func readOrders(ctx context.Context, conn *pgxpool.Pool, customerID string) ([]*Order, error) {
	rows, _ := conn.Query(ctx, "select id, order_id, customer_id, order_date, order_status from chaos.orders where customer_id = $1",
		customerID)
	return pgx.CollectRows[*Order](rows, func(row pgx.CollectableRow) (*Order, error) {
		var order Order
		err := row.Scan(&order.ID, &order.OrderID, &order.CustomerID, &order.OrderDate, &order.OrderStatus)

		if err != nil {
			return nil, err
		}

		return &order, nil
	})
}

func createOrUpdateCustomers(conn *pgxpool.Pool, customers []*Customer) ([]*Customer, error) {
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return nil, err
	}

	defer tx.Rollback(context.Background())

	const customersCount = 100

	var newCustomers []*Customer

	if len(customers) < customersCount {
		nc, err := createCustomers(tx, customersCount-len(customers))
		if err != nil {
			return nil, err
		}

		newCustomers = nc
	}

	customers, err = updateCustomers(tx, customers)
	if err != nil {
		return nil, err
	}

	allCustomers := append(customers, newCustomers...)

	tx.Commit(context.Background())

	return allCustomers, nil
}

func updateCustomers(tx pgx.Tx, customers []*Customer) ([]*Customer, error) {
	if len(customers) == 0 {
		return customers, nil
	}

	const percent = 15

	var percentCount = len(customers) * percent / 100
	randIndexes := rand.Perm(len(customers))[:percentCount]

	fmt.Printf("updating %d customers\n", percentCount)

	for _, index := range randIndexes {
		customer := customers[index]
		if err := updateCustomer(tx, customer); err != nil {
			return nil, err
		}
	}

	fmt.Printf("done updating %d customers\n", percentCount)

	return customers, nil
}

func updateCustomer(tx pgx.Tx, customer *Customer) error {
	r := rand.Intn(2)

	var (
		field     string
		valueFrom string
		valueTo   string
	)

	if r == 0 {
		field = "full_name"
		valueFrom = customer.FullName
		valueTo = gofakeit.Name()
		customer.FullName = valueTo
	} else if r == 1 {
		field = "email"
		valueFrom = customer.Email
		valueTo = gofakeit.Email()
		customer.Email = valueTo
	}

	if _, err := tx.Exec(context.Background(),
		`update chaos.customers set full_name = $1, email = $2 where customer_id = $3`,
		customer.FullName, customer.Email, customer.CustomerID); err != nil {
		return err
	}

	event := &CustomerEvent{
		ID:         customer.ID,
		CustomerID: customer.CustomerID,
		Kind:       "update",
		EventDate:  time.Now(),
		FieldName:  &field,
		ValueFrom:  &valueFrom,
		ValueTo:    &valueTo,
	}

	if err := saveCustomerEvent(tx, event); err != nil {
		return err
	}

	return nil
}

func createCustomers(tx pgx.Tx, count int) ([]*Customer, error) {
	newCustomers := make([]*Customer, count)

	fmt.Printf("creating %d customers\n", count)

	for i := 0; i < count; i++ {
		custID, err := uuid.NewUUID()
		if err != nil {
			return nil, err
		}

		customer := &Customer{
			ID:           0,
			CustomerID:   custID.String(),
			RegisterDate: time.Now(),
			FullName:     gofakeit.Name(),
			Email:        gofakeit.Email(),
		}

		r := tx.QueryRow(context.Background(),
			`
				insert into chaos.customers(customer_id, register_date, full_name, email)
				values ($1, $2, $3, $4)
				returning id
				`, customer.CustomerID, customer.RegisterDate, customer.FullName, customer.Email)

		if err = r.Scan(&customer.ID); err != nil {
			return nil, err
		}

		event := &CustomerEvent{
			ID:         customer.ID,
			CustomerID: customer.CustomerID,
			Kind:       "insert",
			EventDate:  time.Now(),
			FieldName:  nil,
			ValueFrom:  nil,
			ValueTo:    nil,
		}

		if err = saveCustomerEvent(tx, event); err != nil {
			return nil, err
		}

		newCustomers[i] = customer
	}

	fmt.Printf("done creating %d customers\n", count)

	return newCustomers, nil
}

func saveCustomerEvent(tx pgx.Tx, event *CustomerEvent) error {
	_, err := tx.Exec(context.Background(),
		`
			insert into chaos.customers_events(id, customer_id, kind, event_date, field_name, value_from, value_to) values ($1, $2, $3, $4, $5, $6, $7)
			`,
		event.ID, event.CustomerID, event.Kind, event.EventDate, event.FieldName, event.ValueFrom, event.ValueTo)
	return err
}

func readCustomers(conn *pgxpool.Pool) ([]*Customer, error) {
	rows, _ := conn.Query(context.Background(), "select id, customer_id, register_date, full_name, email from chaos.customers")

	return pgx.CollectRows[*Customer](rows, func(row pgx.CollectableRow) (*Customer, error) {
		var customer Customer

		err := row.Scan(&customer.ID, &customer.CustomerID, &customer.RegisterDate, &customer.FullName, &customer.Email)
		if err != nil {
			return nil, err
		}

		return &customer, nil
	})
}

func createSlotAndPub(conn *pgxpool.Pool) error {
	_, _ = conn.Exec(context.Background(), `DROP PUBLICATION chaos_pub`)
	_, _ = conn.Exec(context.Background(), `select pg_drop_replication_slot('chaos_slot')`)

	_, err := conn.Exec(context.Background(),
		`
	CREATE PUBLICATION chaos_pub FOR TABLE chaos.customers, chaos.orders;
	`)

	if err != nil {
		return err
	}

	_, err = conn.Exec(context.Background(),
		`select pg_create_logical_replication_slot('chaos_slot', 'pgoutput')`)

	return err
}

func createTablesIfNotExist(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.Background(),
		`
DO $$
BEGIN

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'orderstatus') THEN
		CREATE TYPE orderstatus AS ENUM ('new', 'paid', 'sent', 'delivered', 'completed', 'cancelled');
	END IF;

	CREATE SCHEMA IF NOT EXISTS chaos;

	DROP TABLE IF EXISTS chaos.customers;
	DROP TABLE IF EXISTS chaos.customers_rec;
	DROP TABLE IF EXISTS chaos.customers_events;
	DROP TABLE IF EXISTS chaos.customers_events_rec;
	DROP TABLE IF EXISTS chaos.orders;
	DROP TABLE IF EXISTS chaos.orders_rec;
	DROP TABLE IF EXISTS chaos.orders_events;
	DROP TABLE IF EXISTS chaos.orders_events_rec;

	CREATE TABLE IF NOT EXISTS chaos.customers 
	(
	    id bigserial not null primary key, 
	    customer_id uuid not null unique,
	    register_date timestamp not null default now(),
	    full_name text not null,
		email text not null
	);

	CREATE TABLE IF NOT EXISTS chaos.customers_rec 
	(
		table_id bigserial not null primary key,
	    id bigint not null, 
	    customer_id uuid not null unique,
	    register_date timestamp not null,
	    full_name text not null,
		email text not null
	);

	CREATE TABLE IF NOT EXISTS chaos.customers_events
	(
	    id bigint not null,
	    customer_id uuid not null,
	    kind text not null,
	    event_date timestamp not null,
		field_name text null,
		value_from text null,
		value_to text null
	);

	CREATE TABLE IF NOT EXISTS chaos.customers_events_rec
	(
	    id bigint not null,
	    customer_id uuid not null,
	    kind text not null,
	    event_date timestamp not null,
		event_payload text not null
	);

	CREATE TABLE IF NOT EXISTS chaos.orders
	(
	  	id bigserial not null primary key,
		order_id uuid not null unique,
		customer_id uuid not null,
		order_date timestamp not null,
		order_status orderstatus not null
	);

	CREATE TABLE IF NOT EXISTS chaos.orders_rec
	(
		table_id bigserial not null primary key,
	  	id bigint not null,
		order_id uuid not null unique,
		customer_id uuid not null,
		order_date timestamp not null,
		order_status orderstatus not null
	);

	CREATE TABLE IF NOT EXISTS chaos.orders_events
	(
		id bigint not null,
		order_id uuid not null,
		kind text not null,
		event_date timestamp not null,
		field_name text null,
		from_value text null,
		to_value text null
	);

	CREATE TABLE IF NOT EXISTS chaos.orders_events_rec
	(
		id bigint not null,
		order_id uuid null,
		kind text not null,
		event_date timestamp not null,
		event_payload text not null
	);

END $$;
	`)

	return err
}

func getConnectionParams() (host, port, db, user, password string) {
	host, port, db, user, password = os.Getenv("PGHOST"), os.Getenv("PGPORT"), os.Getenv("PGDB"), os.Getenv("PGUSER"), os.Getenv("PGPASSWORD")
	if host == "" {
		fmt.Println("PGHOST is not set")
		os.Exit(1)
	}

	if port == "" {
		fmt.Println("PGPORT is not set")
		os.Exit(1)
	}

	if db == "" {
		fmt.Println("PGDB is not set")
		os.Exit(1)
	}

	if user == "" {
		fmt.Println("PGUSER is not set")
		os.Exit(1)
	}

	if password == "" {
		fmt.Println("PGPASSWORD is not set")
		os.Exit(1)
	}

	return
}

func readTxID(conn *pgxpool.Pool) (uint32, error) {
	res := conn.QueryRow(context.Background(), "select txid_current()")
	var txID uint32
	err := res.Scan(&txID)
	if err != nil {
		return 0, err
	}

	return txID, nil
}

func insertData(conn *pgxpool.Pool, iter, i int) error {
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return err
	}

	// see the docs on Rollback
	defer tx.Rollback(context.Background())

	row := tx.QueryRow(context.Background(),
		//"insert into customers (customerid, firstname, lastname) values (gen_random_uuid (), $1, $2) returning customerid",
		"insert into customers (val, reading) values ($1, $2) returning id",
		fmt.Sprintf("first %d %d", iter, i),
		i > 0)

	var customerID any
	err = row.Scan(&customerID)
	if err != nil {
		return err
	}

	//for orderID := 0; orderID < 3; orderID++ {
	//	_, err := tx.Exec(context.Background(),
	//		"insert into orders(orderid, orderdate, status, customerid) values (gen_random_uuid(), current_timestamp, 'NEW', $1)",
	//		customerID)
	//	if err != nil {
	//		return err
	//	}
	//}

	return tx.Commit(context.Background())
}

//func main1() {
//	integrationtest.MustLoad(".env")
//
//	connString, replConnString, slotName, pubName := readEnv()
//
//	log.Printf("slotName: %q, publication name: %q\n", slotName, pubName)
//
//	pool, err := pgxpool.New(context.Background(), connString)
//	if err != nil {
//		log.Fatalln("unable to connect to postgres: ", err)
//	}
//	err = createTableSlotAndPub(pool, slotName, pubName)
//	defer pool.Close()
//
//	opts := replicator.NewOptions(slotName, pubName, 15*time.Second, 65*time.Second)
//	outCh := make(chan []byte)
//	listener := repl2json.MustCreateNewJson(outCh, repl2json.ListenerJSONOptions{})
//	repl := replicator.MustCreate(opts, listener)
//	doneChan := make(chan struct{})
//
//	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
//	go writeToTable(ctx, pool)
//	go printJson(ctx, outCh, doneChan)
//	defer cancel()
//	err = repl.Start(ctx, doneChan)
//	if err != nil {
//		log.Fatalln(err)
//	}
//
//	<-ctx.Done()
//	<-doneChan
//}

func writeToTable(ctx context.Context, pool *pgxpool.Pool) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			insert(ctx, pool)
		}
	}
}

func insert(ctx context.Context, pool *pgxpool.Pool) {
	data := make([]byte, 1024)
	rand.Read(data)
	text := base64.StdEncoding.EncodeToString(data)
	_, err := pool.Exec(ctx, "insert into test_pub_table (data) values ($1)", text)
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}

func createTableSlotAndPub(conn *pgxpool.Pool, slotName string, pubName string) error {
	_, err := conn.Exec(context.Background(), "CREATE TABLE test_pub_table (id bigserial, data text)")
	if err != nil {
		return fmt.Errorf("unable to create table: %w", err)
	}
	_, err = conn.Exec(context.Background(), "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName)
	if err != nil {
		return fmt.Errorf("unable to create slot: %w", err)
	}
	_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE test_pub_table", pubName))
	if err != nil {
		return fmt.Errorf("unable to create publication: %w", err)
	}
	return nil
}

func printJson(ctx context.Context, ch <-chan []byte, doneChan chan struct{}) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case bytes := <-ch:
			fmt.Println(string(bytes))
		}
	}

	doneChan <- struct{}{}
}

func readEnv() (connString, replConnString, slotName, pubName string) {
	connString = os.Getenv("PGLOG2JSON_CONN_STRING")
	if connString == "" {
		log.Fatalln("PGLOG2JSON_CONN_STRING is not set")
	}
	const replSuffix = "?replication=database"
	if strings.HasSuffix(connString, replSuffix) {
		replConnString = connString
		connString = strings.TrimSuffix(connString, replSuffix)
	} else {
		replConnString = connString + replSuffix
	}
	slotName = os.Getenv("PGLOG2JSON_SLOT_NAME")
	if slotName == "" {
		slotName = "pglog2json_default_slot"
	}
	pubName = os.Getenv("PGLOG2JSON_PUB_NAME")
	if pubName == "" {
		pubName = slotName + "_pub"
	}
	return connString, replConnString, slotName, pubName
}
