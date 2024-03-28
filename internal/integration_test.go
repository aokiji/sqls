package handler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/sourcegraph/jsonrpc2"

	"github.com/sqls-server/sqls/internal/database"
	"github.com/sqls-server/sqls/internal/handler"
	"github.com/sqls-server/sqls/internal/lsp"
)

type TestContext struct {
	h              jsonrpc2.Handler
	conn           *jsonrpc2.Conn
	connServer     *jsonrpc2.Conn
	server         *handler.Server
	ctx            context.Context
	test           *testing.T
	dbConfig       *database.DBConfig
	sequence       int
}

func newTestContext(t *testing.T, dbConfig *database.DBConfig) *TestContext {
	server := handler.NewServer()
	handler := jsonrpc2.HandlerWithError(server.Handle)
	ctx := context.Background()
	return &TestContext{
		h:      handler,
		ctx:    ctx,
		server: server,
		test:   t,
		dbConfig: dbConfig,
	}
}

func (tx *TestContext) setup() {
	tx.test.Helper()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	tx.initServer()
	tx.waitServerReady()
}

func (tx *TestContext) tearDown() {
	if tx.conn != nil {
		if err := tx.conn.Close(); err != nil {
			tx.test.Fatal("conn.Close:", err)
		}
	}

	if tx.connServer != nil {
		if err := tx.connServer.Close(); err != nil {
			if !errors.Is(err, jsonrpc2.ErrClosed) {
				tx.test.Fatal("connServer.Close:", err)
			}
		}
	}
}

func (tx *TestContext) initServer() {
	tx.test.Helper()

	// Prepare the server and client connection.
	client, server := net.Pipe()
	tx.connServer = jsonrpc2.NewConn(tx.ctx, jsonrpc2.NewBufferedStream(server, jsonrpc2.VSCodeObjectCodec{}), tx.h)
	tx.conn = jsonrpc2.NewConn(tx.ctx, jsonrpc2.NewBufferedStream(client, jsonrpc2.VSCodeObjectCodec{}), tx.h)

	// Initialize Language Server
	params := lsp.InitializeParams{
		InitializationOptions: lsp.InitializeOptions{
			ConnectionConfig: tx.dbConfig,
		},
	}
	if err := tx.conn.Call(tx.ctx, "initialize", params, nil); err != nil {
		tx.test.Fatal("conn.Call initialize:", err)
	}
}

func (tx *TestContext) waitServerReady() {
	tx.test.Helper()

	timeToWait := 100 * time.Millisecond
	tries := 10
	isUpdated := false
	for i := 0; i < tries && !isUpdated; i++ {
		isUpdated = tx.server.UpdateCompleted()
		time.Sleep(timeToWait)
		tx.test.Logf("Server is not yet fully updated, waiting %s", timeToWait)
	}

	if !isUpdated {
		tx.test.Fatal("Timeout waiting for server to be fully updated")
	}
}

func (tx *TestContext) generateFileURI() string {
	tx.sequence += 1
	uri := fmt.Sprintf("file:///home/user/file%d.sql", tx.sequence)
	return uri
}

func (tx *TestContext) setFileText(fileURI, openText string) {
	tx.test.Helper()

	didOpenParams := lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        fileURI,
			LanguageID: "sql",
			Version:    0,
			Text:       openText,
		},
	}
	if err := tx.conn.Call(tx.ctx, "textDocument/didOpen", didOpenParams, nil); err != nil {
		tx.test.Fatal("conn.Call textDocument/didOpen:", err)
	}
}

func (tx *TestContext) requestCompletionAt(uri string, position lsp.Position) []lsp.CompletionItem {
	completionParams := lsp.CompletionParams{
		CompletionContext: lsp.CompletionContext{TriggerKind: 1},
		TextDocumentPositionParams: lsp.TextDocumentPositionParams{
			Position:     position,
			TextDocument: lsp.TextDocumentIdentifier{URI: uri}},
	}

	var completionItems []lsp.CompletionItem
	if err := tx.conn.Call(tx.ctx, "textDocument/completion", completionParams, &completionItems); err != nil {
		tx.test.Fatal("conn.Call initialize:", err)
	}

	return completionItems
}


func ensureDatabaseReadyOrSkip(t *testing.T, envVariable string) string {
	t.Helper()
	value, ok := os.LookupEnv(envVariable)
	if !ok {
		t.Skip("No database available for integration test")
	}
	return value
}

func TestCompletionIntegration(t *testing.T) {
	t.Helper()

	t.Run("mysql 57", func(t *testing.T) {
		databaseSource := ensureDatabaseReadyOrSkip(t, "SQLS_TEST_MYSQL57_SOURCE")
		completionIntegrationTest(t, &database.DBConfig{DataSourceName: databaseSource, Driver: "mysql"})
	})

	t.Run("postgres 12", func(t *testing.T) {
		databaseSource := ensureDatabaseReadyOrSkip(t, "SQLS_TEST_POSTGRES12_SOURCE")
		completionIntegrationTest(t, &database.DBConfig{DataSourceName: databaseSource, Driver: "postgresql"})
	})
}

func completionIntegrationTest(t *testing.T, dbConfig *database.DBConfig ) {
	tx := newTestContext(t, dbConfig)
	tx.setup()
	t.Cleanup(func() { tx.tearDown() })

	t.Run("Simple table completion", func(t *testing.T) {
		uri := tx.generateFileURI()
		t.Parallel()

		tx.setFileText(uri, "SELECT * FROM client ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(uri, lsp.Position{Character: 20, Line: 0})

		expectToFindCompletionItemWithLabel(t, "clients", completionItems)
		expectToFindCompletionItemWithLabel(t, "client_types", completionItems)
		expectToFindCompletionItemWithLabel(t, "extra.client_custom_info", completionItems)
	})

	t.Run("Simple column completion", func(t *testing.T) {
		uri := tx.generateFileURI()
		t.Parallel()

		tx.setFileText(uri, "SELECT  FROM clients ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(uri, lsp.Position{Character: 7, Line: 0})

		expectToFindCompletionItemWithLabel(t, "id", completionItems)
		expectToFindCompletionItemWithLabel(t, "name", completionItems)
		expectToFindCompletionItemWithLabel(t, "type_id", completionItems)
	})

	t.Run("Column completion on duplicated table in other schema", func(t *testing.T) {
		uri := tx.generateFileURI()
		t.Parallel()

		tx.setFileText(uri, "SELECT  FROM extra.clients ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(uri, lsp.Position{Character: 7, Line: 0})

		expectToFindCompletionItemWithLabel(t, "id", completionItems)
		expectToFindCompletionItemWithLabel(t, "name", completionItems)
		expectToFindCompletionItemWithLabel(t, "type_id", completionItems)
		expectToFindCompletionItemWithLabel(t, "extra_data_field", completionItems)
	})

	t.Run("Join completion using given table foreign key", func(t *testing.T) {
		uri := tx.generateFileURI()
		t.Parallel()

		tx.setFileText(uri, "SELECT * FROM clients join ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(uri, lsp.Position{Character: 27, Line: 0})

		expectToFindCompletionItemWithLabel(t, "client_types c1 ON c1.id = clients.type_id", completionItems)
	})
}

func expectToFindCompletionItemWithLabel(t *testing.T, lookupLabel string, completionItems []lsp.CompletionItem) *lsp.CompletionItem {
	var foundCompletionItem *lsp.CompletionItem
	for _, completionItem := range completionItems {
		if completionItem.Label == lookupLabel {
			foundCompletionItem = &completionItem
		}
	}

	if foundCompletionItem == nil {
		t.Errorf("Expected to find a completion item with label %s", lookupLabel)
		return nil
	}

	return foundCompletionItem
}
