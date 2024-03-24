package handler

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"testing"

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
	uri            string
	test           *testing.T
	databaseSource string
}

func newTestContext(t *testing.T) *TestContext {
	server := handler.NewServer()
	handler := jsonrpc2.HandlerWithError(server.Handle)
	ctx := context.Background()
	return &TestContext{
		h:      handler,
		ctx:    ctx,
		server: server,
		uri:    "file:///home/user/file.sql",
		test:   t,
	}
}

func (tx *TestContext) setup() {
	tx.test.Helper()
	tx.ensureDatabaseReadyOrSkip()
	tx.initServer()
}

func (tx *TestContext) tearDown() {
	if tx.conn != nil {
		if err := tx.conn.Close(); err != nil {
			log.Fatal("conn.Close:", err)
		}
	}

	if tx.connServer != nil {
		if err := tx.connServer.Close(); err != nil {
			if !errors.Is(err, jsonrpc2.ErrClosed) {
				log.Fatal("connServer.Close:", err)
			}
		}
	}
}

func (tx *TestContext) ensureDatabaseReadyOrSkip() {
	tx.test.Helper()
	value, ok := os.LookupEnv("SQLS_TEST_POSTGRES_SOURCE")
	if !ok {
		tx.test.Skip("No database available for integration test")
	}
	tx.databaseSource = value
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
			ConnectionConfig: &database.DBConfig{Driver: "postgresql", DataSourceName: tx.databaseSource},
		},
	}
	if err := tx.conn.Call(tx.ctx, "initialize", params, nil); err != nil {
		tx.test.Fatal("conn.Call initialize:", err)
	}
}

func (tx *TestContext) setFileText(openText string) {
	tx.test.Helper()

	didOpenParams := lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        tx.uri,
			LanguageID: "sql",
			Version:    0,
			Text:       openText,
		},
	}
	if err := tx.conn.Call(tx.ctx, "textDocument/didOpen", didOpenParams, nil); err != nil {
		tx.test.Fatal("conn.Call textDocument/didOpen:", err)
	}
}

func (tx *TestContext) requestCompletionAt(position lsp.Position) []lsp.CompletionItem {
	completionParams := lsp.CompletionParams{
		CompletionContext: lsp.CompletionContext{TriggerKind: 1},
		TextDocumentPositionParams: lsp.TextDocumentPositionParams{
			Position:     position,
			TextDocument: lsp.TextDocumentIdentifier{URI: tx.uri}},
	}

	var completionItems []lsp.CompletionItem
	if err := tx.conn.Call(tx.ctx, "textDocument/completion", completionParams, &completionItems); err != nil {
		tx.test.Fatal("conn.Call initialize:", err)
	}

	return completionItems
}

func TestCompletionIntegration(t *testing.T) {
	tx := newTestContext(t)
	tx.setup()
	t.Cleanup(func() { tx.tearDown() })

	t.Run("Simple table completion", func(t *testing.T) {
		tx.setFileText("SELECT * FROM client ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(lsp.Position{Character: 20, Line: 0})

		expectToFindCompletionItemWithLabel(t, "clients", completionItems)
		expectToFindCompletionItemWithLabel(t, "client_types", completionItems)
		expectToFindCompletionItemWithLabel(t, "extra.client_custom_info", completionItems)
	})

	t.Run("Simple column completion", func(t *testing.T) {
		tx.setFileText("SELECT  FROM clients ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(lsp.Position{Character: 7, Line: 0})

		expectToFindCompletionItemWithLabel(t, "id", completionItems)
		expectToFindCompletionItemWithLabel(t, "name", completionItems)
		expectToFindCompletionItemWithLabel(t, "type_id", completionItems)
	})

	t.Run("Join completion using given table foreign key", func(t *testing.T) {
		tx.setFileText("SELECT * FROM clients join ORDER BY id ASC")
		completionItems := tx.requestCompletionAt(lsp.Position{Character: 27, Line: 0})

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
