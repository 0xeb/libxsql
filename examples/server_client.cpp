/**
 * server_client.cpp - TCP server/client for remote SQL queries
 *
 * Demonstrates xsql::socket::Server and xsql::socket::Client.
 * Run with --server to start server, or --client to query it.
 *
 * Usage:
 *   ./server_client --server 12345
 *   ./server_client --client localhost 12345 "SELECT * FROM items"
 */

#include <xsql/database.hpp>
#include <xsql/vtable.hpp>
#include <xsql/socket/socket.hpp>
#include <cstdio>
#include <cstring>
#include <vector>
#include <thread>
#include <chrono>

struct Item {
    int id;
    std::string name;
    int quantity;
};

// Shared data for both server and demo mode
std::vector<Item> g_items = {
    {1, "Screwdriver", 50},
    {2, "Hammer", 25},
    {3, "Wrench", 30},
    {4, "Pliers", 40},
    {5, "Tape Measure", 60}
};

xsql::VTableDef g_def;
xsql::Database g_db;

void setup_database() {
    g_def = xsql::table("items")
        .count([]() { return g_items.size(); })
        .column_int("id", [](size_t i) { return g_items[i].id; })
        .column_text("name", [](size_t i) { return g_items[i].name; })
        .column_int("quantity", [](size_t i) { return g_items[i].quantity; })
        .build();

    g_db.open(":memory:");
    g_db.register_table(g_def.name.c_str(), &g_def);
    g_db.create_table(g_def.name.c_str(), g_def.name.c_str());
}

xsql::socket::QueryResult execute_query(const std::string& sql) {
    auto result = g_db.query(sql);

    xsql::socket::QueryResult qr;
    qr.success = result.ok();
    qr.error = result.error;
    qr.columns = result.columns;

    for (const auto& row : result) {
        qr.rows.push_back(row.values);
    }
    return qr;
}

int run_server(int port) {
    setup_database();

    xsql::socket::Server server;
    server.set_query_handler(execute_query);

    printf("Server listening on port %d\n", port);
    printf("Press Ctrl+C to stop\n\n");

    if (!server.run(port)) {
        fprintf(stderr, "Failed to start server on port %d\n", port);
        return 1;
    }
    return 0;
}

int run_client(const char* host, int port, const char* sql) {
    xsql::socket::Client client;

    if (!client.connect(host, port)) {
        fprintf(stderr, "Failed to connect to %s:%d\n", host, port);
        return 1;
    }

    auto result = client.query(sql);
    client.disconnect();

    if (!result.success) {
        fprintf(stderr, "Query error: %s\n", result.error.c_str());
        return 1;
    }

    // Print header
    for (size_t i = 0; i < result.columns.size(); i++) {
        if (i > 0) printf(" | ");
        printf("%s", result.columns[i].c_str());
    }
    printf("\n");

    // Print separator
    for (size_t i = 0; i < result.columns.size(); i++) {
        if (i > 0) printf("-+-");
        printf("--------");
    }
    printf("\n");

    // Print rows
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); i++) {
            if (i > 0) printf(" | ");
            printf("%s", row[i].c_str());
        }
        printf("\n");
    }

    printf("\n%zu row(s)\n", result.row_count());
    return 0;
}

int run_demo() {
    setup_database();

    printf("Demo: Starting server in background...\n");

    // Start server in background thread
    xsql::socket::Server server;
    server.set_query_handler(execute_query);
    server.run_async(12346);

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Run client queries
    printf("\nConnecting as client...\n\n");

    xsql::socket::Client client;
    if (!client.connect("127.0.0.1", 12346)) {
        fprintf(stderr, "Failed to connect\n");
        return 1;
    }

    // Query 1
    printf("Query: SELECT * FROM items\n");
    auto result = client.query("SELECT * FROM items");
    if (result.success) {
        for (const auto& row : result.rows) {
            printf("  %s | %s | %s\n", row[0].c_str(), row[1].c_str(), row[2].c_str());
        }
    }

    // Query 2
    printf("\nQuery: SELECT name FROM items WHERE quantity > 35\n");
    result = client.query("SELECT name FROM items WHERE quantity > 35");
    if (result.success) {
        for (const auto& row : result.rows) {
            printf("  %s\n", row[0].c_str());
        }
    }

    // Query 3
    printf("\nQuery: SELECT SUM(quantity) FROM items\n");
    result = client.query("SELECT SUM(quantity) FROM items");
    if (result.success) {
        printf("  Total: %s\n", result.rows[0][0].c_str());
    }

    client.disconnect();
    server.stop();

    printf("\nDemo complete.\n");
    return 0;
}

void print_usage(const char* prog) {
    printf("Usage:\n");
    printf("  %s --server <port>              Start SQL server\n", prog);
    printf("  %s --client <host> <port> <sql> Query server\n", prog);
    printf("  %s --demo                       Run demo (server + client)\n", prog);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "--server") == 0 && argc >= 3) {
        return run_server(atoi(argv[2]));
    }

    if (strcmp(argv[1], "--client") == 0 && argc >= 5) {
        return run_client(argv[2], atoi(argv[3]), argv[4]);
    }

    if (strcmp(argv[1], "--demo") == 0) {
        return run_demo();
    }

    print_usage(argv[0]);
    return 1;
}
