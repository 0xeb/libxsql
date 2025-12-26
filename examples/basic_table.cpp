/**
 * basic_table.cpp - Simple index-based virtual table
 *
 * Demonstrates the core xsql::table() builder API.
 */

#include <xsql/database.hpp>
#include <xsql/vtable.hpp>
#include <cstdio>
#include <vector>

struct Product {
    int id;
    std::string name;
    double price;
};

int main() {
    // Sample data
    std::vector<Product> products = {
        {1, "Apple", 1.50},
        {2, "Banana", 0.75},
        {3, "Cherry", 3.00},
        {4, "Date", 2.25},
        {5, "Elderberry", 4.50}
    };

    // Define virtual table
    auto def = xsql::table("products")
        .count([&]() { return products.size(); })
        .column_int("id", [&](size_t i) { return products[i].id; })
        .column_text("name", [&](size_t i) { return products[i].name; })
        .column_double("price", [&](size_t i) { return products[i].price; })
        .build();

    // Open database and register table
    xsql::Database db;
    if (!db.open(":memory:")) {
        fprintf(stderr, "Failed to open database: %s\n", db.last_error().c_str());
        return 1;
    }

    db.register_table(def.name.c_str(), &def);
    db.create_table(def.name.c_str(), def.name.c_str());

    // Query: All products
    printf("All products:\n");
    auto result = db.query("SELECT * FROM products");
    if (!result.ok()) {
        fprintf(stderr, "Query error: %s\n", result.error.c_str());
        return 1;
    }
    for (const auto& row : result) {
        printf("  %s | %s | $%s\n", row[0].c_str(), row[1].c_str(), row[2].c_str());
    }

    // Query: Filtered
    printf("\nProducts over $2:\n");
    result = db.query("SELECT name, price FROM products WHERE price > 2.0");
    for (const auto& row : result) {
        printf("  %s: $%s\n", row[0].c_str(), row[1].c_str());
    }

    // Query: Aggregation
    result = db.query("SELECT COUNT(*), AVG(price), MAX(price) FROM products");
    printf("\nStats: count=%s, avg=$%s, max=$%s\n",
           result[0][0].c_str(), result[0][1].c_str(), result[0][2].c_str());

    return 0;
}
