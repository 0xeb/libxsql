/**
 * writable_table.cpp - Virtual table with UPDATE and DELETE support
 *
 * Demonstrates column_*_rw() setters, deletable(), and on_modify() hook.
 */

#include <xsql/database.hpp>
#include <xsql/vtable.hpp>
#include <cstdio>
#include <vector>

struct Task {
    int id;
    std::string title;
    bool done;
};

int main() {
    // Sample data
    std::vector<Task> tasks = {
        {1, "Write documentation", false},
        {2, "Fix bug #123", false},
        {3, "Review PR", true},
        {4, "Deploy to staging", false}
    };

    // Define writable virtual table
    auto def = xsql::table("tasks")
        .count([&]() { return tasks.size(); })
        .on_modify([](const std::string& op) {
            printf("[Hook] %s\n", op.c_str());
        })
        .column_int("id", [&](size_t i) { return tasks[i].id; })
        .column_text_rw("title",
            [&](size_t i) { return tasks[i].title; },
            [&](size_t i, const char* v) {
                tasks[i].title = v;
                return true;
            })
        .column_int_rw("done",
            [&](size_t i) { return tasks[i].done ? 1 : 0; },
            [&](size_t i, int v) {
                tasks[i].done = (v != 0);
                return true;
            })
        .deletable([&](size_t i) {
            tasks.erase(tasks.begin() + i);
            return true;
        })
        .build();

    // Open database and register table
    xsql::Database db;
    db.open(":memory:");
    db.register_table(def.name.c_str(), &def);
    db.create_table(def.name.c_str(), def.name.c_str());

    // Show initial state
    auto print_tasks = [&]() {
        auto result = db.query("SELECT id, title, done FROM tasks");
        for (const auto& row : result) {
            printf("  [%s] %s - %s\n",
                   row[2] == "1" ? "x" : " ",
                   row[0].c_str(),
                   row[1].c_str());
        }
    };

    printf("Initial tasks:\n");
    print_tasks();

    // UPDATE: Mark task as done
    printf("\nMarking task 2 as done...\n");
    db.exec("UPDATE tasks SET done = 1 WHERE id = 2");
    print_tasks();

    // UPDATE: Rename task
    printf("\nRenaming task 1...\n");
    db.exec("UPDATE tasks SET title = 'Write README.md' WHERE id = 1");
    print_tasks();

    // DELETE: Remove completed tasks
    printf("\nDeleting completed tasks...\n");
    db.exec("DELETE FROM tasks WHERE done = 1");
    print_tasks();

    printf("\nFinal count: ");
    auto result = db.query("SELECT COUNT(*) FROM tasks");
    printf("%s tasks remaining\n", result[0][0].c_str());

    return 0;
}
