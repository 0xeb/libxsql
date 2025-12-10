#pragma once

/**
 * @file cli.hpp
 * @brief CLI argument parsing skeleton for *sql tools
 *
 * Provides common argument parsing for direct/serve/client modes.
 * All *sql tools (idasql, pdbsql, etc.) can use this for consistent UX.
 */

#include <string>
#include <optional>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstring>

namespace xsql::thinclient {

// ============================================================================
// CLI Modes
// ============================================================================

enum class cli_mode {
    direct,     // -s db -c query (open, query, close)
    serve,      // -s db --serve --port N (open, listen)
    client      // --port N -c query (connect to server)
};

// ============================================================================
// CLI Arguments
// ============================================================================

struct cli_args {
    cli_mode mode = cli_mode::direct;

    // Database path (direct/serve modes)
    std::string database;

    // Query options
    std::string query;           // -c "..."
    std::string query_file;      // -f file.sql

    // Server options
    int port = 5555;
    std::string bind_address = "127.0.0.1";
    bool serve = false;

    // Output options
    std::string output_format = "csv";  // csv, json, table

    // Misc
    bool help = false;
    bool version = false;

    // Get the SQL to execute (from -c or -f)
    std::string get_sql() const {
        if (!query.empty()) {
            return query;
        }
        if (!query_file.empty()) {
            return read_file(query_file);
        }
        return {};
    }

private:
    static std::string read_file(const std::string& path) {
        std::ifstream file(path);
        if (!file) {
            throw std::runtime_error("Cannot open file: " + path);
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }
};

// ============================================================================
// Argument Parser
// ============================================================================

class arg_parser {
public:
    arg_parser(const std::string& program_name, const std::string& description)
        : program_name_(program_name), description_(description) {}

    std::optional<cli_args> parse(int argc, char** argv) {
        cli_args args;

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];

            if (arg == "-h" || arg == "--help") {
                args.help = true;
            }
            else if (arg == "--version") {
                args.version = true;
            }
            else if (arg == "-s" || arg == "--source") {
                if (++i >= argc) {
                    error("Missing argument for " + arg);
                    return std::nullopt;
                }
                args.database = argv[i];
            }
            else if (arg == "-c" || arg == "--command") {
                if (++i >= argc) {
                    error("Missing argument for " + arg);
                    return std::nullopt;
                }
                args.query = argv[i];
            }
            else if (arg == "-f" || arg == "--file") {
                if (++i >= argc) {
                    error("Missing argument for " + arg);
                    return std::nullopt;
                }
                args.query_file = argv[i];
            }
            else if (arg == "-o" || arg == "--output") {
                if (++i >= argc) {
                    error("Missing argument for " + arg);
                    return std::nullopt;
                }
                args.output_format = argv[i];
            }
            else if (arg == "--serve") {
                args.serve = true;
            }
            else if (arg == "--port") {
                if (++i >= argc) {
                    error("Missing argument for " + arg);
                    return std::nullopt;
                }
                try {
                    args.port = std::stoi(argv[i]);
                } catch (...) {
                    error("Invalid port number: " + std::string(argv[i]));
                    return std::nullopt;
                }
            }
            else if (arg == "--bind") {
                if (++i >= argc) {
                    error("Missing argument for " + arg);
                    return std::nullopt;
                }
                args.bind_address = argv[i];
            }
            else if (arg[0] == '-') {
                error("Unknown option: " + arg);
                return std::nullopt;
            }
            else {
                // Positional argument - treat as database if not set
                if (args.database.empty()) {
                    args.database = arg;
                } else {
                    error("Unexpected argument: " + arg);
                    return std::nullopt;
                }
            }
        }

        // Handle --help
        if (args.help) {
            print_help();
            return std::nullopt;
        }

        // Handle --version
        if (args.version) {
            std::cout << program_name_ << " version 1.0.0\n";
            return std::nullopt;
        }

        // Determine mode
        args.mode = detect_mode(args);

        // Validate
        if (!validate(args)) {
            return std::nullopt;
        }

        return args;
    }

private:
    std::string program_name_;
    std::string description_;

    cli_mode detect_mode(const cli_args& args) {
        if (args.serve) {
            return cli_mode::serve;
        }
        if (args.database.empty() && args.port != 5555) {
            // No database but has port -> client mode
            return cli_mode::client;
        }
        if (args.database.empty() && (!args.query.empty() || !args.query_file.empty())) {
            // No database but has query -> assume client mode with default port
            return cli_mode::client;
        }
        return cli_mode::direct;
    }

    bool validate(const cli_args& args) {
        switch (args.mode) {
        case cli_mode::direct:
            if (args.database.empty()) {
                error("No database specified. Use -s <database>");
                print_usage();
                return false;
            }
            if (args.query.empty() && args.query_file.empty()) {
                error("No query specified. Use -c <query> or -f <file>");
                print_usage();
                return false;
            }
            break;

        case cli_mode::serve:
            if (args.database.empty()) {
                error("No database specified for serve mode. Use -s <database>");
                print_usage();
                return false;
            }
            break;

        case cli_mode::client:
            if (args.query.empty() && args.query_file.empty()) {
                error("No query specified for client mode. Use -c <query> or -f <file>");
                print_usage();
                return false;
            }
            break;
        }
        return true;
    }

    void error(const std::string& msg) {
        std::cerr << program_name_ << ": error: " << msg << "\n";
    }

    void print_usage() {
        std::cerr << "Usage: " << program_name_ << " [options]\n";
        std::cerr << "Try '" << program_name_ << " --help' for more information.\n";
    }

    void print_help() {
        std::cout << description_ << "\n\n";
        std::cout << "Usage:\n";
        std::cout << "  " << program_name_ << " -s <database> -c <query>     Direct mode: query and exit\n";
        std::cout << "  " << program_name_ << " -s <database> -f <file>      Direct mode: run SQL file\n";
        std::cout << "  " << program_name_ << " -s <database> --serve        Server mode: listen for queries\n";
        std::cout << "  " << program_name_ << " --port <N> -c <query>        Client mode: query running server\n";
        std::cout << "\n";
        std::cout << "Options:\n";
        std::cout << "  -s, --source <path>    Database/source file path\n";
        std::cout << "  -c, --command <sql>    SQL query to execute\n";
        std::cout << "  -f, --file <path>      SQL file to execute\n";
        std::cout << "  -o, --output <format>  Output format: csv, json, table (default: csv)\n";
        std::cout << "\n";
        std::cout << "Server options:\n";
        std::cout << "  --serve                Start HTTP server mode\n";
        std::cout << "  --port <N>             Port number (default: 5555)\n";
        std::cout << "  --bind <addr>          Bind address (default: 127.0.0.1)\n";
        std::cout << "\n";
        std::cout << "Other:\n";
        std::cout << "  -h, --help             Show this help\n";
        std::cout << "  --version              Show version\n";
        std::cout << "\n";
        std::cout << "Examples:\n";
        std::cout << "  " << program_name_ << " -s test.db -c \"SELECT * FROM funcs\"\n";
        std::cout << "  " << program_name_ << " -s test.db --serve --port 8080\n";
        std::cout << "  " << program_name_ << " --port 8080 -c \"SELECT COUNT(*) FROM funcs\"\n";
        std::cout << "  curl localhost:8080/query -d \"SELECT * FROM funcs\"\n";
    }
};

// ============================================================================
// Convenience function
// ============================================================================

/**
 * Parse command line arguments.
 *
 * @param argc Argument count
 * @param argv Argument values
 * @param program_name Name of the program (for help/errors)
 * @param description Brief description (for help)
 * @return Parsed arguments, or nullopt if --help or error
 */
inline std::optional<cli_args> parse_args(
    int argc, char** argv,
    const std::string& program_name,
    const std::string& description)
{
    arg_parser parser(program_name, description);
    return parser.parse(argc, argv);
}

}  // namespace xsql::thinclient
