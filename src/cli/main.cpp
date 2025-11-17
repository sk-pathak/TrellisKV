#include <cstdint>
#include <iostream>
#include <string>
#include <utility>

#include "trelliskv/messages.h"
#include "trelliskv/node_info.h"
#include "trelliskv/tcp_client.h"
#include "trelliskv/types.h"

void print_usage(const char* program_name) {
    std::cout << "TrellisKV CLI Client" << std::endl;
    std::cout << "Usage: " << program_name << " <server> <command> [args...]"
              << std::endl;
    std::cout << std::endl;
    std::cout << "Server Format:" << std::endl;
    std::cout << "  host:port" << std::endl;
    std::cout << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  get <key>          - Get value for key" << std::endl;
    std::cout << "  put <key> <value>  - Put key-value pair" << std::endl;
    std::cout << "  delete <key>       - Delete key" << std::endl;
    std::cout << "  health [--details] - Check node health status" << std::endl;
    std::cout << "  help               - Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " localhost:5000 get mykey"
              << std::endl;
    std::cout << "  " << program_name << " localhost:5000 put mykey myvalue"
              << std::endl;
    std::cout << "  " << program_name << " localhost:5000 delete mykey"
              << std::endl;
    std::cout << "  " << program_name << " localhost:5000 health --details"
              << std::endl;
}

std::pair<std::string, uint16_t> parse_address(const std::string& addr) {
    size_t colon_pos = addr.find(':');
    if (colon_pos == std::string::npos) {
        throw std::invalid_argument(
            "Invalid address format. Expected host:port");
    }

    std::string host = addr.substr(0, colon_pos);
    uint16_t port =
        static_cast<uint16_t>(std::stoi(addr.substr(colon_pos + 1)));

    return {host, port};
}

trelliskv::NodeAddress parse_server_address(const std::string& server_str) {
    auto [host, port] = parse_address(server_str);
    return trelliskv::NodeAddress(host, port);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    try {
        std::string server_str = argv[1];

        if (server_str == "help" || server_str == "--help" ||
            server_str == "-h") {
            print_usage(argv[0]);
            return 0;
        }

        if (argc < 3) {
            std::cerr << "Error: Command required" << std::endl;
            print_usage(argv[0]);
            return 1;
        }

        std::string command = argv[2];

        if (command != "get" && command != "put" && command != "delete" &&
            command != "health" && command != "help") {
            std::cerr << "Error: Unknown command '" << command << "'"
                      << std::endl;
            print_usage(argv[0]);
            return 1;
        }

        if (command == "help") {
            print_usage(argv[0]);
            return 0;
        }

        if (command == "get" && argc < 4) {
            std::cerr << "Error: GET command requires a key" << std::endl;
            return 1;
        }
        if (command == "put" && argc < 5) {
            std::cerr << "Error: PUT command requires key and value"
                      << std::endl;
            return 1;
        }
        if (command == "delete" && argc < 4) {
            std::cerr << "Error: DELETE command requires a key" << std::endl;
            return 1;
        }

        auto server_address = parse_server_address(server_str);

        trelliskv::TcpClient client;
        auto connect_result = client.connect(server_address);
        if (!connect_result.is_success()) {
            std::cerr << "Error: Failed to connect to server: "
                      << connect_result.error() << std::endl;
            return 1;
        }

        if (command == "get") {
            std::string key = argv[3];
            auto consistency = trelliskv::ConsistencyLevel::EVENTUAL;

            auto result = client.send_get_request(key, consistency);
            if (!result.is_success()) {
                std::cerr << "Error: " << result.error() << std::endl;
                return 1;
            }

            auto response = result.value();
            if (response.is_success()) {
                std::cout << response.value.value_or("") << std::endl;
            } else if (response.is_not_found()) {
                std::cout << "Key not found" << std::endl;
            } else {
                std::cerr << "Error: " << response.error_message << std::endl;
                return 1;
            }

        } else if (command == "put") {
            std::string key = argv[3];
            std::string value = argv[4];
            auto consistency = trelliskv::ConsistencyLevel::EVENTUAL;

            auto result = client.send_put_request(key, value, consistency);
            if (!result.is_success()) {
                std::cerr << "Error: " << result.error() << std::endl;
                return 1;
            }

            auto response = result.value();
            if (response.is_success()) {
                std::cout << "OK" << std::endl;
            } else {
                std::cerr << "Error: " << response.error_message << std::endl;
                return 1;
            }

        } else if (command == "delete") {
            std::string key = argv[3];
            auto consistency = trelliskv::ConsistencyLevel::EVENTUAL;

            auto result = client.send_delete_request(key, consistency);
            if (!result.is_success()) {
                std::cerr << "Error: " << result.error() << std::endl;
                return 1;
            }

            auto response = result.value();
            if (response.is_success()) {
                std::cout << "OK" << std::endl;
            } else if (response.is_not_found()) {
                std::cout << "Key not found" << std::endl;
            } else {
                std::cerr << "Error: " << response.error_message << std::endl;
                return 1;
            }

        } else if (command == "health") {
            bool include_details =
                (argc > 3 && std::string(argv[3]) == "--details");

            auto result = client.send_health_check_request(include_details);
            if (!result.is_success()) {
                std::cerr << "Error: " << result.error() << std::endl;
                return 1;
            }

            auto response = dynamic_cast<const trelliskv::HealthCheckResponse*>(
                result.value().get());
            if (response && response->is_success()) {
                std::cout << "Node ID: " << response->node_id << std::endl;
                std::cout << "Status: "
                          << (response->is_healthy ? "HEALTHY" : "UNHEALTHY")
                          << std::endl;
                std::cout << "State: "
                          << (response->node_state ==
                                      trelliskv::NodeState::ACTIVE
                                  ? "ACTIVE"
                                  : "FAILED")
                          << std::endl;
                std::cout << "Uptime: " << response->uptime << std::endl;

                if (include_details) {
                    std::cout << "\nDetailed Statistics:" << std::endl;
                    std::cout << "  Active Connections: "
                              << response->active_connections << std::endl;
                    std::cout << "  Total Nodes: " << response->total_nodes
                              << std::endl;
                    std::cout << "  Local Keys: " << response->local_keys
                              << std::endl;
                    std::cout
                        << "  Total Requests: " << response->total_requests
                        << std::endl;
                }
            } else {
                std::cerr << "Error: Failed to get health status" << std::endl;
                return 1;
            }
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}