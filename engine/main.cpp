#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <chrono>
#include <atomic>
#include <thread>
#include <algorithm>
#include <string>
#include <limits>
#include <cmath>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    using socket_t = SOCKET;
#else
    #include <unistd.h>
    #include <arpa/inet.h>
    using socket_t = int;
#endif

#include "protocol.h"
#include "concurrent_queue.h"

using namespace std;
using namespace std::chrono;

constexpr int SERVER_PORT = 8080;

struct SymbolStats {
    double best_bid = 0.0;
    double best_ask = numeric_limits<double>::infinity();
    double total_volume = 0.0;
    uint64_t msg_count = 0;
};

uint64_t get_current_micros() {
    return duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

// Ingest / TCP Receiver Thread
void receiver_worker(socket_t client_sock, ConcurrentQueue<MarketMessage>& queue, atomic<uint64_t>& rx_count) {
    MarketMessage msg;
    while (true) {
        size_t total_read = 0;
        char* buffer = reinterpret_cast<char*>(&msg);

        while (total_read < sizeof(MarketMessage)) {
            int bytes = recv(client_sock, buffer + total_read, sizeof(MarketMessage) - total_read, 0);
            if (bytes <= 0) {
                queue.finish();
                return;
            }
            total_read += bytes;
        }

        rx_count.fetch_add(1, memory_order_relaxed);
        queue.push(msg);
    }
}

// Analytics / Processing Thread
void analytics_worker(ConcurrentQueue<MarketMessage>& queue, atomic<uint64_t>& proc_count, atomic<uint64_t>& total_latency_us) {
    unordered_map<string, SymbolStats> book;

    while (auto item = queue.pop()) {
        MarketMessage msg = *item;
        uint64_t now = get_current_micros();
        if (now >= msg.timestamp) {
            total_latency_us.fetch_add(now - msg.timestamp, memory_order_relaxed);
        }

        auto& stats = book[string(msg.symbol)];
        stats.msg_count++;
        stats.total_volume += msg.quantity;

        if (msg.type == OrderType::BUY) {
            stats.best_bid = max(stats.best_bid, msg.price);
        } else {
            stats.best_ask = min(stats.best_ask, msg.price);
        }

        proc_count.fetch_add(1, memory_order_relaxed);
    }

    cout << "\n================ MARKET ANALYTICS ================\n";
    for (const auto& [symbol, stats] : book) {
        const double spread = (stats.best_bid > 0.0 && isfinite(stats.best_ask))
                                  ? stats.best_ask - stats.best_bid
                                  : 0.0;
        cout << symbol << ":\n"
             << "  Best Bid : " << fixed << setprecision(2) << stats.best_bid << "\n"
             << "  Best Ask : " << (isfinite(stats.best_ask) ? stats.best_ask : 0.0) << "\n"
             << "  Spread   : " << spread << "\n"
             << "  Volume   : " << stats.total_volume << "\n"
             << "  Messages : " << stats.msg_count << "\n\n";
    }
}

int main() {
    #ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        cerr << "WSAStartup failed.\n";
        return 1;
    }
    #endif
    
    socket_t server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == INVALID_SOCKET) {
        cerr << "Socket creation failed.\n";
        WSACleanup();
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(SERVER_PORT);

    //THE ::bind HERE - This bypasses the namespace collision!
    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        cerr << "Bind failed.\n";
        closesocket(server_fd);
        WSACleanup();
        return 1;
    }
    
    if (listen(server_fd, 1) == SOCKET_ERROR) {
        cerr << "Listen failed.\n";
        closesocket(server_fd);
        WSACleanup();
        return 1;
    }

    cout << "[Engine] Waiting for Producer on port " << SERVER_PORT << "...\n";

    socket_t client_sock = accept(server_fd, nullptr, nullptr);
    if (client_sock == INVALID_SOCKET) {
        cerr << "Accept failed.\n";
        closesocket(server_fd);
        WSACleanup();
        return 1;
    }
    cout << "[Engine] Producer connected. Processing stream...\n";

    ConcurrentQueue<MarketMessage> queue;
    atomic<uint64_t> rx_count{0};
    atomic<uint64_t> proc_count{0};
    atomic<uint64_t> total_latency_us{0};

    auto start_time = high_resolution_clock::now();

    thread rx_thread(receiver_worker, client_sock, ref(queue), ref(rx_count));
    thread analytics_thread(analytics_worker, ref(queue), ref(proc_count), ref(total_latency_us));

    rx_thread.join();
    analytics_thread.join();

    auto end_time = high_resolution_clock::now();
    double total_sec = duration<double>(end_time - start_time).count();

    uint64_t total_processed = proc_count.load();
    double throughput = total_sec > 0 ? (total_processed / total_sec) : 0.0;
    double avg_latency = total_processed > 0 ? (static_cast<double>(total_latency_us.load()) / total_processed) : 0.0;

    cout << "========== PERFORMANCE BENCHMARK ==========\n"
         << "Messages received  : " << rx_count.load() << "\n"
         << "Messages processed : " << total_processed << "\n"
         << "Producer threads   : 3\n"
         << "Consumer threads   : 2\n"
         << "Processes          : 2\n"
         << "Total Elapsed Time : " << fixed << setprecision(3) << total_sec << " s\n"
         << "Throughput         : " << static_cast<uint64_t>(throughput) << " msg/sec\n"
         << "Avg End-to-End Latency: " << fixed << setprecision(2) << avg_latency << " us\n"
         << "Dropped messages   : " << (rx_count.load() - total_processed) << "\n"
         << "===========================================\n";

    closesocket(client_sock);
    closesocket(server_fd);
    
    #ifdef _WIN32
    WSACleanup();
    #endif
    
    return 0;
}