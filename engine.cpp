#include <winsock2.h>
#include <ws2tcpip.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <unordered_map>

#include "concurrent_queue.h"
#include "protocol.h"

using namespace std;
using socket_t = SOCKET;

namespace {
constexpr unsigned short kServerPort = 8080;

struct SymbolStats {
    double best_bid = 0.0;
    double best_ask = numeric_limits<double>::infinity();
    double total_volume = 0.0;
    uint64_t messages = 0;
    uint64_t total_latency_us = 0;
};

uint64_t now_microseconds() {
    return static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(
            chrono::system_clock::now().time_since_epoch()).count());
}

string symbol_from(const MarketMessage& message) {
    const char* first = message.symbol;
    const char* last = find(first, first + sizeof(message.symbol), '\0');
    return string(first, last);
}

void receiver_worker(socket_t client, ConcurrentQueue<MarketMessage>& queue,
                     atomic<uint64_t>& received) {
    for (;;) {
        MarketMessage message{};
        int total_read = 0;
        char* destination = reinterpret_cast<char*>(&message);

        while (total_read < static_cast<int>(sizeof(MarketMessage))) {
            const int bytes = recv(client, destination + total_read,
                                   static_cast<int>(sizeof(MarketMessage)) - total_read, 0);
            if (bytes == SOCKET_ERROR) {
                cerr << "recv() failed: " << WSAGetLastError() << "\n";
                queue.finish();
                return;
            }
            if (bytes == 0) {
                // A clean TCP close is valid only between complete message frames.
                if (total_read != 0) {
                    cerr << "Connection closed in the middle of a MarketMessage.\n";
                }
                queue.finish();
                return;
            }
            total_read += bytes;
        }

        received.fetch_add(1, memory_order_relaxed);
        queue.push(move(message));
    }
}

void analytics_worker(ConcurrentQueue<MarketMessage>& queue,
                      atomic<uint64_t>& processed,
                      unordered_map<string, SymbolStats>& statistics) {
    while (optional<MarketMessage> item = queue.pop()) {
        const MarketMessage& message = *item;
        SymbolStats& stats = statistics[symbol_from(message)];
        ++stats.messages;
        stats.total_volume += message.quantity;

        if (message.type == OrderType::BUY) {
            stats.best_bid = max(stats.best_bid, message.price);
        } else {
            stats.best_ask = min(stats.best_ask, message.price);
        }

        const uint64_t now = now_microseconds();
        if (now >= message.timestamp) {
            stats.total_latency_us += now - message.timestamp;
        }
        processed.fetch_add(1, memory_order_relaxed);
    }
}
}  // namespace

int main() {
    WSADATA wsa_data{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        cerr << "WSAStartup failed.\n";
        return 1;
    }

    socket_t listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET) {
        cerr << "socket() failed: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }

    int opt = 1;
    if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char*>(&opt), sizeof(opt)) == SOCKET_ERROR) {
        cerr << "setsockopt() failed: " << WSAGetLastError() << "\n";
        closesocket(listener);
        WSACleanup();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(kServerPort);
    if (bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR ||
        listen(listener, SOMAXCONN) == SOCKET_ERROR) {
        cerr << "bind() or listen() failed: " << WSAGetLastError() << "\n";
        closesocket(listener);
        WSACleanup();
        return 1;
    }

    cout << "Engine listening on port " << kServerPort << ".\n";
    socket_t client = accept(listener, nullptr, nullptr);
    if (client == INVALID_SOCKET) {
        cerr << "accept() failed: " << WSAGetLastError() << "\n";
        closesocket(listener);
        WSACleanup();
        return 1;
    }

    ConcurrentQueue<MarketMessage> queue;
    atomic<uint64_t> received{0};
    atomic<uint64_t> processed{0};
    unordered_map<string, SymbolStats> statistics;
    const auto started = chrono::steady_clock::now();

    thread receiver(receiver_worker, client, ref(queue), ref(received));
    thread analytics(analytics_worker, ref(queue), ref(processed), ref(statistics));
    receiver.join();
    analytics.join();

    const double elapsed_seconds = chrono::duration<double>(
        chrono::steady_clock::now() - started).count();
    closesocket(client);
    closesocket(listener);
    WSACleanup();

    uint64_t total_latency_us = 0;
    cout << "\n=== MARKET ANALYTICS ===\n";
    for (const auto& entry : statistics) {
        const string& symbol = entry.first;
        const SymbolStats& stats = entry.second;
        const double best_ask = isfinite(stats.best_ask) ? stats.best_ask : 0.0;
        const double spread = (stats.best_bid > 0.0 && isfinite(stats.best_ask))
                                  ? stats.best_ask - stats.best_bid : 0.0;
        const double average_latency = stats.messages == 0 ? 0.0
            : static_cast<double>(stats.total_latency_us) / stats.messages;
        total_latency_us += stats.total_latency_us;

        cout << symbol << ": bid=" << fixed << setprecision(2) << stats.best_bid
                  << ", ask=" << best_ask << ", spread=" << spread
                  << ", volume=" << stats.total_volume
                  << ", latency=" << average_latency << " us\n";
    }

    const uint64_t total_messages = processed.load();
    const double throughput = elapsed_seconds > 0.0 ? total_messages / elapsed_seconds : 0.0;
    const double average_latency = total_messages == 0 ? 0.0
        : static_cast<double>(total_latency_us) / total_messages;
    cout << "\n=== PERFORMANCE BENCHMARK ===\n"
              << "Total messages processed: " << total_messages << "\n"
              << "Throughput: " << fixed << setprecision(0) << throughput << " msg/sec\n"
              << "Average end-to-end latency: " << setprecision(2) << average_latency << " us\n"
              << "Elapsed time: " << setprecision(3) << elapsed_seconds << " seconds\n";
    return 0;
}
