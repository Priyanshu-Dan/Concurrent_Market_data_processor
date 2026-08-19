#include <winsock2.h>
#include <ws2tcpip.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "protocol.h"

using namespace std;
using socket_t = SOCKET;

namespace {
constexpr const char* kServerAddress = "127.0.0.1";
constexpr unsigned short kServerPort = 8080;
constexpr int kMessagesPerWorker = 300000;

mutex g_send_mutex;

uint64_t now_microseconds() {
    return static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(
            chrono::system_clock::now().time_since_epoch()).count());
}

bool send_message(socket_t socket, const MarketMessage& message) {
    const char* data = reinterpret_cast<const char*>(&message);
    int remaining = static_cast<int>(sizeof(MarketMessage));

    while (remaining > 0) {
        const int sent = send(socket, data, remaining, 0);
        if (sent == SOCKET_ERROR || sent == 0) {
            return false;
        }
        data += sent;
        remaining -= sent;
    }
    return true;
}

void producer_worker(socket_t socket, const char* symbol, double initial_price,
                     atomic<bool>& send_failed) {
    mt19937_64 random_engine(random_device{}());
    uniform_real_distribution<double> price_delta(-0.50, 0.50);
    uniform_real_distribution<double> quantity(0.01, 2.50);
    bernoulli_distribution is_buy(0.5);

    double price = initial_price;
    for (int i = 0; i < kMessagesPerWorker; ++i) {
        if (send_failed.load(memory_order_relaxed)) {
            return;
        }

        price += price_delta(random_engine);
        if (price <= 0.0) {
            price = initial_price;
        }

        MarketMessage message{};
        strncpy(message.symbol, symbol, sizeof(message.symbol) - 1);
        message.price = price;
        message.quantity = quantity(random_engine);
        message.type = is_buy(random_engine) ? OrderType::BUY : OrderType::SELL;
        message.timestamp = now_microseconds();

        // Keep the entire frame together: TCP is a byte stream and send can be partial.
        lock_guard<mutex> lock(g_send_mutex);
        if (!send_message(socket, message)) {
            send_failed.store(true, memory_order_relaxed);
            return;
        }
    }
}
}  // namespace

int main() {
    WSADATA wsa_data{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        cerr << "WSAStartup failed.\n";
        return 1;
    }

    socket_t socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socket == INVALID_SOCKET) {
        cerr << "socket() failed: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(kServerPort);
    if (InetPtonA(AF_INET, kServerAddress, &server.sin_addr) != 1) {
        cerr << "Invalid server address.\n";
        closesocket(socket);
        WSACleanup();
        return 1;
    }

    cout << "Connecting to " << kServerAddress << ':' << kServerPort << "...\n";
    if (connect(socket, reinterpret_cast<const sockaddr*>(&server), sizeof(server)) == SOCKET_ERROR) {
        cerr << "connect() failed: " << WSAGetLastError() << "\n";
        closesocket(socket);
        WSACleanup();
        return 1;
    }

    atomic<bool> send_failed{false};
    const auto started = chrono::steady_clock::now();
    vector<thread> workers;
    workers.emplace_back(producer_worker, socket, "BTC", 104500.0, ref(send_failed));
    workers.emplace_back(producer_worker, socket, "ETH", 4800.0, ref(send_failed));
    workers.emplace_back(producer_worker, socket, "SOL", 240.0, ref(send_failed));

    for (thread& worker : workers) {
        worker.join();
    }

    const double elapsed_seconds = chrono::duration<double>(
        chrono::steady_clock::now() - started).count();
    shutdown(socket, SD_SEND);  // Tell the engine that the input stream is complete.
    closesocket(socket);
    WSACleanup();

    if (send_failed.load()) {
        cerr << "A send() operation failed.\n";
        return 1;
    }

    cout << "Sent " << (3 * kMessagesPerWorker) << " messages in "
              << elapsed_seconds << " seconds.\n";
    return 0;
}
