#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <random>
#include <chrono>
#include <cstring>
#include <string>
#include <atomic>

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

using namespace std;
using namespace std::chrono;

constexpr const char* SERVER_IP = "127.0.0.1";
constexpr int SERVER_PORT = 8080;
constexpr int TOTAL_MESSAGES_PER_THREAD = 300000;

mutex socket_mutex;

uint64_t get_current_micros() {
    return duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

bool send_all(socket_t socket, const MarketMessage& message) {
    const char* data = reinterpret_cast<const char*>(&message);
    int remaining = static_cast<int>(sizeof(message));

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

void market_data_generator(socket_t sock_fd, const string& symbol, double base_price,
                           atomic<bool>& send_failed) {
    mt19937_64 rng(random_device{}());
    uniform_real_distribution<double> delta_dist(-0.5, 0.5);
    uniform_real_distribution<double> qty_dist(0.01, 2.5);
    uniform_int_distribution<int> side_dist(0, 1);

    double current_price = base_price;

    for (int i = 0; i < TOTAL_MESSAGES_PER_THREAD; ++i) {
        if (send_failed.load(memory_order_relaxed)) {
            return;
        }
        current_price += delta_dist(rng);
        if (current_price <= 1.0) current_price = base_price;

        MarketMessage msg{};
        strncpy(msg.symbol, symbol.c_str(), sizeof(msg.symbol) - 1);
        msg.price = current_price;
        msg.quantity = qty_dist(rng);
        msg.type = (side_dist(rng) == 0) ? OrderType::BUY : OrderType::SELL;
        msg.timestamp = get_current_micros();

        {
            lock_guard<mutex> lock(socket_mutex);
            if (!send_all(sock_fd, msg)) {
                send_failed.store(true, memory_order_relaxed);
                return;
            }
        }
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

    socket_t sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        cerr << "Socket creation failed.\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    cout << "[Producer] Connecting to Engine on port " << SERVER_PORT << "...\n";
    while (connect(sock, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == SOCKET_ERROR) {
        this_thread::sleep_for(milliseconds(200));
    }
    cout << "[Producer] Connected. Starting producer threads...\n";

    auto start_time = high_resolution_clock::now();

    vector<thread> producers;
    atomic<bool> send_failed{false};
    
    producers.emplace_back(market_data_generator, sock, "BTC", 104500.0, ref(send_failed));
    producers.emplace_back(market_data_generator, sock, "ETH", 4800.0, ref(send_failed));
    producers.emplace_back(market_data_generator, sock, "SOL", 240.0, ref(send_failed));

    for (auto& t : producers) {
        t.join();
    }

    auto end_time = high_resolution_clock::now();
    double elapsed_ms = duration<double, milli>(end_time - start_time).count();

    cout << "[Producer] Finished sending data in " << elapsed_ms << " ms.\n";

    closesocket(sock);
    
    #ifdef _WIN32
    WSACleanup();
    #endif
    
    return send_failed ? 1 : 0;
}