#pragma once
#include <cstdint>

using namespace std;

enum class OrderType : uint8_t {
    BUY = 0,
    SELL = 1
};

#pragma pack(push, 1)
struct MarketMessage {
    char symbol[8];
    double price;
    double quantity;
    OrderType type;
    uint64_t timestamp;
};
#pragma pack(pop)
