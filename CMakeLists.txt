cmake_minimum_required(VERSION 3.10)
project(MarketDataProcessor)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add include directory for shared headers
include_directories(include)

# Find Threads (required for std::thread)
find_package(Threads REQUIRED)

# Process 1: Producer
add_executable(producer producer/main.cpp)
target_link_libraries(producer PRIVATE Threads::Threads)

# Process 2: Engine
add_executable(engine engine/main.cpp)
target_link_libraries(engine PRIVATE Threads::Threads)

# Link Windows Sockets if building on Windows
if(WIN32)
    target_link_libraries(producer PRIVATE ws2_32)
    target_link_libraries(engine PRIVATE ws2_32)
endif()