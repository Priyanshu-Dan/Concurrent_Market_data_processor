# Concurrent Market Data Processor

A high-throughput, low-latency market data processing pipeline built in C++17. Designed to simulate the backend of a high-frequency financial exchange, this system decouples data ingestion from real-time analytics using TCP sockets for Inter-Process Communication (IPC) and a synchronized Producer-Consumer threading model.

## System Architecture

The project consists of two isolated processes communicating over a local network, utilizing 7 concurrent threads to prevent I/O bottlenecks.

                 ┌──────────────────────┐
                 │ Process 1: Producer  │
                 │ (Market Data Engine) │
                 │                      │
                 │ Thread 1: BTC worker │
                 │ Thread 2: ETH worker │
                 │ Thread 3: SOL worker │
                 └──────────┬───────────┘
                            │
                     IPC / TCP Socket
                            │
                            ▼
                 ┌──────────────────────┐
                 │ Process 2: Engine    │
                 │ (Analytics & Order)  │
                 │                      │
                 │ Thread 1: TCP Rx     │
                 │ Thread 2: Analytics  │
                 └──────────────────────┘

## Key Technical Features

* **Inter-Process Communication (IPC):** Streams tightly packed binary structs over TCP sockets to minimize serialization overhead and network latency.
* **Producer-Consumer Synchronization:** Implements a thread-safe message queue using `std::mutex` and `std::condition_variable` to safely hand off data between the I/O thread and CPU-bound analytics thread.
* **Lock Contention Management:** Utilizes `std::atomic` counters for lock-free performance benchmarking and throughput tracking across multiple threads.
* **Automated Build Pipeline:** Includes a custom PowerShell script for seamless MinGW native compilation on Windows.

## Performance Benchmarks

Tested locally on Windows via MinGW. The system successfully processes and calculates real-time spreads and volume without dropping messages.

* **Total Messages Processed:** 900,000
* **Peak Throughput:** ~91,800 msg/sec
* **Average End-to-End Latency:** ~60 µs
* **Execution Time:** < 10 seconds

## Build and Run

1. Clone the repository and navigate to the project root.
2. Run the automated build script:
   ```powershell
   .\build.ps1
What this script does:

Invokes the MinGW g++ compiler with the C++17 standard (-std=c++17).

Applies aggressive compiler optimizations for maximum throughput (-O3).

Maps the header file directories (-I include).

Links the native Windows networking API required for TCP sockets (-lws2_32).

Open two separate terminal windows.

Start the server (Engine): .\engine.exe

Start the data feed (Producer): .\producer.exe
