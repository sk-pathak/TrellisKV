# Trellis — draft

- Trellis: distributed cache

## Repository layout
---------------------------------

- `src/` — main application code
- `include/` — public/internal headers
- `build/` — build files
- `docs/` — design docs and usage (later)
- `scripts/` — testing+benchmarks. (later)
- `docker/` — docker things. (later)

## Quick build
---------------------

### Prerequisites

- CMake 3.20+
- C++17 compiler (GCC 7+, Clang 5+)
- Docker + Docker Compose (for cluster mode)

### Build

- `git clone https://github.com/sk-pathak/trellisKV.git`
- `mkdir -p build && cd build`
- `cmake ..`
- `make -j$(nproc)`

## Plans
--------------------

- eventual consistency -> conflict?
- replication
- sharding
- communication (tcp)
- data transfer?
- eviction? lru
- cli
- concurrency

## Next
--------------------

- thread pool?