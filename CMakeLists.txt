cmake_minimum_required(VERSION 3.20)
project(Trellis VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DTDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -flto -ffast-math")

# Performance optimization flags
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -funroll-loops -finline-functions")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# Find required packages
find_package(Threads REQUIRED)
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    add_library(nlohmann_json INTERFACE)
    add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)
endif()

# Include directories
include_directories(include)

# Source files (exclude CLI main)
file(GLOB_RECURSE SOURCES "src/*.cpp")
list(REMOVE_ITEM SOURCES "${CMAKE_SOURCE_DIR}/src/cli/main.cpp")
file(GLOB_RECURSE HEADERS "include/*.h")

# Create main executable
add_executable(trellis ${SOURCES} ${HEADERS})
target_link_libraries(trellis Threads::Threads nlohmann_json::nlohmann_json)

# Create CLI client executable
add_executable(trellis-cli src/cli/main.cpp src/tcp_client.cpp src/json_serializer.cpp src/messages.cpp)
target_link_libraries(trellis-cli Threads::Threads nlohmann_json::nlohmann_json)

# Installation
install(TARGETS trellis trellis-cli
    RUNTIME DESTINATION bin
)