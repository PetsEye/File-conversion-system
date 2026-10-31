cmake_minimum_required(VERSION 3.16)
project(minireduce LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)

add_library(mrcore
  src/log.cpp
  src/net/socket.cpp
  src/proto/messages.cpp
  src/convert/converters.cpp
  src/node/coordinator.cpp
  src/node/worker.cpp
)
target_include_directories(mrcore PUBLIC src)
target_link_libraries(mrcore PUBLIC Threads::Threads)

add_executable(mrnode src/main.cpp)
target_link_libraries(mrnode PRIVATE mrcore)

enable_testing()
add_executable(test_proto tests/test_proto.cpp)
target_link_libraries(test_proto PRIVATE mrcore)
add_test(NAME proto COMMAND test_proto)

install(TARGETS mrnode RUNTIME DESTINATION bin)
