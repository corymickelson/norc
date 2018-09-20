cmake_minimum_required(VERSION 3.8)
project(norc)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RELEASE)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(NAPI_CPP_EXCEPTIONS "yes")
set(CMAKE_CXX_FLAGS -fPIC)
set(CMAKE_EXPORT_COMPILE_COMMANDS "yes")

file(GLOB SOURCE_FILES
        "include/*.hh"
        "include/*.hpp"
        "src/*.h"
        "src/*.cc")

add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
if (NOT PROJECT_DIR)
    message(WARNING "Project dir not explicity set using cmake source directory: ${CMAKE_SOURCE_DIR}")
    set(NAPI_MODULE "${CMAKE_SOURCE_DIR}/node_modules/node-addon-api")
    set(NAPI_SRC "${CMAKE_SOURCE_DIR}/node_modules/node-addon-api/src")
else()
    set(NAPI_MODULE "${PROJECT_DIR}/node_modules/node-addon-api")
    set(NAPI_SRC  "${PROJECT_DIR}/node_modules/node-addon-api/src")
endif()
message(WARNING "Using napi: ${NAPI_MODULE}")
target_include_directories(${PROJECT_NAME} PRIVATE
        ${NAPI_MODULE}
        ${NAPI_MODULE}/include
        ${NAPI_SRC}
        ${CMAKE_JS_INC})
target_link_libraries(${PROJECT_NAME} PRIVATE ${CMAKE_JS_LIB})
find_package(Threads REQUIRED)

file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/build/${CMAKE_BUILD_TYPE}/)

# protobuf
find_library(PROTOBUF_LIBRARY protobuf /usr/local/lib /usr/lib)
find_path(PROTOBUF_INCLUDES protobuf/ /usr/local/include/google /usr/include/google)

# snappy
#find_path(SNAPPY_INCLUDE_DIR snappy.h /usr/local/include /usr/include)
#find_library(SNAPPY_LIBRARY snappy /usr/local/lib /usr/lib /usr/local/lib64 /usr/lib64 /lib64 /lib)

# lz4
#find_path(LZ4_INCLUDE_DIR lz4.h /usr/include /usr/local/include)
#find_library(LZ4_LIBRARY lz4 /lib /lib64 /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64)

# orc
find_path(ORC_INCLUDE_DIR orc/ /usr/local/include /usr/include)
find_library(ORC_LIBRARY orc  /usr/local/lib /usr/lib)

target_include_directories(${PROJECT_NAME} PRIVATE
        ${ORC_INCLUDE_DIR}
        ${PROTOBUF_INCLUDE_DIR})
target_link_libraries(${PROJECT_NAME} PRIVATE
        ${ORC_LIBRARY}
        Threads::Threads
        ${PROTOBUF_LIBRARY})
message("using protobuf: ${PROTOBUF_LIBRARY}")
message("using orc library: ${ORC_LIBRARY}")
add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND "${CMAKE_COMMAND}"
        -E copy $<TARGET_FILE:norc> ${CMAKE_SOURCE_DIR}/build/${CMAKE_BUILD_TYPE}/)
