cmake_minimum_required(VERSION 3.8)
project(norc)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RELEASE)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(NAPI_CPP_EXCEPTIONS "yes")
set(CMAKE_CXX_FLAGS -fPIC)

file(GLOB SOURCE_FILES
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
        ${NAPI_SRC}
        ${CMAKE_JS_INC})
target_link_libraries(${PROJECT_NAME} PRIVATE ${CMAKE_JS_LIB})
find_package(Threads REQUIRED)

file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/build/${CMAKE_BUILD_TYPE})

find_library(PROTOBUF_LIBRARY protobuf /usr/local/lib /usr/lib)
find_path(ORC_INCLUDE_DIR orc/ /usr/local/include /usr/include)
find_library(ORC_LIBRARY orc  /usr/local/lib /usr/lib)
find_path(PROTOBUF_INCLUDES protobuf/ /usr/local/include/google /usr/include/google)
target_include_directories(${PROJECT_NAME} PRIVATE ${ORC_INCLUDE_DIR} ${PROTOBUF_INCLUDE_DIR})
target_link_libraries(${PROJECT_NAME} PRIVATE ${ORC_LIBRARY} Threads::Threads ${PROTOBUF_LIBRARY})

add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND "${CMAKE_COMMAND}"
        -E copy $<TARGET_FILE:norc> ${CMAKE_SOURCE_DIR}/build/${CMAKE_BUILD_TYPE})