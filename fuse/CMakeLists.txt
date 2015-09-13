
cmake_minimum_required(VERSION 2.8.3)

set(SOURCES fuse-webhdfs.c)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/../cmake/Modules/")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/bin)

find_package(FUSE REQUIRED)

include_directories(${FUSE_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/lib)

set(binDir ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/bin)
file(MAKE_DIRECTORY ${binDir})

add_executable(fuse-webhdfs ${SOURCES})
target_link_libraries(fuse-webhdfs webhdfs ${FUSE})

if (NOT WIN32)
  install(TARGETS fuse-webhdfs RUNTIME DESTINATION bin)
endif ()

