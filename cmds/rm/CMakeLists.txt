cmake_minimum_required(VERSION 2.6)

set(SOURCES rm.c)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../../${WEBHDFS_DIST_NAME}/bin)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../${WEBHDFS_DIST_NAME}/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/../../${WEBHDFS_DIST_NAME}/lib)

add_executable(ahrm rm.c)
target_link_libraries(ahrm webhdfsposix)
