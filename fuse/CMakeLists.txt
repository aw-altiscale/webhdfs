
cmake_minimum_required(VERSION 2.8.3)

set(SOURCES fuse-webhdfs.c)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/../cmake/Modules/")

find_package(FUSE REQUIRED)

set(binDir ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/bin)
file(MAKE_DIRECTORY ${binDir})

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/include)
include_directories(${FUSE_INCLUDE_DIRS})
link_directories(${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/lib)

add_executable(fuse-webhdfs ${SOURCES})
target_link_libraries(fuse-webhdfs webhdfs ${FUSE})

get_target_property(binPath fuse-webhdfs LOCATION)
add_custom_command(TARGET fuse-webhdfs POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${binPath} ${binDir})

if (NOT WIN32)
  install(TARGETS fuse-webhdfs RUNTIME DESTINATION bin)
endif ()

