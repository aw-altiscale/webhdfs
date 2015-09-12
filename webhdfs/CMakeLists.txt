cmake_minimum_required(VERSION 2.6)

set(PUBLIC_HEADERS webhdfs.h compat.h)
set(PRIVATE_HEADERS webhdfs_p.h buffer.h)
set(SOURCES webhdfs.c file.c dir.c buffer.c request.c response.c config.c snapshot.c compat.c)

find_library(CURL curl)
find_library(YAJL yajl)
find_package(GLOG)
if (GLOG_FOUND)
  include_directories(${GLOG_INCLUDE_DIRS})
  set(LIBS ${LIBS} ${GLOG_LIBRARIES})
  set(CMAKE_CFLAGS ${CMAKE_CFLAGS} -DGLOG)
endif (GLOG_FOUND)

set(libDir ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/lib)
set(incDir ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/include/webhdfs)
file(MAKE_DIRECTORY ${libDir})
file(MAKE_DIRECTORY ${incDir})

set(LIBRARY_OUTPUT_PATH ${libDir})

add_library(webhdfs_s STATIC ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})
add_library(webhdfs SHARED ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})

set(WEBHDFS_VERSION_MAJOR 0)
set(WEBHDFS_VERSION_MINOR 0)
set(WEBHDFS_VERSION_PATCH 1)
set(WEBHDFS_VERSION_STRING ${WEBHDFS_VERSION_MAJOR}.${WEBHDFS_VERSION_MINOR}.${WEBHDFS_VERSION_PATCH})
set_target_properties(webhdfs PROPERTIES VERSION ${WEBHDFS_VERSION_STRING}
                                          SOVERSION ${WEBHDFS_VERSION_MAJOR})

target_link_libraries(webhdfs ${CURL} ${YAJL})
target_link_libraries(webhdfs_s ${CURL} ${YAJL})

# copy public headers to output directory
foreach(header ${PUBLIC_HEADERS})
  set (header ${CMAKE_CURRENT_SOURCE_DIR}/${header})

  exec_program(${CMAKE_COMMAND} ARGS -E copy_if_different ${header} ${incDir})

  add_custom_command(TARGET webhdfs_s POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different ${header} ${incDir})
endforeach(header ${PUBLIC_HEADERS})

if(NOT WIN32)
  install(TARGETS webhdfs LIBRARY DESTINATION lib${LIB_SUFFIX})
  install(TARGETS webhdfs_s ARCHIVE DESTINATION lib${LIB_SUFFIX})
  install(FILES ${PUBLIC_HEADERS} DESTINATION include/webhdfs)
endif()

