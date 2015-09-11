cmake_minimum_required(VERSION 2.6)

set(PUBLIC_HEADERS posix.h)
set(PRIVATE_HEADERS webhdfs_p.h buffer.h)
set(SOURCES bootstrap.c)

set(libDir ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/lib)
set(incDir ${CMAKE_CURRENT_BINARY_DIR}/../${WEBHDFS_DIST_NAME}/include/webhdfs)
file(MAKE_DIRECTORY ${libDir})
file(MAKE_DIRECTORY ${incDir})

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../webhdfs)

set(LIBRARY_OUTPUT_PATH ${libDir})

add_library(webhdfsposix_s STATIC ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})
add_library(webhdfsposix SHARED ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})

target_link_libraries(webhdfsposix webhdfs)
target_link_libraries(webhdfsposix_s webhdfs_s)

# copy public headers to output directory
foreach(header ${PUBLIC_HEADERS})
  set (header ${CMAKE_CURRENT_SOURCE_DIR}/${header})

  exec_program(${CMAKE_COMMAND} ARGS -E copy_if_different ${header} ${incDir})

  add_custom_command(TARGET webhdfsposix_s POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different ${header} ${incDir})
endforeach(header ${PUBLIC_HEADERS})

if(NOT WIN32)
  install(TARGETS webhdfsposix LIBRARY DESTINATION lib${LIB_SUFFIX})
  install(TARGETS webhdfsposix_s ARCHIVE DESTINATION lib${LIB_SUFFIX})
  install(FILES ${PUBLIC_HEADERS} DESTINATION include/webhdfs)
endif()

