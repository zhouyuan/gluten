include("${CMAKE_CURRENT_LIST_DIR}/arm64-linux-neon.cmake")

if("${PORT}" STREQUAL "openssl")
    set(VCPKG_LIBRARY_LINKAGE dynamic)
endif()