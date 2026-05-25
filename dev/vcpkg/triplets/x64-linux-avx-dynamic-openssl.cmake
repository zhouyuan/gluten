include("${CMAKE_CURRENT_LIST_DIR}/x64-linux-avx-base.cmake")

if("${PORT}" STREQUAL "openssl")
    set(VCPKG_LIBRARY_LINKAGE dynamic)
endif()
