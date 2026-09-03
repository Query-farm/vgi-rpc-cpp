function(vgi_rpc_find_iroh_cabi)
    if(TARGET vgi_iroh::cabi)
        return()
    endif()

    if(VGI_RPC_IROH_CABI_ROOT)
        find_package(vgi_iroh_cabi CONFIG QUIET
            PATHS "${VGI_RPC_IROH_CABI_ROOT}"
            NO_DEFAULT_PATH)
        if(NOT TARGET vgi_iroh::cabi)
            message(FATAL_ERROR
                "VGI_RPC_IROH_CABI_ROOT does not contain a complete vgi-iroh-cabi package: "
                "${VGI_RPC_IROH_CABI_ROOT}")
        endif()
        return()
    endif()

    find_package(vgi_iroh_cabi CONFIG QUIET)
    if(TARGET vgi_iroh::cabi)
        return()
    endif()

    if(WIN32)
        message(FATAL_ERROR
            "Windows native Iroh requires VGI_RPC_IROH_CABI_ROOT pointing to the released "
            "package, which includes Rust's versioned windows-targets import libraries")
    endif()

    find_path(VGI_IROH_CABI_INCLUDE_DIR vgi_iroh.h REQUIRED)
    find_library(VGI_IROH_CABI_LIBRARY NAMES vgi_iroh_cabi REQUIRED)
    add_library(vgi_iroh::cabi UNKNOWN IMPORTED GLOBAL)
    set_target_properties(vgi_iroh::cabi PROPERTIES
        IMPORTED_LOCATION "${VGI_IROH_CABI_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${VGI_IROH_CABI_INCLUDE_DIR}"
    )

    if(APPLE)
        find_library(VGI_IROH_SECURITY Security REQUIRED)
        find_library(VGI_IROH_SYSTEM_CONFIGURATION SystemConfiguration REQUIRED)
        find_library(VGI_IROH_CORE_FOUNDATION CoreFoundation REQUIRED)
        find_library(VGI_IROH_FOUNDATION Foundation REQUIRED)
        target_link_libraries(vgi_iroh::cabi INTERFACE
            "${VGI_IROH_SECURITY}"
            "${VGI_IROH_SYSTEM_CONFIGURATION}"
            "${VGI_IROH_CORE_FOUNDATION}"
            "${VGI_IROH_FOUNDATION}"
            iconv
            objc
        )
    elseif(UNIX)
        find_package(Threads REQUIRED)
        target_link_libraries(vgi_iroh::cabi INTERFACE
            Threads::Threads ${CMAKE_DL_LIBS} m rt util)
    else()
        message(FATAL_ERROR "native Iroh has no static-link contract for this platform")
    endif()
endfunction()
