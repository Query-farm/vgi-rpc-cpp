// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

// Shared-library visibility / DLL-export macro.
//
// When building the library itself, define VGI_RPC_BUILDING to export symbols.
// Consumers linking against the library leave VGI_RPC_BUILDING undefined,
// so VGI_RPC_EXPORT resolves to a visibility/dllimport attribute as appropriate.
//
// The Windows attributes apply only to a genuinely shared build.  A static
// library needs neither, and telling a consumer to `dllimport` from one makes
// every symbol an unresolved external at link time — which is what a static
// Windows build did before VGI_RPC_SHARED gated this.  CMake puts that
// definition on the target's INTERFACE only when the library really is shared,
// so it travels to consumers of the installed package too.

#if defined(VGI_RPC_SHARED) && (defined(_WIN32) || defined(__CYGWIN__))
  #ifdef VGI_RPC_BUILDING
    #define VGI_RPC_EXPORT __declspec(dllexport)
  #else
    #define VGI_RPC_EXPORT __declspec(dllimport)
  #endif
#elif defined(__GNUC__) || defined(__clang__)
  #ifdef VGI_RPC_BUILDING
    #define VGI_RPC_EXPORT __attribute__((visibility("default")))
  #else
    #define VGI_RPC_EXPORT
  #endif
#else
  #define VGI_RPC_EXPORT
#endif
