# cpp-httplib

- **Version**: 0.16.3
- **Source**: https://github.com/yhirose/cpp-httplib
- **Tag**: v0.16.3
- **License**: MIT (see LICENSE)
- **Vendored on**: 2026-03-28 (0.15.3); updated to 0.16.3 on 2026-09-02
- **Files**: Single header (`httplib.h`)

## Why 0.16.3

0.16.3 fixes Windows thread-join issues that ghidrasql's live RPC path needs.
Before 2026-09-02 a consumer build fetched a *second*, pristine 0.16.3 from
GitHub, because this vendored copy was still 0.15.3 -- so httplib could be built
two different ways depending on build configuration. That
divergence was not academic: the fetched copy carried neither local patch
below, so streamed NDJSON responses could not be gzip-compressed under
ghidrasql (a red test), and compression silently ran at upstream level 6
instead of the benchmarked level 1 (no test at all). Bumping this copy to
0.16.3 let that second fetch be deleted, so every consumer now shares ONE httplib:
one version, one set of local patches, one zlib, one set of
CPPHTTPLIB_*_SUPPORT macros -- which is also what keeps header-only ODR safe
across libghidra, ghidrasql, and fastmcpp.

## How to Update

1. Go to https://github.com/yhirose/cpp-httplib/releases
2. Download `httplib.h` from the desired release
3. Replace `httplib.h` in this directory
4. **Re-apply every entry under "Local Patches" below** -- a wholesale
   replace silently drops them
5. Update the version in this file and in `CMakeLists.txt`

## Local Patches

Changes made directly to the vendored `httplib.h`, not present upstream.
Each is marked in-line with an `xsql local patch` comment at the change site.

- **`detail::can_compress_content_type()` gzip allowlist** (2026-08-23):
  added `application/x-ndjson` alongside `application/json`. Upstream's
  allowlist is a fixed switch over specific content-type strings (plus a
  generic `text/*` fallback); `application/x-ndjson` -- the content-type
  `xsql::thinclient::http_query_server` sends for `X-XSQL-Stream: ndjson`
  streaming responses -- matched neither, so NDJSON bulk exports could never
  be gzip-compressed even when the client sent `Accept-Encoding: gzip`.
  Live-verified via curl against a real server: the identical query under
  `X-XSQL-Stream: 1` (`application/json`) compressed correctly while
  `X-XSQL-Stream: ndjson` never did. Regression-guarded by
  `HttpQueryServerStreaming.StreamedResponseIsGzipCompressedWhenClientAcceptsIt`
  in the project's HTTP query-server test suite.
- **`gzip_compressor` compression level** (2026-08-24): `Z_BEST_SPEED`
  (level 1), not upstream's `Z_DEFAULT_COMPRESSION` (level 6). Every *sql
  tool's HTTP response is generated fresh per request (never cached) and the
  HTTP servers are `serialize_requests`-protected (one query at a time), so
  compression CPU is paid on every query and adds directly to that query's
  latency -- unlike a static asset compressed once and served many times,
  where a higher level is free after the first request. Measured live
  against a real 50,000-row functions dump (7.5 MB uncompressed JSON, from a
  full level 1-9 sweep): level 6 took 163.8ms for
  a 5.30x ratio; level 1 took 56.5ms (2.9x faster) for a 4.21x ratio -- keeps
  ~79% of the compression benefit at little more than a third of the CPU
  cost.
