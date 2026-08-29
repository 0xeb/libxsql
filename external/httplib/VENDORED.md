# cpp-httplib

- **Version**: 0.15.3
- **Source**: https://github.com/yhirose/cpp-httplib
- **Tag**: v0.15.3
- **License**: MIT (see LICENSE)
- **Vendored on**: 2026-03-28
- **Files**: Single header (`httplib.h`)

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
  in `tests/libxsql/test_http_query_server.cpp` (monorepo-private).
- **`gzip_compressor` compression level** (2026-08-24): `Z_BEST_SPEED`
  (level 1), not upstream's `Z_DEFAULT_COMPRESSION` (level 6). Every *sql
  tool's HTTP response is generated fresh per request (never cached) and the
  HTTP servers are `serialize_requests`-protected (one query at a time), so
  compression CPU is paid on every query and adds directly to that query's
  latency -- unlike a static asset compressed once and served many times,
  where a higher level is free after the first request. Measured live
  against a real 50,000-row functions dump from a large PDB (7.5 MB
  uncompressed JSON, full level 1-9 sweep in
  `kb/pdbsql/benchmarks/large-pdb-benchmarks.md`): level 6 took 163.8ms for
  a 5.30x ratio; level 1 took 56.5ms (2.9x faster) for a 4.21x ratio -- keeps
  ~79% of the compression benefit at little more than a third of the CPU
  cost.
