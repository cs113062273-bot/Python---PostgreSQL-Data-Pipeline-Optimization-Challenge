# Reproducible PostgreSQL pipeline

This repository contains the latest verified **temp-stage** implementation:
one-pass JSON normalization, fused native COPY, bulk key construction, full
foreign-key validation, durable commit, native counters and six version records.

The source tree contains only runtime dependencies, selected regression tests,
build/reproduction scripts, pinned source manifests and required licenses.
Generated databases, measurements, research notes and compiler caches stay local.
The `bench/*_probe.py` helpers are dependencies of the selected implementation;
their historical names do not select extra experiments in the reproduction runner.

## Reproduce

Requirements: **Windows x64, Python 3.11 x64, Git and internet access**.
Download the ZIP and its SHA-256 file from this repository's latest Release.
Verify the ZIP with `Get-FileHash -Algorithm SHA256`, then extract into a new folder.
Do not reuse a previous benchmark directory.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\reproduce.ps1 -Rows 64 -Repeats 1
```

This creates a fresh Python environment, installs pinned dependencies, downloads
and verifies the official PostgreSQL 17.11 archive, checks the four included native
DLLs and all 32 unchanged organizer files, runs unit tests, and measures the
unchanged original against temp-stage on fresh identical inputs. Every candidate
run checks exact six-table outputs, constraints, counters, source preservation,
sequence options, cleanup and six version records. Correctness verification runs
outside the processing timer. The isolated loopback server uses port 55433 and
stops when the runner finishes. Existing data directories are never overwritten.

Use another fresh extraction for the full comparison:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\reproduce.ps1 -Rows 1000000 -Repeats 3
```

Allow **at least 50 GiB free** for three 1M pairs and retained input/output databases.
Results are written under ignored `reports/`; inspect `all_passed`,
`summary.median_wall_seconds` and each paired reduction. Measure on an otherwise
idle machine. A 64-row run verifies functionality, not a performance percentage.
The first PostgreSQL download is approximately 326 MiB and is cached locally.
`-PostgreSQLArchive <path>` reuses the exact official ZIP after SHA-256 validation.

The private lab uses `wal_level=minimal`, no replication, and keeps `fsync`,
`full_page_writes` and `synchronous_commit` enabled. This startup configuration
applies only to the newly created lab cluster.

## Build the native code

A source clone contains no compiled binaries. Use a fresh clone with the full
PostgreSQL **17.11** Windows SDK and Zig **0.14.1**:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -BuildNative -Compiler C:\tools\zig\zig.exe -Sdk C:\tools\postgresql\pgsql
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\reproduce.ps1 -SkipSetup -Rows 64 -Repeats 1
```

`native/packages.json` pins the exact seven C/header files, batch size, extension
SQL and PostgreSQL vendor sources. A source build records its actual DLL hashes
in ignored local package receipts; compiler/debug paths can change binary hashes.
The Release DLLs are separately verified against their published hashes.
The Release ZIP's `PORTABLE_MANIFEST.json` records the authoritative source commit
and every shipped file hash. A synthetic Git marker created in an extracted ZIP
is not the submission commit.

## Scope and provenance

Fresh-extraction validation on 2026-11-04, with 1M unchanged generated rows,
measured **104.598869s original / 6.296233s temp-stage**, a **93.9806% reduction**.
This was one matched local pair; exact output and all integrity/version checks
passed. This run establishes neither 95% nor 98%. Timings depend on environment
and scheduling; earlier measurements are not a guarantee for another machine.
This is a Windows local reproduction, **not an official competition score or
completed Windows-GUI/Linux-SSH submission qualification**. Linux extension
builds and the official SSH workflow have not been qualified by this release.
The original manager source remains under `app/`.

Organizer: `igorsiecz/kaggle_postgre_challenge`, commit
`5dfde9d967425982229b92d6ffe98efcd74612e9`, Apache-2.0; see `upstream.json` and
`app/LICENSE`. QueryFoundry recipe dependency: `Agnuxo1/QueryFoundry`, commit
`32c283f32fe35e9b5bd0e538bd6631f305863fb3`; see `vendor/QueryFoundry-LICENSE`.
Vendored PostgreSQL 17.11 sources retain `vendor/postgresql-17.11/COPYRIGHT`.
The root project license is `LICENSE`.
