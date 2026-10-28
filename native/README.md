# Isolated native normalizer research

This is an unqualified experiment toward the 90% complete-processing target.
It has not been integrated into the application and does not establish official
eligibility, Linux/SSH support or any whole-pipeline percentage. The original
user-recipe function validator still rejects non-native functions, including
this DLL. A future trusted implementation would require a demonstrably
equivalent safety mechanism; a blanket whitelist or volatility-catalog change
is not implemented or justified by component speed.

`challenge_extract64(jsonb)` traverses the JSONB object once, returns the exact
64 TEXT extractions and evaluates the existing sufficient ASCII numeric/date
domain. It performs no SPI calls, relation access, writes, filesystem access,
network calls, identity allocation or persistent result caching. Invalid input
is represented by a false domain proof; it is not accepted or repaired. The
intended full-pipeline fallback remains the original complete SQL program.

The output record must contain exactly the known 64 native TEXT columns and a
final BOOLEAN proof. Unknown, duplicate or differently typed output descriptors
raise an error. Numeric/date proof arithmetic uses integer bounds; actual
NUMERIC and timestamp transformations are not implemented by this first probe.

The Windows probe uses an official PG17.11 SDK extracted from the retained
binary archive and portable Zig 0.14.1, verified against the official release
SHA256. `scripts/build_native_probe.py` retains every build attempt, identifies
the source and driver, and never overwrites an existing library. Linux PGXS
build instructions are provided by the Makefile but have not been tested.

All native loads occur in a separate PostgreSQL cluster at loopback port
55433, under `.runtime/native_pgdata`. Source/final durability settings remain
on. The application benchmark cluster is at 55432. A same-process helper
verifies the isolated data directory and loaded source/library identity.

The first successful checks compare 4,247 JSON payloads against all 64 original
operator values and the SQL domain predicate, with zero differences. Three
wrong output descriptors are rejected by the C implementation. The 100k CTAS
component uses three rotating pairs and compares every resulting row/field
exactly after timing. Its medians are 0.484485s SQL and 0.240384s native C.
Only normalization/proof CTAS is timed; final COPY/index/FK/commit/version work
is absent. Shared reference creation warms both modes identically.

References: [PostgreSQL C functions](https://www.postgresql.org/docs/17/xfunc-c.html),
[PostgreSQL JSON implementation](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/adt/jsonfuncs.c),
[official Zig releases](https://ziglang.org/download/).


The later compact API and byte-frame decoder pass 461 full six-recipe/error
comparisons on mutated fixtures. The transient trusted package also passes
non-superuser ownership/code-protection/cleanup tests, without changing the
application's untrusted SQL function policy. These extend the research; managed
counters/version/recovery and complete timing remain to be qualified.

The compact integration has since passed actual native COPY layout/default
fallback checks, sixteen pre-commit faults and exact versioned retry, populated
dump/restore, metadata/namespace ownership negatives, and isolated immediate
server-stop/WAL rollback/retry. The 100k fresh full-service pair is 13.137550s
original / 3.039362s compact (76.865078% reduction); it is a single local pair.
See docs/ACTIVE_90.md for current evidence and its limits. The main application
defaults and untrusted-recipe function validator remain unchanged.

The later fresh 1M full-service pair is 106.639991s original / 13.570365s
compact (87.274601% reduction), with all exact six outputs and original
mandatory gates. A separate live full EXCEPT ALL/source/catalog/version audit
also passes after component experiments. This remains a single local pair;
90%, repeated stability and official deployment/eligibility are not established.
