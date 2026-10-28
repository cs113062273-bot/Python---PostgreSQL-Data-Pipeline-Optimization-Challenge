# Typed axis COPY experiment

The full-service best remains the separately audited 87.274601% compact C
pair. The axis experiment below is a component and has not replaced it.

`native/challenge_axis_ingest.c` supplies already typed axis Datums to a locally
renamed, otherwise unchanged PostgreSQL 17.11 COPY FROM engine. It uses actual
table1 identity values keyed by actual raw_id, checks the complete compact frame,
and lets PostgreSQL evaluate its native identity default, NOT NULL/CHECK,
index entries, row triggers and WAL operations. It never supplies primary IDs,
disables constraints or installs into the pure-normalizer trusted package.

The fixed-record parent hash rejects above `work_mem / 96` entries. A large
submission would need a bounded fallback/chunk design before this can be
integrated. The function checks read/write table privileges, rejects ordinary
non-superusers explicitly, and is revoked from PUBLIC. It is isolated research,
not an eligibility-qualified user-facing loader or a blanket recipe whitelist.

The original six recipes and typed table2 loader pass 241 mutated-input cases,
including all 14 axis positions, malformed values, first SQLSTATE/message,
native identities/FKs and every canonical six-table output. The original
program remains the whole-batch fallback when the compact domain is unproven.
See `reports/native-axis-ingest-semantics.json`.

A rotating three-pair 1M component compares native binary-file export/import
against the typed source. Each has 2,321,522 actual axes; all eight nonidentity
fields and full multiplicity pass bidirectional EXCEPT ALL. Medians are
1.860775s binary-file versus 1.333877s typed source, about 0.526899s saved.
The target is a separate temporary table with native identity/CHECK, matching
index absence during the existing bulk ingest. Output, cache and private files
are rolled back/removed. This excludes final indexes, FKs, durable commit,
managed counters and six versions. It is not a full processing percentage.

`vendor/postgresql-17.11/SOURCE.json` pins downloaded REL_17_11 sources by SHA
and retains PostgreSQL's COPYRIGHT. No server executable or original application
file is changed. Failed build attempts are retained; the successful build has
source SHA a8974ec7f1f809774c55da38612a9589499bc26d5acca6c24ed97cd122060fbc.
No PG_TRY handler is used in the GNU-target DLL against the MSVC-built server;
the source context is per call and has no backend global pointer to clean up.

The wider 4/8-worker native index/FK component also completes twelve rollback
trials. Medians 5.022380s (128MB/2), 5.048158s (512MB/4), 5.028677s (512MB/8)
and 4.958029s (1024MB/8) overlap substantially; no setting is adopted for a
whole-service performance claim. See native-c-maintenance-wide-components-1m.

References: [PG17.11 COPY engine](https://github.com/postgres/postgres/blob/REL_17_11/src/backend/commands/copyfrom.c),
[official bulk loading](https://www.postgresql.org/docs/17/populate.html).
