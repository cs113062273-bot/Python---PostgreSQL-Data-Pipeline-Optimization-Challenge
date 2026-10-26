# QueryFoundry direct source control

`queryfoundry_recipes.py` is byte-for-byte `dbperf/recipes.py` from
https://github.com/Agnuxo1/QueryFoundry at submitted commit
`32c283f32fe35e9b5bd0e538bd6631f305863fb3`. It is used directly, without an
independent rewrite, by `qf_payload`, `qf_lazy` and `qf_lazy_path1`.
The original Apache-2.0 license is retained alongside it.

The module resolves `app/SQL files` relative to this project's root, so the
original source can run unchanged against the preserved official recipes.
`qf_lazy_path1` starts from its original `lazy_keys` path and replaces only the
table1 recipe with our #>> path extraction candidate. This remains experimental
because the original lazy path has unresolved error-semantic differences.
