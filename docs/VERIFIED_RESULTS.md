Active objective: **90% less complete processing time**, not achieved.
See [current 90% research](ACTIVE_90.md). Earlier 85% thresholds below are historical.

Latest single 1M pair: **105.120996s original / 11.423356s shared typed
details, 89.133136% reduction**. Same durable minimal-WAL settings, exact six
outputs, native counters/constraints/identities/versions, independent live
EXCEPT ALL audit and pre/post-commit crash/dump checks pass. One local AB pair,
not repeated or official eligibility qualification. Still 0.911256s short of
the actual paired 90% threshold. See native-details-official-1m-pair.json.

Latest single local 1M pair: **103.818006s original / 11.689431s compact C
with durable minimal-WAL empty-table reset, 88.740459% reduction**. Identical
server settings, exact six outputs, source, native integrity/counters/versions
and independent live EXCEPT ALL audit pass. Pre/post-commit actual server
crashes, exact retry and populated dump/restore also pass. Native sequence
crash gaps are checked against recorded WAL reservations, not claimed gapless.
One AB pair; no 90%, repeated stability, Linux/SSH or official eligibility claim.
See native-minimal-official-1m-pair.json and ACTIVE_90.md for all limitations.

New best single local 1M pair (2026-10-29): **106.639991s original /
13.570365s compact C, 87.274601% less complete service wall**. Candidate
passes six exact typed outputs, counters, protected catalog/sequence/source
and six version records. One fresh AB pair; eligibility for the trusted C
implementation, repeated stability and Linux/SSH are unproven. The actual
paired 90% threshold is 10.663999s (another 2.906365s required).
See native-c-compact-official-1m-pair.json. Historical results follow.

Latest fresh 1M pair (2026-10-28): **113.174393s unchanged official versus
18.202447s parent_hint_copy, 83.916462% less complete processing wall time**.
This is one pair, passes all native output/integrity/version gates, and does
not achieve 85% or replace the best single inline pair (84.811364%). The
native MEASURED PROCESSING TOTAL values are separately 113.079168s versus
18.115297s (83.979987% less). No cross-cohort control is used.
The three-pair calendar result below remains a separate historical repeated
comparison. See ACTIVE_85.md and reports/parent-hint-pair-analysis.json.

Latest local result: three verified 1M pairs measure 84.627281% less complete
processing time, official median 272.559992s versus calendar_copy 41.899881s.
The first complete pair and two continuation pairs retain AB/BA/AB order;
processing hooks are identical, while post-timer verifier and observer startup
repairs are documented. Failed attempts are retained and excluded. Every run
passes six outputs/counters/six versions/catalog/sequence checks; exact full
values/multiplicities are checked once per mode in each constituent cohort.
Independent full live auditing of these six databases remains pending.
See calendar-table5-three-pairs-analysis.json and ACTIVE_85.md. The 85% target
is not achieved. Earlier 83.67% and historical eight-database audits below remain
separate; large absolute timing drift is not attributed to a new algorithm.

# 可重跑的實測結果與查核範圍

目前沒有達到耗時減少 95%。最新重新配對的本機完整流程，官方原始版
107.541923 秒、我們的 guarded_copy 23.898150 秒，減少 **77.777829%**。
這不是只計 SQL：包含原始流程的 preflight、轉換、暫存、COPY 檔案、索引／
外鍵重建與驗證、提交、實際計數器及六筆版本註冊。

| 測試組 | 官方原始版 | guarded_copy | 耗時降幅 |
|---|---:|---:|---:|
| 舊三輪的中位數 | 110.523788s | 20.030262s | 81.876967% |
| 服務恢復後的新配對，各一輪 | 107.541923s | 23.898150s | 77.777829% |

保留較慢的新結果，不將兩組混算，也不把 81.88% 說成每次都能達到。
95% 的定義是候選耗時不超過同組官方耗時的 5%；以新配對計，需要低於
5.377096 秒。這些都是本機 Windows 資料，不是官方排名或驗收成績。

## 已實際核對

- 原始 app 的 32 個檔案，在目前工作目錄及舊實測的固定 7b0f077 工作樹中，
  都符合 upstream.json 的位元組 SHA。generator 與 seed 沒有修改。
- 舊實測及新配對使用相同 atomic compiler SHA：
  ce4f60cd7aae88e708301ccbcbe8b97dbafebded5cf8ea4cf34bd0f21028fbfd。
  實際 SQL 快照與原生版本紀錄中的 batch SHA 相符。
- 兩組共八個實際資料庫都重新讀取 100 萬筆原始資料；實際 JSON 與 metadata
  的完整來源摘要相同，不只相信來源提供的 raw_hash。
- 八個資料庫共 48 次整表雙向 EXCEPT ALL，涵蓋 42,572,424 筆關聯結果資料列。
  參考結果來自新執行的官方原始流程，不是先前儲存的預期雜湊。
- 流水號按照關聯語義對齊：保留所有業務欄位、來源 raw_id 和 dimension
  內容；不要求不同執行順序產生相同的 surrogate ID。缺少 dimension 與其
  欄位全為 NULL 的情形有獨立標記。文字採 C collation 比較。
- 原始六個 destination 計數器與實際資料列數相符，六筆 control versions
  及其 SQL provenance 仍存在。來源筆數另外直接讀取，沒有虛構第七個計數器。
- schema／constraint／index 定義、sequence options／ownership 一致。
  另查全部 index 的 valid／ready／live、owner、ACL、reloptions、tablespace、
  RLS／partition flags，以及結果／metadata／control tables 的 logged 狀態。
  fsync、full_page_writes、synchronous_commit 均開啟。
- 檢查器反例：故意修改一筆輸出、保持筆數不變，必須被拒絕；修改 JSON
  但保留 raw_hash，也必須被偵測。反例只在 64 筆獨立 fixture 中執行並回滾。
- 51 個 compiler tests、六個原始 app tests、32 個官方檔案 SHA 再次通過。

原生欄位比較採 session-private TEMP 表及 EXCEPT ALL，比較每個值與重複次數。
指紋僅作額外診斷，沒有取代逐筆比較。稽核在所有計時結束後執行，不會被算成
新 benchmark，也不會回頭改寫歷史耗時。

## 自己重跑

以下使用目前私有 PostgreSQL 測試環境。每次選新的 output 檔名，避免覆蓋證據。

```powershell
$env:PGHOST='127.0.0.1'
$env:PGPORT='55432'
$env:PGUSER='challenge'
.venv/Scripts/python.exe -m bench.audit_ranked_result --offline --output reports/my-offline-audit.json
.venv/Scripts/python.exe -m bench.audit_ranked_result --output reports/my-live-audit.json
```

offline 查算式、來源檔案、SQL SHA、計時項目和版本證據；live 另外重查八個
資料庫、建立獨立反例 fixture，並以自動刪除的 TEMP 表做逐筆比對。它不修改
八個既有資料庫的業務資料。歷史報告本身無法證明另一台機器的耗時；要量新
耗時，必須另外執行 fresh cohort driver。

## 負面結果與尚未完成事項

UNLOGGED 只用於可重建、交易提交前刪除的 cache：局部 CTAS 中位數
4.211855s → 3.793340s、該階段 WAL 大幅下降，但完整新配對是
18.649798s → 18.976154s，慢 1.75%，因此沒有採用。
整數安全檢查的 planner barrier 在 100k 三輪局部測試也沒有產生 Memoize，
中位數 0.495796s → 0.501394s，沒有採用。局部改善不等於全流程勝出。

17.74 秒的 phased 單次測試沒有配對，且只有六表指紋驗證，不能冒充逐筆驗證。
舊文件中該處「exact」用詞已修正。

私有 PostgreSQL 曾非正常停止。重新接續時沒有發現伺服器程序，WAL 恢復後
才進行本次完整重查。13:11 的 Windows shared-memory error 487 不足以證明
後續停機原因；根因仍未釐清，這也不等於原先 0xc0000374 的原因已查明。
原先六個已提交結果在恢復後仍一致，另外兩個新配對資料庫也完成逐筆重查；
這不能被誇大成所有故障模式均已解決。

Linux／SSH、最終 GUI/default 整合、300M 資源界線和 managed COPY chunk recovery
仍未完成。有限測試不是所有 malformed input 或官方 eligibility 的完整證明。

原始證據：
[新配對](../reports/native-official-recheck-after-recovery-1m.json)、
[八庫逐筆稽核](../reports/ranked-cohort-live-audit-v4.json)、
[catalog 補查](../reports/ranked-cohort-catalog-supplement.json)、
[未預期停機](../reports/unexpected-postgres-unavailable-audit.json)。
稽核程式早期的兩個假設錯誤（兩種 excluded envelope、六個 destination counters）
已修正，失敗報告保留。v3 的 JSONB 逐筆比較由我們主動取消，改用更小的原生
欄位表示；v4 仍完整執行雙向 EXCEPT ALL，並通過全部八庫。


## Independent recheck of the latest and best single pairs

reports/parent-hint-inline-live-audit.json passed both cohorts and four live
databases (two unchanged official, parent_hint_copy, inline_copy). The auditor
recomputed arithmetic and provenance, then compared every logical result table
with full EXCEPT ALL multiplicities against the newly executed official rows,
rehashed actual source contents, and checked counters plus six native versions
per database. Its edited-output/edited-source negative controls passed.
The separate parent-hint-inline-catalog-audit.json also passed all four
databases: original owners/ACL/schema, native sequence metadata, valid/ready/live
indexes, and logged source/final/counter/control tables with fsync,
full_page_writes, synchronous_commit ON. These audits do not remeasure time,
prove Linux/SSH eligibility or upgrade a single pair into a repeated result.

Post-change regression checks: 51 compiler tests, six organizer tests, and
byte equality of all 32 pinned organizer files passed. The physical cache layout
experiment remains unselected; no submission/default implementation changed.
