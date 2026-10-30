# 在另一台 Windows 電腦測試

不需要搬走原本約 200GB 的資料夾。這個 ZIP 只含測試所需程式、依賴鎖定、
必要的原生套件及 Windows x64 PostgreSQL 17.11 執行檔，**不含歷史資料庫
與效能報告**。`PORTABLE_MANIFEST.json` 記錄隨包檔案的 SHA-256。

## 準備

- Windows x64、Python 3.11 x64 和 Git，可從 PowerShell 執行 `python`、`git`。
- 首次需要網路安裝 Python 依賴；不需要複製 `.venv`。
- 解壓到全新資料夾；避免 OneDrive 等同步目錄。
- 確認本機 55433 連接埠沒有其他 PostgreSQL。腳本只建立專案私有服務，
  不修改系統 PostgreSQL；不要指向重要資料。
- 100k 三輪預檢要求約 7GB 可用空間；1M 三輪保守預留約 48GiB。
  這是保留所有新測試副本與驗證空間的預算，並非承諾精確峰值。

## 執行

在解壓目錄開 PowerShell，先跑 64 筆的安裝／功能冒煙測試：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\test-other-pc.ps1 -Rows 64 -Repeats 1
```

效能測試請把 ZIP **另解壓到新目錄**，再跑：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\test-other-pc.ps1 -Rows 100000 -Repeats 3
```

測試較大的 100 萬筆，把 `-Rows` 改為 `1000000`。
要重現歷史最佳結果使用的 minimal-WAL 條件，可加 `-MinimalWal`；
腳本只重啟新建的隔離服務，所有耐久性開關維持開啟，結束會還原 replica。
不加此選項則測試預設 replica；兩者結果須分開解讀。

腳本會驗證壓縮包檔案 SHA、安裝依賴、初始化全新 PostgreSQL、建立原版
版本控制資料，並用未修改的官方產生器重新產生輸入。原版／新版各從
全新資料副本開始，AB／BA 交替，每次新版都比較六張表完整值及重複次數，
並檢查計數、六個版本、約束／索引、序列選項、來源與暫存清理。
第二輪起所需的獨立原版 oracle 不納入計時比較。

結果在 `reports/portable-筆數-時間.json`，完整子報告與日誌在同名目錄。
查看 `summary.median_wall_reduction_percent`，以及
`summary.every_pair_at_least_90_percent`。另外保留原生報告計入總數的耗時，
與完整 service wall 分開呈現。**不保證另一台電腦也會改善 90%，這正是
這份測試要量測的事情。這也不是 Kaggle 官方跑分。**

測試資料會保留於該解壓目錄的 `.runtime/native_pgdata`，便於查驗。
不要在服務執行時直接刪除資料目錄。需要停止時，在該目錄執行：

```powershell
& '.\.runtime\postgresql\pgsql\bin\pg_ctl.exe' -D '.\.runtime\native_pgdata' -m fast -w stop
```

## 範圍

這個包可用於 Windows x64 本機對照測試。它攜帶來源綁定的既有 Windows
原生套件，不是 Linux 安裝包，也不含編譯工具或離線 Python wheel。
`PORTABLE_MANIFEST.json` 記錄每個實際檔案的 SHA（含最新未提交程式）；
解壓後產生的空 Git 標記 commit 僅供測試程式記錄環境，並非公開提交 commit。
歷史報告只供追溯，新電腦必須以新產生的原版／新版結果計算減時比例。
