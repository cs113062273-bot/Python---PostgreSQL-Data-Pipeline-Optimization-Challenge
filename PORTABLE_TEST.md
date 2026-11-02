# 在另一台 Windows 電腦測 Turbo

不需要搬原本約 248 GiB 的資料夾。請從 GitHub 下載
[pipeline-test-windows-turbo.zip](https://github.com/cs113062273-bot/Python---PostgreSQL-Data-Pipeline-Optimization-Challenge/raw/refs/heads/main/dist/pipeline-test-windows-turbo.zip)，
解壓到全新資料夾。壓縮包約 34 MiB，含程式、Windows x64 PostgreSQL 17.11、
已編譯並綁定來源雜湊的 C 擴充套件與 Python 依賴清單。
不含歷史資料庫、資料匯出、編譯器、快取或 `.venv`。
只 clone GitHub 原始碼時，也要把 `dist` 裡的這個 ZIP 另解壓後再執行。

## 準備

- Windows x64、Python 3.11 x64 和 Git；PowerShell 能執行 `python`、`git`。
- 首次需要網路安裝 Python 依賴，不需要另裝 PostgreSQL 或 C 編譯器。
- 解壓路徑避免 OneDrive 等同步目錄；55433 連接埠須空閒。
- 100 萬筆、三輪保守預留 **48 GiB** 可用磁碟，100k 三輪約 7 GiB。
  程式會重新產生輸入並保留各次資料庫供查驗，因此跑完會再次佔用空間。
- 插電、關閉省電模式及大型背景程式，所有輪次維持相同條件。

## 執行

在解壓目錄開 PowerShell。正式重測建議跑 100 萬筆、三輪，
使用目前 Turbo 驗證過的 minimal-WAL 設定：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\test-other-pc.ps1 -Rows 1000000 -Repeats 3 -Candidate turbo -MinimalWal
```

若要先確認安裝及功能，可以先在另一個全新解壓目錄跑 64 筆：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\test-other-pc.ps1 -Rows 64 -Repeats 1 -Candidate turbo -MinimalWal
```

64 筆只驗證功能，不能用來判定 90%。每次完整執行都要用新的解壓目錄；
舊資料會保留，腳本遇到既有資料目錄會停止。磁碟有限可先用
`-Rows 1000000 -Repeats 1` 跑一組，保守預留約 22 GiB；單組無法證明穩定性。

腳本會核對 `PORTABLE_MANIFEST.json` 的逐檔 SHA、安裝鎖定依賴、初始化
隔離 PostgreSQL、建立原版版本控制資料，再用未修改的官方產生器產生輸入。
原版／Turbo 從全新副本開始，三輪順序為 AB、BA、AB。
第一輪原版輸出作為後續 BA 輪的比對基準，不額外保留一個 oracle 資料庫。
每次 Turbo 都在計時外完整比較六張表的值及重複次數，並檢查計數、
六個版本、約束／索引、序列、來源、原生快速路徑及暫存清理。

`-MinimalWal` 只重啟這個新建的專案私有服務，`fsync`、`full_page_writes`、
`synchronous_commit` 維持開啟，結束還原 replica。
省略它會測 replica；Turbo 尚未完成 replica 的效能與恢復資格驗證，結果須分開解讀。
需要對照舊 shared 版時，可改成 `-Candidate shared`。

## 判讀結果

結果在 `reports/portable-turbo-1000000-時間.json`，子報告與日誌在同名目錄。
確認 `all_passed` 為 `true`，再看：

- `summary.median_wall_seconds`：本機同批原版與 Turbo 的完整處理時間中位數。
- `summary.median_wall_reduction_percent`：兩者中位數計算的減時比例。
- `summary.every_pair_at_least_90_percent`：是否每組配對都至少減少 90%。
- `pairs`：逐組時間、順序及比例，保留慢的結果，不挑單次最佳值。

**90% 目前仍未正式達成，也不保證換機後一定達成。** 2026-11-02 的四組
96% 結果遇到原版異常變慢（352–399 秒，歷史約 112–119 秒），不可引用為成功。
新電腦硬體不同，原版不必剛好等於歷史秒數；應看同機同設定、完整且穩定的
配對結果，排查背景負載、降頻及異常慢基線。這不是 Kaggle 官方分數，
C 擴充套件的官方接受度、Linux/SSH 與 3 億筆規模仍未驗證。

## 空間與停止服務

本機盤點：`.runtime` 約 247.6 GiB，`.venv` 約 0.21 GiB，`reports` 約 0.18 GiB。
這些歷史測試資料不必搬到新電腦；Git 忽略新產生的報告及大型執行產物。
既有已追蹤報告及少量選定新佐證仍保留在 GitHub，供追溯。

測試資料在解壓目錄的 `.runtime/native_pgdata`。需要停止服務時，在該目錄執行：

```powershell
& '.\.runtime\postgresql\pgsql\bin\pg_ctl.exe' -D '.\.runtime\native_pgdata' -m fast -w stop
```

確認服務停止、報告已保存後，即可自行移除不再需要的整個解壓資料夾。
這次換機整理不會刪除舊機的任何資料庫。

壓縮包逐檔雜湊才是實際來源及二進位身分；包內記錄的 base commit
是打包起點。解壓後的空 Git 標記 commit 僅供環境記錄，不能當作公開提交身分。
