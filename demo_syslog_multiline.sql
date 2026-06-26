/*
================================================================================
  syslog形式マルチラインログ COPY INTO 取り込みデモ
================================================================================

  目的:
    - syslog形式で、ペイロード部分に改行コードを含むマルチラインログを
      COPY INTOで取り込み、Snowflake内で1レコードに結合する方法のデモ

  前提:
    - 事前のデータ加工（Fluentd, Cribl等による正規化）は不可
    - S3（またはステージ）上の生syslogファイルをそのまま取り込む

  アーキテクチャ:
    S3/Stage (生syslog .log)
      ↓ COPY INTO（1行=1レコードで丸呑み）
    Bronze層 (RAW_LINE + METADATA)
      ↓ SQL Window関数 + LISTAGG（ヘッダパターンで結合）
    Silver層 (1レコード=1イベント)
      ↓ REGEXP_SUBSTR（フィールド抽出）
    Gold層 (パース済み構造化ビュー)

  実行方法:
    1. Snowsight で内部ステージ syslog_stage を作成（Step 2まで実行）
    2. Snowsight の「+ Files」ボタンから sample_syslog.log をステージにアップロード
    3. Step 4 以降を順に実行

  所要時間: 約5分
================================================================================
*/

-- ============================================================================
-- Step 0: セットアップ
-- ============================================================================

CREATE DATABASE IF NOT EXISTS SYSLOG_DEMO;
USE DATABASE SYSLOG_DEMO;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- ============================================================================
-- Step 1: ファイルフォーマット定義
-- ============================================================================
-- ポイント:
--   FIELD_DELIMITER = NONE → 1行全体を単一VARCHARカラムとして取得
--   ESCAPE_UNENCLOSED_FIELD = NONE → 行末\による意図しない行結合を防止
--   SKIP_BLANK_LINES = TRUE → 空行をスキップ

CREATE OR REPLACE FILE FORMAT syslog_raw_fmt
  TYPE = CSV
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\n'
  ESCAPE_UNENCLOSED_FIELD = NONE
  SKIP_BLANK_LINES = TRUE;

-- ============================================================================
-- Step 2: 内部ステージ作成
-- ============================================================================

CREATE OR REPLACE STAGE syslog_stage
  FILE_FORMAT = syslog_raw_fmt;

-- ============================================================================
-- Step 3: サンプルデータのアップロード
-- ============================================================================
-- Snowsight GUI からアップロードする場合:
--   1. 左ペイン → Data → Databases → SYSLOG_DEMO → RAW → Stages → SYSLOG_STAGE
--   2. 「+ Files」ボタンをクリック
--   3. sample_syslog.log を選択してアップロード
--
-- SnowSQL を使う場合:
--   PUT file:///path/to/sample_syslog.log @syslog_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- アップロード確認
LIST @syslog_stage;

-- ============================================================================
-- Step 4: Bronze層テーブル作成 & COPY INTO
-- ============================================================================
-- Bronze層: 1行=1レコードでそのまま格納
-- METADATA$FILE_ROW_NUMBER で行順を保持（マルチライン結合に必須）

CREATE OR REPLACE TABLE bronze_syslog (
  raw_line         VARCHAR,
  source_file      VARCHAR,
  file_row_number  NUMBER,
  load_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO bronze_syslog (raw_line, source_file, file_row_number)
  FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @syslog_stage
  )
  FILE_FORMAT = syslog_raw_fmt
  FORCE = TRUE;

-- ============================================================================
-- Step 4b: Bronze層 取り込み確認
-- ============================================================================
-- 全行が個別レコードとして入っていることを確認（10行 = 4レコード分）
SELECT file_row_number, LEFT(raw_line, 100) AS line_preview
FROM bronze_syslog
ORDER BY file_row_number;

-- ============================================================================
-- Step 5: Silver層 - マルチライン結合
-- ============================================================================
-- ロジック:
--   1. syslogヘッダパターンで「新レコード開始行」を判定
--   2. Window関数 SUM() OVER() でレコードグループIDを採番
--   3. LISTAGG で同一グループの行を改行区切りで結合
--
-- 注意: Snowflakeの RLIKE/REGEXP_LIKE は「全体マッチ」がデフォルト
--       部分マッチとして使う場合は末尾に .* を付ける必要がある

CREATE OR REPLACE TABLE silver_syslog AS
WITH tagged AS (
  SELECT
    raw_line,
    source_file,
    file_row_number,
    -- syslogヘッダパターン: <PRI>Mon DD HH:MM:SS hostname ...
    -- RLIKE は全体マッチなので末尾に .* が必要
    CASE WHEN raw_line RLIKE '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+\\S+.*'
         THEN 1 ELSE 0 END AS is_new_record
  FROM bronze_syslog
),
grouped AS (
  SELECT
    *,
    -- 累計SUMで各行にレコードグループIDを付与
    SUM(is_new_record) OVER (
      PARTITION BY source_file
      ORDER BY file_row_number
      ROWS UNBOUNDED PRECEDING
    ) AS record_group_id
  FROM tagged
)
SELECT
  source_file,
  record_group_id,
  MIN(file_row_number) AS start_line,
  MAX(file_row_number) AS end_line,
  COUNT(*) AS line_count,
  LISTAGG(raw_line, '\n') WITHIN GROUP (ORDER BY file_row_number) AS merged_log
FROM grouped
WHERE record_group_id > 0
GROUP BY source_file, record_group_id;

--結合データの確認
SELECT * FROM silver_syslog;

-- 結合結果確認
SELECT
  record_group_id,
  start_line,
  end_line,
  line_count,
  LEFT(merged_log, 120) AS log_preview
FROM silver_syslog
ORDER BY record_group_id;

-- ============================================================================
-- Step 6: Gold層 - 構造化ビュー（フィールド抽出）
-- ============================================================================

CREATE OR REPLACE VIEW gold_syslog AS
SELECT
  source_file,
  record_group_id,
  start_line,
  line_count,
  merged_log,
  -- Priority (PRI値)
  REGEXP_SUBSTR(merged_log, '^<([0-9]+)>', 1, 1, 'e')::NUMBER AS priority,
  -- Facility & Severity (PRI値から算出)
  BITSHIFTRIGHT(REGEXP_SUBSTR(merged_log, '^<([0-9]+)>', 1, 1, 'e')::NUMBER, 3) AS facility,
  BITAND(REGEXP_SUBSTR(merged_log, '^<([0-9]+)>', 1, 1, 'e')::NUMBER, 7) AS severity,
  -- Timestamp（生文字列）
  REGEXP_SUBSTR(merged_log, '^<[0-9]+>([A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2})', 1, 1, 'e') AS timestamp_raw,
  -- Hostname
  REGEXP_SUBSTR(merged_log, '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+(\\S+)', 1, 1, 'e') AS hostname,
  -- Program[PID]
  REGEXP_SUBSTR(merged_log, '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+\\S+\\s+(\\S+?):', 1, 1, 'e') AS program,
  -- Message（ヘッダ以降）
  REGEXP_SUBSTR(merged_log, '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+\\S+\\s+\\S+:\\s+(.*)', 1, 1, 'es') AS message
FROM silver_syslog;

-- 構造化結果確認
SELECT
  record_group_id,
  priority,
  facility,
  severity,
  timestamp_raw,
  hostname,
  program,
  line_count,
  LEFT(message, 80) AS message_preview
FROM gold_syslog
ORDER BY record_group_id;

-- ============================================================================
-- Step 7: 検証 - 期待値との比較
-- ============================================================================

-- サンプルデータの期待値:
--   レコード1: ゾーンファイルロードエラー（4行 → 1レコード）
--   レコード2: DNSクエリ通常ログ（1行 → 1レコード）
--   レコード3: ゾーン転送失敗（4行 → 1レコード）
--   レコード4: MXクエリ通常ログ（1行 → 1レコード）

SELECT
  record_group_id,
  line_count,
  CASE
    WHEN record_group_id = 1 AND line_count = 4 THEN 'PASS'
    WHEN record_group_id = 2 AND line_count = 1 THEN 'PASS'
    WHEN record_group_id = 3 AND line_count = 4 THEN 'PASS'
    WHEN record_group_id = 4 AND line_count = 1 THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM silver_syslog
ORDER BY record_group_id;

-- ============================================================================
-- Step 8: 大規模データ負荷テスト（オプション）
-- ============================================================================
-- 1000万行のダミーsyslogデータを生成してパフォーマンスを計測する
-- 実測値: XSウェアハウスで約8秒、Mediumで約5秒

-- 1000万行生成（約60%がヘッダ行、40%が継続行）
CREATE OR REPLACE TABLE bronze_syslog_large AS
WITH generator AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
  FROM TABLE(GENERATOR(ROWCOUNT => 10000000))
),
syslog_data AS (
  SELECT
    rn AS file_row_number,
    'syslog_batch/server-' || LPAD(MOD(rn, 50)::VARCHAR, 2, '0') || '/messages.log' AS source_file,
    MOD(CEIL(rn / 50.0) - 1, 5) AS row_in_cycle
  FROM generator
)
SELECT
  CASE
    WHEN row_in_cycle IN (1, 2) THEN
      '  at com.example.service.' ||
      DECODE(MOD(file_row_number, 7), 0,'UserService',1,'AuthManager',2,'DataProcessor',
             3,'QueueHandler',4,'CacheManager',5,'NetworkIO',6,'DBConnector') ||
      '.process(Unknown Source:' || MOD(file_row_number, 500)::VARCHAR || ')'
    ELSE
      '<' || (MOD(file_row_number, 192))::VARCHAR || '>' ||
      DECODE(MOD(file_row_number, 12), 0,'Jan',1,'Feb',2,'Mar',3,'Apr',4,'May',5,'Jun',
             6,'Jul',7,'Aug',8,'Sep',9,'Oct',10,'Nov',11,'Dec') || ' ' ||
      LPAD((MOD(file_row_number, 28) + 1)::VARCHAR, 2, ' ') || ' ' ||
      LPAD(MOD(file_row_number, 24)::VARCHAR, 2, '0') || ':' ||
      LPAD(MOD(file_row_number, 60)::VARCHAR, 2, '0') || ':' ||
      LPAD(MOD(file_row_number * 7, 60)::VARCHAR, 2, '0') || ' ' ||
      'server-' || LPAD(MOD(file_row_number, 50)::VARCHAR, 2, '0') || ' ' ||
      DECODE(MOD(file_row_number, 8), 0,'sshd[' || MOD(file_row_number,9999)::VARCHAR || ']',
             1,'named[2481]', 2,'kernel', 3,'systemd[1]',
             4,'httpd[' || MOD(file_row_number,9999)::VARCHAR || ']',
             5,'postfix/smtpd[' || MOD(file_row_number,9999)::VARCHAR || ']',
             6,'crond[' || MOD(file_row_number,9999)::VARCHAR || ']',
             7,'auditd[' || MOD(file_row_number,9999)::VARCHAR || ']') || ': ' ||
      'Event message #' || file_row_number::VARCHAR || ' - ' ||
      DECODE(MOD(file_row_number, 5),
             0, 'Failed password for invalid user admin from 192.168.' || MOD(file_row_number,256)::VARCHAR,
             1, 'Connection closed by user root',
             2, 'OOM killed process ' || MOD(file_row_number, 9999)::VARCHAR,
             3, 'zone example.jp/IN: loading failed',
             4, 'GET /api/v1/health HTTP/1.1 200')
  END AS raw_line,
  source_file,
  file_row_number,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS load_ts
FROM syslog_data;

-- 生成結果確認
SELECT
  COUNT(*) AS total_rows,
  COUNT(CASE WHEN raw_line RLIKE '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+\\S+.*' THEN 1 END) AS header_rows,
  COUNT(*) - COUNT(CASE WHEN raw_line RLIKE '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+\\S+.*' THEN 1 END) AS continuation_rows
FROM bronze_syslog_large;

-- Silver層結合（パフォーマンス計測対象）
CREATE OR REPLACE TABLE silver_syslog_large AS
WITH tagged AS (
  SELECT
    raw_line, source_file, file_row_number,
    CASE WHEN raw_line RLIKE '^<[0-9]+>[A-Za-z]{3}\\s+[0-9]+\\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\\s+\\S+.*'
         THEN 1 ELSE 0 END AS is_new_record
  FROM bronze_syslog_large
),
grouped AS (
  SELECT *,
    SUM(is_new_record) OVER (
      PARTITION BY source_file
      ORDER BY file_row_number
      ROWS UNBOUNDED PRECEDING
    ) AS record_group_id
  FROM tagged
)
SELECT
  source_file, record_group_id,
  MIN(file_row_number) AS start_line,
  MAX(file_row_number) AS end_line,
  COUNT(*) AS line_count,
  LISTAGG(raw_line, '\n') WITHIN GROUP (ORDER BY file_row_number) AS merged_log
FROM grouped
WHERE record_group_id > 0
GROUP BY source_file, record_group_id;

-- 結合結果サマリ
SELECT
  COUNT(*) AS total_records,
  SUM(line_count) AS total_lines_merged,
  COUNT(CASE WHEN line_count = 1 THEN 1 END) AS single_line_records,
  COUNT(CASE WHEN line_count > 1 THEN 1 END) AS multiline_records,
  AVG(line_count) AS avg_lines_per_record
FROM silver_syslog_large;

-- 実行時間確認（直前のCREATE TABLE文）
SELECT
  warehouse_size,
  ROUND(total_elapsed_time / 1000.0, 1) AS elapsed_sec,
  rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
WHERE query_text LIKE '%silver_syslog_large AS%'
  AND query_text LIKE '%CREATE OR REPLACE TABLE%'
  AND query_text NOT LIKE '%QUERY_HISTORY%'
  AND execution_status = 'SUCCESS'
ORDER BY start_time DESC
LIMIT 1;

-- ============================================================================
-- Step 9: クリーンアップ（必要に応じて実行）
-- ============================================================================

-- DROP TABLE bronze_syslog_large;
-- DROP TABLE silver_syslog_large;
-- DROP DATABASE SYSLOG_DEMO;
