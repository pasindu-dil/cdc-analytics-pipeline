-- ============================================================
-- ClickHouse Schema (Auto-generated from MySQL)
-- Source DB  : bsmsc
-- Generated  : 2026-05-03T08:11:59.792Z
-- Kafka      : kafka:29092
-- Topic prefix: dbserver1
-- ============================================================

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS bsmsc;


-- ═══════════════════════════════════════════════════════════
-- TABLE: activity_log  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.activity_log
(
    id                        UInt64,
    log_name                  String,
    description               String,
    subject_id                Nullable(UInt64),
    subject_type              String,
    causer_id                 Nullable(UInt64),
    causer_type               String,
    properties                String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.activity_log_kafka_queue
(
    id                        Nullable(UInt64),
    log_name                  Nullable(String),
    description               Nullable(String),
    subject_id                Nullable(UInt64),
    subject_type              Nullable(String),
    causer_id                 Nullable(UInt64),
    causer_type               Nullable(String),
    properties                Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.activity_log',
    kafka_group_name           = 'ch-activity-log-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.activity_log_kafka_mv
TO bsmsc.activity_log
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(log_name, '') AS log_name,
    coalesce(description, '') AS description,
    coalesce(subject_id, 0) AS subject_id,
    coalesce(subject_type, '') AS subject_type,
    coalesce(causer_id, 0) AS causer_id,
    coalesce(causer_type, '') AS causer_type,
    coalesce(properties, '') AS properties,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.activity_log_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: aliases  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.aliases
(
    id                        UInt32,
    alias                     String,
    word                      String,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.aliases_kafka_queue
(
    id                        Nullable(UInt32),
    alias                     Nullable(String),
    word                      Nullable(String),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.aliases',
    kafka_group_name           = 'ch-aliases-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.aliases_kafka_mv
TO bsmsc.aliases
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(alias, '') AS alias,
    coalesce(word, '') AS word,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.aliases_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: billing_accounts  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.billing_accounts
(
    id                        Int32,
    name                      String,
    corporation_id            Int32,
    package_id                Int32,
    credit_balance            Float64,
    current_month_outstanding Nullable(Float64),
    credit_limit              Nullable(Float32),
    is_charged_on_delivery    Int8,
    is_flash_allowed          Int8,
    is_auto_recover           Int8,
    cdr_tag                   String,
    enabled                   Int8,
    start_date                Nullable(Date),
    expire_date               Nullable(Date),
    suspended                 Int8,
    is_api                    Int8,
    monthly_usage             Nullable(Float64),
    billing_number            Nullable(Int64),
    last_payment_time         Nullable(DateTime),
    last_payment              Nullable(Float64),
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.billing_accounts_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    corporation_id            Nullable(Int32),
    package_id                Nullable(Int32),
    credit_balance            Nullable(Float64),
    current_month_outstanding Nullable(Float64),
    credit_limit              Nullable(Float32),
    is_charged_on_delivery    Nullable(Int8),
    is_flash_allowed          Nullable(Int8),
    is_auto_recover           Nullable(Int8),
    cdr_tag                   Nullable(String),
    enabled                   Nullable(Int8),
    start_date                Nullable(String),
    expire_date               Nullable(String),
    suspended                 Nullable(Int8),
    is_api                    Nullable(Int8),
    monthly_usage             Nullable(Float64),
    billing_number            Nullable(Int64),
    last_payment_time         Nullable(String),
    last_payment              Nullable(Float64),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.billing_accounts',
    kafka_group_name           = 'ch-billing-accounts-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.billing_accounts_kafka_mv
TO bsmsc.billing_accounts
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(package_id, 0) AS package_id,
    coalesce(credit_balance, 0) AS credit_balance,
    coalesce(current_month_outstanding, 0) AS current_month_outstanding,
    coalesce(credit_limit, 0) AS credit_limit,
    coalesce(is_charged_on_delivery, 0) AS is_charged_on_delivery,
    coalesce(is_flash_allowed, 0) AS is_flash_allowed,
    coalesce(is_auto_recover, 0) AS is_auto_recover,
    coalesce(cdr_tag, '') AS cdr_tag,
    coalesce(enabled, 1) AS enabled,
    toDate(parseDateTimeBestEffortOrNull(start_date)) AS start_date,
    toDate(parseDateTimeBestEffortOrNull(expire_date)) AS expire_date,
    coalesce(suspended, 0) AS suspended,
    coalesce(is_api, 0) AS is_api,
    coalesce(monthly_usage, 0) AS monthly_usage,
    coalesce(billing_number, 0) AS billing_number,
    parseDateTimeBestEffortOrNull(last_payment_time) AS last_payment_time,
    coalesce(last_payment, 0) AS last_payment,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.billing_accounts_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: billing_details  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.billing_details
(
    id                        Int64,
    billing_account_id        Int32,
    pre_tax_amount            Nullable(Float64),
    telecom_levy              Nullable(Float64),
    cess                      Nullable(Float64),
    sscl                      Nullable(Float64),
    promo_levy                Int8,
    vat                       Nullable(Float64),
    svat                      Nullable(Float64),
    date                      Date,
    created_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.billing_details_kafka_queue
(
    id                        Nullable(Int64),
    billing_account_id        Nullable(Int32),
    pre_tax_amount            Nullable(Float64),
    telecom_levy              Nullable(Float64),
    cess                      Nullable(Float64),
    sscl                      Nullable(Float64),
    promo_levy                Nullable(Int8),
    vat                       Nullable(Float64),
    svat                      Nullable(Float64),
    date                      Nullable(String),
    created_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.billing_details',
    kafka_group_name           = 'ch-billing-details-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.billing_details_kafka_mv
TO bsmsc.billing_details
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(pre_tax_amount, 0) AS pre_tax_amount,
    coalesce(telecom_levy, 0) AS telecom_levy,
    coalesce(cess, 0) AS cess,
    coalesce(sscl, 0) AS sscl,
    coalesce(promo_levy, 0) AS promo_levy,
    coalesce(vat, 0) AS vat,
    coalesce(svat, 0) AS svat,
    toDate(parseDateTimeBestEffortOrNull(date)) AS date,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.billing_details_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: billing_units  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.billing_units
(
    id                        Int64,
    name                      String,
    character_length          Int32,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.billing_units_kafka_queue
(
    id                        Nullable(Int64),
    name                      Nullable(String),
    character_length          Nullable(Int32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.billing_units',
    kafka_group_name           = 'ch-billing-units-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.billing_units_kafka_mv
TO bsmsc.billing_units
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(character_length, 160) AS character_length,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.billing_units_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: blacklisted_text  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.blacklisted_text
(
    id                        UInt32,
    text                      String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.blacklisted_text_kafka_queue
(
    id                        Nullable(UInt32),
    text                      Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.blacklisted_text',
    kafka_group_name           = 'ch-blacklisted-text-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.blacklisted_text_kafka_mv
TO bsmsc.blacklisted_text
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(text, '') AS text,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.blacklisted_text_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: blacklists  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.blacklists
(
    id                        Int32,
    number                    Nullable(UInt64),
    corporation_id            Nullable(Int32),
    mask_id                   Nullable(Int32),
    channel                   String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.blacklists_kafka_queue
(
    id                        Nullable(Int32),
    number                    Nullable(UInt64),
    corporation_id            Nullable(Int32),
    mask_id                   Nullable(Int32),
    channel                   Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.blacklists',
    kafka_group_name           = 'ch-blacklists-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.blacklists_kafka_mv
TO bsmsc.blacklists
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(mask_id, 0) AS mask_id,
    coalesce(channel, 'admin') AS channel,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.blacklists_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: blackout_times  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.blackout_times
(
    id                        UInt32,
    start_time                String,
    end_time                  String,
    corporation_id            Int32,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.blackout_times_kafka_queue
(
    id                        Nullable(UInt32),
    start_time                Nullable(String),
    end_time                  Nullable(String),
    corporation_id            Nullable(Int32),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.blackout_times',
    kafka_group_name           = 'ch-blackout-times-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.blackout_times_kafka_mv
TO bsmsc.blackout_times
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(start_time, '') AS start_time,
    coalesce(end_time, '') AS end_time,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.blackout_times_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: campaign_blacklists  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.campaign_blacklists
(
    id                        Int32,
    number                    Nullable(Int64),
    request_id                Int64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.campaign_blacklists_kafka_queue
(
    id                        Nullable(Int32),
    number                    Nullable(Int64),
    request_id                Nullable(Int64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.campaign_blacklists',
    kafka_group_name           = 'ch-campaign-blacklists-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.campaign_blacklists_kafka_mv
TO bsmsc.campaign_blacklists
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(request_id, 0) AS request_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.campaign_blacklists_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: campaign_summaries  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.campaign_summaries
(
    id                        UInt64,
    date                      DateTime,
    request_id                Int64,
    corporation_id            Int32,
    mask                      String,
    campaign_name             String,
    channel                   String,
    corporation_name          String,
    user_name                 String,
    total_parts               Nullable(UInt32),
    submitted_parts           Nullable(UInt32),
    delivered_parts           Nullable(UInt32),
    expired_parts             Nullable(UInt32),
    cancelled_parts           Nullable(UInt32),
    blocked_parts             Nullable(UInt32),
    failed_parts              Nullable(UInt32),
    billing_number            Nullable(Int64),
    scheduled_time            Nullable(DateTime),
    processed_time            Nullable(DateTime),
    status                    String,
    total_number_count        Nullable(UInt32),
    delivered_number_count    Nullable(UInt32),
    content                   String,
    parts                     Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.campaign_summaries_kafka_queue
(
    id                        Nullable(UInt64),
    date                      Nullable(String),
    request_id                Nullable(Int64),
    corporation_id            Nullable(Int32),
    mask                      Nullable(String),
    campaign_name             Nullable(String),
    channel                   Nullable(String),
    corporation_name          Nullable(String),
    user_name                 Nullable(String),
    total_parts               Nullable(UInt32),
    submitted_parts           Nullable(UInt32),
    delivered_parts           Nullable(UInt32),
    expired_parts             Nullable(UInt32),
    cancelled_parts           Nullable(UInt32),
    blocked_parts             Nullable(UInt32),
    failed_parts              Nullable(UInt32),
    billing_number            Nullable(Int64),
    scheduled_time            Nullable(String),
    processed_time            Nullable(String),
    status                    Nullable(String),
    total_number_count        Nullable(UInt32),
    delivered_number_count    Nullable(UInt32),
    content                   Nullable(String),
    parts                     Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.campaign_summaries',
    kafka_group_name           = 'ch-campaign-summaries-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.campaign_summaries_kafka_mv
TO bsmsc.campaign_summaries
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(date) AS date,
    coalesce(request_id, 0) AS request_id,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(mask, '') AS mask,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(channel, 'web') AS channel,
    coalesce(corporation_name, '') AS corporation_name,
    coalesce(user_name, '') AS user_name,
    coalesce(total_parts, 0) AS total_parts,
    coalesce(submitted_parts, 0) AS submitted_parts,
    coalesce(delivered_parts, 0) AS delivered_parts,
    coalesce(expired_parts, 0) AS expired_parts,
    coalesce(cancelled_parts, 0) AS cancelled_parts,
    coalesce(blocked_parts, 0) AS blocked_parts,
    coalesce(failed_parts, 0) AS failed_parts,
    coalesce(billing_number, 0) AS billing_number,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, '') AS status,
    coalesce(total_number_count, 0) AS total_number_count,
    coalesce(delivered_number_count, 0) AS delivered_number_count,
    coalesce(content, '') AS content,
    coalesce(parts, 1) AS parts,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.campaign_summaries_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: campaign_summaries_old  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.campaign_summaries_old
(
    id                        UInt64,
    date                      Nullable(Date),
    request_id                Int64,
    corporation_id            Int32,
    mask                      String,
    campaign_name             String,
    channel                   String,
    corporation_name          String,
    user_name                 String,
    total_parts               Nullable(UInt32),
    submitted_parts           Nullable(UInt32),
    delivered_parts           Nullable(UInt32),
    expired_parts             Nullable(UInt32),
    cancelled_parts           Nullable(UInt32),
    blocked_parts             Nullable(UInt32),
    failed_parts              Nullable(UInt32),
    billing_number            Nullable(Int64),
    scheduled_time            Nullable(DateTime),
    processed_time            Nullable(DateTime),
    status                    String,
    total_number_count        Nullable(UInt32),
    delivered_number_count    Nullable(UInt32),
    content                   String,
    parts                     Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.campaign_summaries_old_kafka_queue
(
    id                        Nullable(UInt64),
    date                      Nullable(String),
    request_id                Nullable(Int64),
    corporation_id            Nullable(Int32),
    mask                      Nullable(String),
    campaign_name             Nullable(String),
    channel                   Nullable(String),
    corporation_name          Nullable(String),
    user_name                 Nullable(String),
    total_parts               Nullable(UInt32),
    submitted_parts           Nullable(UInt32),
    delivered_parts           Nullable(UInt32),
    expired_parts             Nullable(UInt32),
    cancelled_parts           Nullable(UInt32),
    blocked_parts             Nullable(UInt32),
    failed_parts              Nullable(UInt32),
    billing_number            Nullable(Int64),
    scheduled_time            Nullable(String),
    processed_time            Nullable(String),
    status                    Nullable(String),
    total_number_count        Nullable(UInt32),
    delivered_number_count    Nullable(UInt32),
    content                   Nullable(String),
    parts                     Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.campaign_summaries_old',
    kafka_group_name           = 'ch-campaign-summaries-old-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.campaign_summaries_old_kafka_mv
TO bsmsc.campaign_summaries_old
AS
SELECT
    coalesce(id, 0) AS id,
    toDate(parseDateTimeBestEffortOrNull(date)) AS date,
    coalesce(request_id, 0) AS request_id,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(mask, '') AS mask,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(channel, 'web') AS channel,
    coalesce(corporation_name, '') AS corporation_name,
    coalesce(user_name, '') AS user_name,
    coalesce(total_parts, 0) AS total_parts,
    coalesce(submitted_parts, 0) AS submitted_parts,
    coalesce(delivered_parts, 0) AS delivered_parts,
    coalesce(expired_parts, 0) AS expired_parts,
    coalesce(cancelled_parts, 0) AS cancelled_parts,
    coalesce(blocked_parts, 0) AS blocked_parts,
    coalesce(failed_parts, 0) AS failed_parts,
    coalesce(billing_number, 0) AS billing_number,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, '') AS status,
    coalesce(total_number_count, 0) AS total_number_count,
    coalesce(delivered_number_count, 0) AS delivered_number_count,
    coalesce(content, '') AS content,
    coalesce(parts, 1) AS parts,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.campaign_summaries_old_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: cdr  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.cdr
(
    transaction_id            Int32,
    transaction_time          DateTime,
    charge                    Nullable(Float64),
    billing_account_id        Int32,
    request_id                Nullable(Int64),
    is_processed              Int8,
    promo_levy                Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (transaction_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.cdr_kafka_queue
(
    transaction_id            Nullable(Int32),
    transaction_time          Nullable(String),
    charge                    Nullable(Float64),
    billing_account_id        Nullable(Int32),
    request_id                Nullable(Int64),
    is_processed              Nullable(Int8),
    promo_levy                Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.cdr',
    kafka_group_name           = 'ch-cdr-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.cdr_kafka_mv
TO bsmsc.cdr
AS
SELECT
    coalesce(transaction_id, 0) AS transaction_id,
    parseDateTimeBestEffortOrNull(transaction_time) AS transaction_time,
    coalesce(charge, 0) AS charge,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(request_id, 0) AS request_id,
    coalesce(is_processed, 0) AS is_processed,
    coalesce(promo_levy, 0) AS promo_levy,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.cdr_kafka_queue
WHERE transaction_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: cdr_new  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.cdr_new
(
    transaction_id            Int32,
    transaction_time          Nullable(DateTime),
    charge                    Nullable(Float64),
    billing_account_id        Int32,
    request_id                Nullable(Int64),
    is_processed              Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (transaction_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.cdr_new_kafka_queue
(
    transaction_id            Nullable(Int32),
    transaction_time          Nullable(String),
    charge                    Nullable(Float64),
    billing_account_id        Nullable(Int32),
    request_id                Nullable(Int64),
    is_processed              Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.cdr_new',
    kafka_group_name           = 'ch-cdr-new-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.cdr_new_kafka_mv
TO bsmsc.cdr_new
AS
SELECT
    coalesce(transaction_id, 0) AS transaction_id,
    parseDateTimeBestEffortOrNull(transaction_time) AS transaction_time,
    coalesce(charge, 0) AS charge,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(request_id, 0) AS request_id,
    coalesce(is_processed, 0) AS is_processed,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.cdr_new_kafka_queue
WHERE transaction_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: connection_types  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.connection_types
(
    id                        Int32,
    name                      String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.connection_types_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.connection_types',
    kafka_group_name           = 'ch-connection-types-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.connection_types_kafka_mv
TO bsmsc.connection_types
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.connection_types_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: corporations  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.corporations
(
    id                        Int32,
    name                      String,
    currency                  String,
    timezone                  String,
    reseller_id               Nullable(Int32),
    sales_code                String,
    throughput                Nullable(Int32),
    api_throughput            Nullable(Int32),
    custom_optout_msg         String,
    campaign_alerts           Int8,
    alert_frequency           Nullable(Int32),
    skip_billing              Int8,
    enabled                   Int8,
    billing_mode              String,
    invoice_type              String,
    vat_number                String,
    svat_number               String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.corporations_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    currency                  Nullable(String),
    timezone                  Nullable(String),
    reseller_id               Nullable(Int32),
    sales_code                Nullable(String),
    throughput                Nullable(Int32),
    api_throughput            Nullable(Int32),
    custom_optout_msg         Nullable(String),
    campaign_alerts           Nullable(Int8),
    alert_frequency           Nullable(Int32),
    skip_billing              Nullable(Int8),
    enabled                   Nullable(Int8),
    billing_mode              Nullable(String),
    invoice_type              Nullable(String),
    vat_number                Nullable(String),
    svat_number               Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.corporations',
    kafka_group_name           = 'ch-corporations-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.corporations_kafka_mv
TO bsmsc.corporations
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(currency, '') AS currency,
    coalesce(timezone, '') AS timezone,
    coalesce(reseller_id, 0) AS reseller_id,
    coalesce(sales_code, '') AS sales_code,
    coalesce(throughput, 5) AS throughput,
    coalesce(api_throughput, 5) AS api_throughput,
    coalesce(custom_optout_msg, '') AS custom_optout_msg,
    coalesce(campaign_alerts, 0) AS campaign_alerts,
    coalesce(alert_frequency, 0) AS alert_frequency,
    coalesce(skip_billing, 0) AS skip_billing,
    coalesce(enabled, 1) AS enabled,
    coalesce(billing_mode, 'external') AS billing_mode,
    coalesce(invoice_type, '') AS invoice_type,
    coalesce(vat_number, '') AS vat_number,
    coalesce(svat_number, '') AS svat_number,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.corporations_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: credit_balance_adjustments  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.credit_balance_adjustments
(
    id                        Int32,
    billing_account_id        Int32,
    billing_number            String,
    local_balance             Float32,
    remote_balance            Float32,
    local_credit_limit        Nullable(Float32),
    remote_credit_limit       Nullable(Float32),
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.credit_balance_adjustments_kafka_queue
(
    id                        Nullable(Int32),
    billing_account_id        Nullable(Int32),
    billing_number            Nullable(String),
    local_balance             Nullable(Float32),
    remote_balance            Nullable(Float32),
    local_credit_limit        Nullable(Float32),
    remote_credit_limit       Nullable(Float32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.credit_balance_adjustments',
    kafka_group_name           = 'ch-credit-balance-adjustments-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.credit_balance_adjustments_kafka_mv
TO bsmsc.credit_balance_adjustments
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(billing_number, '') AS billing_number,
    coalesce(local_balance, 0) AS local_balance,
    coalesce(remote_balance, 0) AS remote_balance,
    coalesce(local_credit_limit, 0) AS local_credit_limit,
    coalesce(remote_credit_limit, 0) AS remote_credit_limit,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.credit_balance_adjustments_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: deleted_lists  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.deleted_lists
(
    id                        UInt32,
    list_id                   Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.deleted_lists_kafka_queue
(
    id                        Nullable(UInt32),
    list_id                   Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.deleted_lists',
    kafka_group_name           = 'ch-deleted-lists-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.deleted_lists_kafka_mv
TO bsmsc.deleted_lists
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(list_id, 0) AS list_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.deleted_lists_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: department_billing_accounts  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.department_billing_accounts
(
    department_id             Int32,
    billing_account_id        Int32,
    created_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (department_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.department_billing_accounts_kafka_queue
(
    department_id             Nullable(Int32),
    billing_account_id        Nullable(Int32),
    created_at                Nullable(String),
    created_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.department_billing_accounts',
    kafka_group_name           = 'ch-department-billing-accounts-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.department_billing_accounts_kafka_mv
TO bsmsc.department_billing_accounts
AS
SELECT
    coalesce(department_id, 0) AS department_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.department_billing_accounts_kafka_queue
WHERE department_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: department_masks  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.department_masks
(
    mask_id                   Int32,
    department_id             Int32,
    created_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (mask_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.department_masks_kafka_queue
(
    mask_id                   Nullable(Int32),
    department_id             Nullable(Int32),
    created_at                Nullable(String),
    created_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.department_masks',
    kafka_group_name           = 'ch-department-masks-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.department_masks_kafka_mv
TO bsmsc.department_masks
AS
SELECT
    coalesce(mask_id, 0) AS mask_id,
    coalesce(department_id, 0) AS department_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.department_masks_kafka_queue
WHERE mask_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: departments  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.departments
(
    id                        Int32,
    name                      String,
    signature                 String,
    callback_url              String,
    callback_method           String,
    corporation_id            Int32,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.departments_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    signature                 Nullable(String),
    callback_url              Nullable(String),
    callback_method           Nullable(String),
    corporation_id            Nullable(Int32),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.departments',
    kafka_group_name           = 'ch-departments-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.departments_kafka_mv
TO bsmsc.departments
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(signature, '') AS signature,
    coalesce(callback_url, '') AS callback_url,
    coalesce(callback_method, '') AS callback_method,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.departments_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: dlr  [NO PRIMARY KEY → UUID applied]
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
-- ⚠️  No Primary Key detected in MySQL → UUID (row_id) auto-generated
CREATE TABLE IF NOT EXISTS bsmsc.dlr
(
    row_id                    UUID DEFAULT generateUUIDv4(),
    smsc                      String,
    ts                        String,
    destination               String,
    source                    String,
    service                   String,
    url                       String,
    mask                      Nullable(Int32),
    status                    Nullable(Int32),
    boxc                      String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = MergeTree()   -- No PK: UUID used, append-only (no dedup)

ORDER BY (row_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.dlr_kafka_queue
(
    smsc                      Nullable(String),
    ts                        Nullable(String),
    destination               Nullable(String),
    source                    Nullable(String),
    service                   Nullable(String),
    url                       Nullable(String),
    mask                      Nullable(Int32),
    status                    Nullable(Int32),
    boxc                      Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.dlr',
    kafka_group_name           = 'ch-dlr-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.dlr_kafka_mv
TO bsmsc.dlr
AS
SELECT
    generateUUIDv4()  AS row_id,
    coalesce(smsc, '') AS smsc,
    coalesce(ts, '') AS ts,
    coalesce(destination, '') AS destination,
    coalesce(source, '') AS source,
    coalesce(service, '') AS service,
    coalesce(url, '') AS url,
    coalesce(mask, 0) AS mask,
    coalesce(status, 0) AS status,
    coalesce(boxc, '') AS boxc,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.dlr_kafka_queue
WHERE __op IS NOT NULL;   -- no PK: insert all non-null events;

-- ═══════════════════════════════════════════════════════════
-- TABLE: dlr_event  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.dlr_event
(
    id                        UInt64,
    event                     Int8,
    time                      Nullable(DateTime),
    date                      String,
    parts                     Int8,
    server_ref                String,
    reference                 Nullable(Int64),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.dlr_event_kafka_queue
(
    id                        Nullable(UInt64),
    event                     Nullable(Int8),
    time                      Nullable(String),
    date                      Nullable(String),
    parts                     Nullable(Int8),
    server_ref                Nullable(String),
    reference                 Nullable(Int64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.dlr_event',
    kafka_group_name           = 'ch-dlr-event-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.dlr_event_kafka_mv
TO bsmsc.dlr_event
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(event, 0) AS event,
    parseDateTimeBestEffortOrNull(time) AS time,
    coalesce(date, '') AS date,
    coalesce(parts, 1) AS parts,
    coalesce(server_ref, '') AS server_ref,
    coalesce(reference, 0) AS reference,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.dlr_event_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: download_queues  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.download_queues
(
    id                        Int32,
    user_id                   Int32,
    url                       String,
    download_file             String,
    type                      String,
    scheduled_time            DateTime,
    created_time              Nullable(DateTime),
    downloaded_at             Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.download_queues_kafka_queue
(
    id                        Nullable(Int32),
    user_id                   Nullable(Int32),
    url                       Nullable(String),
    download_file             Nullable(String),
    type                      Nullable(String),
    scheduled_time            Nullable(String),
    created_time              Nullable(String),
    downloaded_at             Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.download_queues',
    kafka_group_name           = 'ch-download-queues-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.download_queues_kafka_mv
TO bsmsc.download_queues
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(url, '') AS url,
    coalesce(download_file, '') AS download_file,
    coalesce(type, '') AS type,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    parseDateTimeBestEffortOrNull(created_time) AS created_time,
    parseDateTimeBestEffortOrNull(downloaded_at) AS downloaded_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.download_queues_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: errors  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.errors
(
    error                     UInt32,
    source                    String,
    description               String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (error)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.errors_kafka_queue
(
    error                     Nullable(UInt32),
    source                    Nullable(String),
    description               Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.errors',
    kafka_group_name           = 'ch-errors-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.errors_kafka_mv
TO bsmsc.errors
AS
SELECT
    coalesce(error, 0) AS error,
    coalesce(source, '') AS source,
    coalesce(description, '') AS description,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.errors_kafka_queue
WHERE error IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: esme  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.esme
(
    id                        Int32,
    system_id                 String,
    password                  String,
    throughput                Nullable(Int32),
    max_parallel_sessions     Nullable(Int32),
    source_ip                 String,
    charset                   String,
    bind_type                 String,
    user_id                   UInt64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.esme_kafka_queue
(
    id                        Nullable(Int32),
    system_id                 Nullable(String),
    password                  Nullable(String),
    throughput                Nullable(Int32),
    max_parallel_sessions     Nullable(Int32),
    source_ip                 Nullable(String),
    charset                   Nullable(String),
    bind_type                 Nullable(String),
    user_id                   Nullable(UInt64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.esme',
    kafka_group_name           = 'ch-esme-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.esme_kafka_mv
TO bsmsc.esme
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(system_id, '') AS system_id,
    coalesce(password, '') AS password,
    coalesce(throughput, 5) AS throughput,
    coalesce(max_parallel_sessions, 1) AS max_parallel_sessions,
    coalesce(source_ip, '') AS source_ip,
    coalesce(charset, 'iso') AS charset,
    coalesce(bind_type, 'transceiver') AS bind_type,
    coalesce(user_id, 0) AS user_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.esme_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: failed_jobs  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.failed_jobs
(
    id                        UInt64,
    uuid                      String,
    connection                String,
    queue                     String,
    payload                   String,
    exception                 String,
    failed_at                 DateTime,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.failed_jobs_kafka_queue
(
    id                        Nullable(UInt64),
    uuid                      Nullable(String),
    connection                Nullable(String),
    queue                     Nullable(String),
    payload                   Nullable(String),
    exception                 Nullable(String),
    failed_at                 Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.failed_jobs',
    kafka_group_name           = 'ch-failed-jobs-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.failed_jobs_kafka_mv
TO bsmsc.failed_jobs
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(uuid, '') AS uuid,
    coalesce(connection, '') AS connection,
    coalesce(queue, '') AS queue,
    coalesce(payload, '') AS payload,
    coalesce(exception, '') AS exception,
    parseDateTimeBestEffortOrNull(failed_at) AS failed_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.failed_jobs_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: inboxes  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.inboxes
(
    id                        UInt64,
    text                      String,
    is_valid                  Int8,
    reply_message             String,
    is_read                   Int8,
    is_important              Int8,
    oa                        String,
    shortcode                 Nullable(UInt64),
    created_at                Nullable(DateTime),
    mt_campaign_id            UInt32,
    corporation_id            Int32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.inboxes_kafka_queue
(
    id                        Nullable(UInt64),
    text                      Nullable(String),
    is_valid                  Nullable(Int8),
    reply_message             Nullable(String),
    is_read                   Nullable(Int8),
    is_important              Nullable(Int8),
    oa                        Nullable(String),
    shortcode                 Nullable(UInt64),
    created_at                Nullable(String),
    mt_campaign_id            Nullable(UInt32),
    corporation_id            Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.inboxes',
    kafka_group_name           = 'ch-inboxes-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.inboxes_kafka_mv
TO bsmsc.inboxes
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(text, '') AS text,
    coalesce(is_valid, 0) AS is_valid,
    coalesce(reply_message, '') AS reply_message,
    coalesce(is_read, 0) AS is_read,
    coalesce(is_important, 0) AS is_important,
    coalesce(oa, '') AS oa,
    coalesce(shortcode, 0) AS shortcode,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(mt_campaign_id, 0) AS mt_campaign_id,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.inboxes_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: incoming_sms  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.incoming_sms
(
    id                        UInt64,
    oa                        String,
    da                        Nullable(UInt64),
    msg                       String,
    received_time             DateTime,
    smsc                      String,
    processed                 Int8,
    comment                   String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.incoming_sms_kafka_queue
(
    id                        Nullable(UInt64),
    oa                        Nullable(String),
    da                        Nullable(UInt64),
    msg                       Nullable(String),
    received_time             Nullable(String),
    smsc                      Nullable(String),
    processed                 Nullable(Int8),
    comment                   Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.incoming_sms',
    kafka_group_name           = 'ch-incoming-sms-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.incoming_sms_kafka_mv
TO bsmsc.incoming_sms
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(oa, '') AS oa,
    coalesce(da, 0) AS da,
    coalesce(msg, '') AS msg,
    parseDateTimeBestEffortOrNull(received_time) AS received_time,
    coalesce(smsc, '') AS smsc,
    coalesce(processed, 0) AS processed,
    coalesce(comment, '') AS comment,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.incoming_sms_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: jobs  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.jobs
(
    id                        UInt64,
    queue                     String,
    payload                   String,
    attempts                  Int8,
    reserved_at               Nullable(UInt32),
    available_at              UInt32,
    created_at                UInt32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.jobs_kafka_queue
(
    id                        Nullable(UInt64),
    queue                     Nullable(String),
    payload                   Nullable(String),
    attempts                  Nullable(Int8),
    reserved_at               Nullable(UInt32),
    available_at              Nullable(UInt32),
    created_at                Nullable(UInt32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.jobs',
    kafka_group_name           = 'ch-jobs-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.jobs_kafka_mv
TO bsmsc.jobs
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(queue, '') AS queue,
    coalesce(payload, '') AS payload,
    coalesce(attempts, 0) AS attempts,
    coalesce(reserved_at, 0) AS reserved_at,
    coalesce(available_at, 0) AS available_at,
    coalesce(created_at, 0) AS created_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.jobs_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: language_lines  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.language_lines
(
    id                        UInt32,
    group                     String,
    key                       String,
    text                      String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.language_lines_kafka_queue
(
    id                        Nullable(UInt32),
    group                     Nullable(String),
    key                       Nullable(String),
    text                      Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.language_lines',
    kafka_group_name           = 'ch-language-lines-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.language_lines_kafka_mv
TO bsmsc.language_lines
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(group, '') AS group,
    coalesce(key, '') AS key,
    coalesce(text, '') AS text,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.language_lines_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: lists  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.lists
(
    id                        Int32,
    name                      String,
    type                      String,
    department_id             Int32,
    is_private                Int8,
    number_count              Int32,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.lists_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    type                      Nullable(String),
    department_id             Nullable(Int32),
    is_private                Nullable(Int8),
    number_count              Nullable(Int32),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.lists',
    kafka_group_name           = 'ch-lists-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.lists_kafka_mv
TO bsmsc.lists
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(type, 'user_defined') AS type,
    coalesce(department_id, 0) AS department_id,
    coalesce(is_private, 1) AS is_private,
    coalesce(number_count, 0) AS number_count,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.lists_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: lists_xxxk  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.lists_xxxk
(
    id                        Int32,
    name                      String,
    type                      String,
    department_id             Int32,
    is_private                Int8,
    number_count              Int32,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.lists_xxxk_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    type                      Nullable(String),
    department_id             Nullable(Int32),
    is_private                Nullable(Int8),
    number_count              Nullable(Int32),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.lists_xxxk',
    kafka_group_name           = 'ch-lists-xxxk-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.lists_xxxk_kafka_mv
TO bsmsc.lists_xxxk
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(type, 'user_defined') AS type,
    coalesce(department_id, 0) AS department_id,
    coalesce(is_private, 1) AS is_private,
    coalesce(number_count, 0) AS number_count,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.lists_xxxk_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: masks  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.masks
(
    id                        Int32,
    mask                      String,
    is_footer                 Int32,
    corporation_id            Int32,
    is_promotional            Int8,
    optout_keyword            String,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.masks_kafka_queue
(
    id                        Nullable(Int32),
    mask                      Nullable(String),
    is_footer                 Nullable(Int32),
    corporation_id            Nullable(Int32),
    is_promotional            Nullable(Int8),
    optout_keyword            Nullable(String),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.masks',
    kafka_group_name           = 'ch-masks-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.masks_kafka_mv
TO bsmsc.masks
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(mask, '') AS mask,
    coalesce(is_footer, 0) AS is_footer,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(is_promotional, 1) AS is_promotional,
    coalesce(optout_keyword, '') AS optout_keyword,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.masks_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: message_templates  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.message_templates
(
    id                        Int32,
    name                      String,
    template                  String,
    department_id             Int32,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.message_templates_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    template                  Nullable(String),
    department_id             Nullable(Int32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.message_templates',
    kafka_group_name           = 'ch-message-templates-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.message_templates_kafka_mv
TO bsmsc.message_templates
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(template, '') AS template,
    coalesce(department_id, 0) AS department_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.message_templates_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: migrations  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.migrations
(
    id                        UInt32,
    migration                 String,
    batch                     Int32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.migrations_kafka_queue
(
    id                        Nullable(UInt32),
    migration                 Nullable(String),
    batch                     Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.migrations',
    kafka_group_name           = 'ch-migrations-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.migrations_kafka_mv
TO bsmsc.migrations
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(migration, '') AS migration,
    coalesce(batch, 0) AS batch,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.migrations_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: model_has_permissions  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.model_has_permissions
(
    permission_id             UInt32,
    model_type                String,
    model_id                  UInt64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (permission_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.model_has_permissions_kafka_queue
(
    permission_id             Nullable(UInt32),
    model_type                Nullable(String),
    model_id                  Nullable(UInt64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.model_has_permissions',
    kafka_group_name           = 'ch-model-has-permissions-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.model_has_permissions_kafka_mv
TO bsmsc.model_has_permissions
AS
SELECT
    coalesce(permission_id, 0) AS permission_id,
    coalesce(model_type, '') AS model_type,
    coalesce(model_id, 0) AS model_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.model_has_permissions_kafka_queue
WHERE permission_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: model_has_roles  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.model_has_roles
(
    role_id                   UInt32,
    model_type                String,
    model_id                  UInt64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (role_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.model_has_roles_kafka_queue
(
    role_id                   Nullable(UInt32),
    model_type                Nullable(String),
    model_id                  Nullable(UInt64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.model_has_roles',
    kafka_group_name           = 'ch-model-has-roles-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.model_has_roles_kafka_mv
TO bsmsc.model_has_roles
AS
SELECT
    coalesce(role_id, 0) AS role_id,
    coalesce(model_type, '') AS model_type,
    coalesce(model_id, 0) AS model_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.model_has_roles_kafka_queue
WHERE role_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: mt_campaigns  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.mt_campaigns
(
    id                        UInt32,
    name                      String,
    valid_reply               String,
    valid_reply_message       String,
    invalid_reply_message     String,
    routing_id                UInt32,
    mask_id                   Int32,
    user_id                   UInt64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.mt_campaigns_kafka_queue
(
    id                        Nullable(UInt32),
    name                      Nullable(String),
    valid_reply               Nullable(String),
    valid_reply_message       Nullable(String),
    invalid_reply_message     Nullable(String),
    routing_id                Nullable(UInt32),
    mask_id                   Nullable(Int32),
    user_id                   Nullable(UInt64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.mt_campaigns',
    kafka_group_name           = 'ch-mt-campaigns-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.mt_campaigns_kafka_mv
TO bsmsc.mt_campaigns
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(valid_reply, '') AS valid_reply,
    coalesce(valid_reply_message, '') AS valid_reply_message,
    coalesce(invalid_reply_message, '') AS invalid_reply_message,
    coalesce(routing_id, 0) AS routing_id,
    coalesce(mask_id, 0) AS mask_id,
    coalesce(user_id, 0) AS user_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.mt_campaigns_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: mt_smpp  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.mt_smpp
(
    id                        Int32,
    routing_id                UInt32,
    user_id                   UInt64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.mt_smpp_kafka_queue
(
    id                        Nullable(Int32),
    routing_id                Nullable(UInt32),
    user_id                   Nullable(UInt64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.mt_smpp',
    kafka_group_name           = 'ch-mt-smpp-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.mt_smpp_kafka_mv
TO bsmsc.mt_smpp
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(routing_id, 0) AS routing_id,
    coalesce(user_id, 0) AS user_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.mt_smpp_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: mt_urls  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.mt_urls
(
    id                        UInt32,
    url                       String,
    routing_id                UInt32,
    method                    String,
    username                  String,
    password                  String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.mt_urls_kafka_queue
(
    id                        Nullable(UInt32),
    url                       Nullable(String),
    routing_id                Nullable(UInt32),
    method                    Nullable(String),
    username                  Nullable(String),
    password                  Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.mt_urls',
    kafka_group_name           = 'ch-mt-urls-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.mt_urls_kafka_mv
TO bsmsc.mt_urls
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(url, '') AS url,
    coalesce(routing_id, 0) AS routing_id,
    coalesce(method, '') AS method,
    coalesce(username, '') AS username,
    coalesce(password, '') AS password,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.mt_urls_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: numbers  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.numbers
(
    id                        Int32,
    number                    Nullable(Int64),
    list_id                   Int32,
    name                      String,
    param1                    String,
    param2                    String,
    param3                    String,
    param4                    String,
    param5                    String,
    param6                    String,
    param7                    String,
    param8                    String,
    param9                    String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.numbers_kafka_queue
(
    id                        Nullable(Int32),
    number                    Nullable(Int64),
    list_id                   Nullable(Int32),
    name                      Nullable(String),
    param1                    Nullable(String),
    param2                    Nullable(String),
    param3                    Nullable(String),
    param4                    Nullable(String),
    param5                    Nullable(String),
    param6                    Nullable(String),
    param7                    Nullable(String),
    param8                    Nullable(String),
    param9                    Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.numbers',
    kafka_group_name           = 'ch-numbers-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.numbers_kafka_mv
TO bsmsc.numbers
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(list_id, 0) AS list_id,
    coalesce(name, '') AS name,
    coalesce(param1, '') AS param1,
    coalesce(param2, '') AS param2,
    coalesce(param3, '') AS param3,
    coalesce(param4, '') AS param4,
    coalesce(param5, '') AS param5,
    coalesce(param6, '') AS param6,
    coalesce(param7, '') AS param7,
    coalesce(param8, '') AS param8,
    coalesce(param9, '') AS param9,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.numbers_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: oauth_access_tokens  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.oauth_access_tokens
(
    id                        String,
    user_id                   Nullable(Int64),
    client_id                 UInt32,
    name                      String,
    scopes                    String,
    revoked                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    expires_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.oauth_access_tokens_kafka_queue
(
    id                        Nullable(String),
    user_id                   Nullable(Int64),
    client_id                 Nullable(UInt32),
    name                      Nullable(String),
    scopes                    Nullable(String),
    revoked                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    expires_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.oauth_access_tokens',
    kafka_group_name           = 'ch-oauth-access-tokens-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.oauth_access_tokens_kafka_mv
TO bsmsc.oauth_access_tokens
AS
SELECT
    coalesce(id, '') AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(client_id, 0) AS client_id,
    coalesce(name, '') AS name,
    coalesce(scopes, '') AS scopes,
    coalesce(revoked, 0) AS revoked,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(expires_at) AS expires_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.oauth_access_tokens_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: oauth_auth_codes  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.oauth_auth_codes
(
    id                        String,
    user_id                   Int64,
    client_id                 UInt32,
    scopes                    String,
    revoked                   Int8,
    expires_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.oauth_auth_codes_kafka_queue
(
    id                        Nullable(String),
    user_id                   Nullable(Int64),
    client_id                 Nullable(UInt32),
    scopes                    Nullable(String),
    revoked                   Nullable(Int8),
    expires_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.oauth_auth_codes',
    kafka_group_name           = 'ch-oauth-auth-codes-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.oauth_auth_codes_kafka_mv
TO bsmsc.oauth_auth_codes
AS
SELECT
    coalesce(id, '') AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(client_id, 0) AS client_id,
    coalesce(scopes, '') AS scopes,
    coalesce(revoked, 0) AS revoked,
    parseDateTimeBestEffortOrNull(expires_at) AS expires_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.oauth_auth_codes_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: oauth_clients  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.oauth_clients
(
    id                        UInt32,
    user_id                   Nullable(Int64),
    name                      String,
    secret                    String,
    redirect                  String,
    personal_access_client    Int8,
    password_client           Int8,
    revoked                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.oauth_clients_kafka_queue
(
    id                        Nullable(UInt32),
    user_id                   Nullable(Int64),
    name                      Nullable(String),
    secret                    Nullable(String),
    redirect                  Nullable(String),
    personal_access_client    Nullable(Int8),
    password_client           Nullable(Int8),
    revoked                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.oauth_clients',
    kafka_group_name           = 'ch-oauth-clients-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.oauth_clients_kafka_mv
TO bsmsc.oauth_clients
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(name, '') AS name,
    coalesce(secret, '') AS secret,
    coalesce(redirect, '') AS redirect,
    coalesce(personal_access_client, 0) AS personal_access_client,
    coalesce(password_client, 0) AS password_client,
    coalesce(revoked, 0) AS revoked,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.oauth_clients_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: oauth_personal_access_clients  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.oauth_personal_access_clients
(
    id                        UInt32,
    client_id                 UInt32,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.oauth_personal_access_clients_kafka_queue
(
    id                        Nullable(UInt32),
    client_id                 Nullable(UInt32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.oauth_personal_access_clients',
    kafka_group_name           = 'ch-oauth-personal-access-clients-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.oauth_personal_access_clients_kafka_mv
TO bsmsc.oauth_personal_access_clients
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(client_id, 0) AS client_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.oauth_personal_access_clients_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: oauth_refresh_tokens  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.oauth_refresh_tokens
(
    id                        String,
    access_token_id           String,
    revoked                   Int8,
    expires_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.oauth_refresh_tokens_kafka_queue
(
    id                        Nullable(String),
    access_token_id           Nullable(String),
    revoked                   Nullable(Int8),
    expires_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.oauth_refresh_tokens',
    kafka_group_name           = 'ch-oauth-refresh-tokens-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.oauth_refresh_tokens_kafka_mv
TO bsmsc.oauth_refresh_tokens
AS
SELECT
    coalesce(id, '') AS id,
    coalesce(access_token_id, '') AS access_token_id,
    coalesce(revoked, 0) AS revoked,
    parseDateTimeBestEffortOrNull(expires_at) AS expires_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.oauth_refresh_tokens_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: otp  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.otp
(
    id                        UInt64,
    identifier                String,
    token                     String,
    validity                  Int32,
    valid                     Int8,
    deleted_at                Nullable(DateTime),
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    invalid_count             Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.otp_kafka_queue
(
    id                        Nullable(UInt64),
    identifier                Nullable(String),
    token                     Nullable(String),
    validity                  Nullable(Int32),
    valid                     Nullable(Int8),
    deleted_at                Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    invalid_count             Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.otp',
    kafka_group_name           = 'ch-otp-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.otp_kafka_mv
TO bsmsc.otp
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(identifier, '') AS identifier,
    coalesce(token, '') AS token,
    coalesce(validity, 0) AS validity,
    coalesce(valid, 1) AS valid,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(invalid_count, 0) AS invalid_count,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.otp_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: packages  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.packages
(
    id                        Int32,
    name                      String,
    description               String,
    credit_limit              Nullable(Float64),
    type                      String,
    enabled                   Int8,
    monthly_plan              Int8,
    monthly_commitment        Nullable(Float32),
    default_charge            Nullable(Float32),
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.packages_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    description               Nullable(String),
    credit_limit              Nullable(Float64),
    type                      Nullable(String),
    enabled                   Nullable(Int8),
    monthly_plan              Nullable(Int8),
    monthly_commitment        Nullable(Float32),
    default_charge            Nullable(Float32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.packages',
    kafka_group_name           = 'ch-packages-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.packages_kafka_mv
TO bsmsc.packages
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(description, '') AS description,
    coalesce(credit_limit, 0) AS credit_limit,
    coalesce(type, '') AS type,
    coalesce(enabled, 1) AS enabled,
    coalesce(monthly_plan, 0) AS monthly_plan,
    coalesce(monthly_commitment, 0) AS monthly_commitment,
    coalesce(default_charge, 0) AS default_charge,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.packages_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: part  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.part
(
    id                        UInt64,
    status                    String,
    submitted_time            DateTime,
    final_status_time         Nullable(DateTime),
    vlr                       Nullable(Int64),
    msg_ref                   Nullable(Int64),
    msg_part                  Int8,
    error_code                String,
    record_id                 UInt64,
    dlr_processed             Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.part_kafka_queue
(
    id                        Nullable(UInt64),
    status                    Nullable(String),
    submitted_time            Nullable(String),
    final_status_time         Nullable(String),
    vlr                       Nullable(Int64),
    msg_ref                   Nullable(Int64),
    msg_part                  Nullable(Int8),
    error_code                Nullable(String),
    record_id                 Nullable(UInt64),
    dlr_processed             Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.part',
    kafka_group_name           = 'ch-part-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.part_kafka_mv
TO bsmsc.part
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(status, '') AS status,
    parseDateTimeBestEffortOrNull(submitted_time) AS submitted_time,
    parseDateTimeBestEffortOrNull(final_status_time) AS final_status_time,
    coalesce(vlr, 0) AS vlr,
    coalesce(msg_ref, 0) AS msg_ref,
    coalesce(msg_part, 0) AS msg_part,
    coalesce(error_code, '') AS error_code,
    coalesce(record_id, 0) AS record_id,
    coalesce(dlr_processed, 0) AS dlr_processed,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.part_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: password_histories  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.password_histories
(
    id                        Int32,
    user_id                   Int32,
    password                  String,
    created_at                DateTime,
    updated_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.password_histories_kafka_queue
(
    id                        Nullable(Int32),
    user_id                   Nullable(Int32),
    password                  Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.password_histories',
    kafka_group_name           = 'ch-password-histories-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.password_histories_kafka_mv
TO bsmsc.password_histories
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(password, '') AS password,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.password_histories_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: password_resets  [NO PRIMARY KEY → UUID applied]
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
-- ⚠️  No Primary Key detected in MySQL → UUID (row_id) auto-generated
CREATE TABLE IF NOT EXISTS bsmsc.password_resets
(
    row_id                    UUID DEFAULT generateUUIDv4(),
    email                     String,
    token                     String,
    created_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = MergeTree()   -- No PK: UUID used, append-only (no dedup)

ORDER BY (row_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.password_resets_kafka_queue
(
    email                     Nullable(String),
    token                     Nullable(String),
    created_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.password_resets',
    kafka_group_name           = 'ch-password-resets-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.password_resets_kafka_mv
TO bsmsc.password_resets
AS
SELECT
    generateUUIDv4()  AS row_id,
    coalesce(email, '') AS email,
    coalesce(token, '') AS token,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.password_resets_kafka_queue
WHERE __op IS NOT NULL;   -- no PK: insert all non-null events;

-- ═══════════════════════════════════════════════════════════
-- TABLE: payments  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.payments
(
    id                        Int64,
    time                      DateTime,
    payment                   Float64,
    billing_account_id        Int32,
    user_id                   Nullable(Int32),
    comment                   String,
    created_at                DateTime,
    updated_at                DateTime,
    op                        String DEFAULT 'c'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.payments_kafka_queue
(
    id                        Nullable(Int64),
    time                      Nullable(String),
    payment                   Nullable(Float64),
    billing_account_id        Nullable(Int32),
    user_id                   Nullable(Int32),
    comment                   Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.payments',
    kafka_group_name           = 'ch-payments-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.payments_kafka_mv
TO bsmsc.payments
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(time) AS time,
    coalesce(payment, 0) AS payment,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(user_id, 0) AS user_id,
    coalesce(comment, '') AS comment,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.payments_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: pending_approval_sms  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.pending_approval_sms
(
    id                        Int32,
    scheduled_time            Nullable(DateTime),
    request_id                Int64,
    campaign_type             String,
    file_name                 String,
    number_count              Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.pending_approval_sms_kafka_queue
(
    id                        Nullable(Int32),
    scheduled_time            Nullable(String),
    request_id                Nullable(Int64),
    campaign_type             Nullable(String),
    file_name                 Nullable(String),
    number_count              Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.pending_approval_sms',
    kafka_group_name           = 'ch-pending-approval-sms-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.pending_approval_sms_kafka_mv
TO bsmsc.pending_approval_sms
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    coalesce(request_id, 0) AS request_id,
    coalesce(campaign_type, '') AS campaign_type,
    coalesce(file_name, '') AS file_name,
    coalesce(number_count, 0) AS number_count,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.pending_approval_sms_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: pending_sms  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.pending_sms
(
    id                        Int32,
    scheduled_time            Nullable(DateTime),
    request_id                Int64,
    campaign_type             String,
    file_name                 String,
    updated_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.pending_sms_kafka_queue
(
    id                        Nullable(Int32),
    scheduled_time            Nullable(String),
    request_id                Nullable(Int64),
    campaign_type             Nullable(String),
    file_name                 Nullable(String),
    updated_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.pending_sms',
    kafka_group_name           = 'ch-pending-sms-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.pending_sms_kafka_mv
TO bsmsc.pending_sms
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    coalesce(request_id, 0) AS request_id,
    coalesce(campaign_type, '') AS campaign_type,
    coalesce(file_name, '') AS file_name,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.pending_sms_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: permission_groups  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.permission_groups
(
    id                        Int32,
    name                      String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.permission_groups_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.permission_groups',
    kafka_group_name           = 'ch-permission-groups-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.permission_groups_kafka_mv
TO bsmsc.permission_groups
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.permission_groups_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: permissions  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.permissions
(
    id                        UInt32,
    permission_group_id       Nullable(Int32),
    name                      String,
    guard_name                String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.permissions_kafka_queue
(
    id                        Nullable(UInt32),
    permission_group_id       Nullable(Int32),
    name                      Nullable(String),
    guard_name                Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.permissions',
    kafka_group_name           = 'ch-permissions-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.permissions_kafka_mv
TO bsmsc.permissions
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(permission_group_id, 0) AS permission_group_id,
    coalesce(name, '') AS name,
    coalesce(guard_name, '') AS guard_name,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.permissions_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: record  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.record
(
    id                        UInt64,
    number                    Nullable(Int64),
    mask                      String,
    content                   String,
    processed_time            Nullable(DateTime),
    status                    String,
    added_time                DateTime,
    is_high_priority          Int8,
    smsc                      Int8,
    request_id                Int64,
    parts                     Int8,
    error_code                String,
    is_unicode                Int8,
    character_count           Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.record_kafka_queue
(
    id                        Nullable(UInt64),
    number                    Nullable(Int64),
    mask                      Nullable(String),
    content                   Nullable(String),
    processed_time            Nullable(String),
    status                    Nullable(String),
    added_time                Nullable(String),
    is_high_priority          Nullable(Int8),
    smsc                      Nullable(Int8),
    request_id                Nullable(Int64),
    parts                     Nullable(Int8),
    error_code                Nullable(String),
    is_unicode                Nullable(Int8),
    character_count           Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.record',
    kafka_group_name           = 'ch-record-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.record_kafka_mv
TO bsmsc.record
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(mask, '') AS mask,
    coalesce(content, '') AS content,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, '') AS status,
    parseDateTimeBestEffortOrNull(added_time) AS added_time,
    coalesce(is_high_priority, 0) AS is_high_priority,
    coalesce(smsc, 0) AS smsc,
    coalesce(request_id, 0) AS request_id,
    coalesce(parts, 1) AS parts,
    coalesce(error_code, '') AS error_code,
    coalesce(is_unicode, 0) AS is_unicode,
    coalesce(character_count, 0) AS character_count,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.record_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: repeated_request_schedules  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.repeated_request_schedules
(
    id                        Int64,
    repeated_request_id       Int64,
    start_date                Date,
    end_date                  Date,
    scheduled_time            String,
    days_of_week              String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.repeated_request_schedules_kafka_queue
(
    id                        Nullable(Int64),
    repeated_request_id       Nullable(Int64),
    start_date                Nullable(String),
    end_date                  Nullable(String),
    scheduled_time            Nullable(String),
    days_of_week              Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.repeated_request_schedules',
    kafka_group_name           = 'ch-repeated-request-schedules-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.repeated_request_schedules_kafka_mv
TO bsmsc.repeated_request_schedules
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(repeated_request_id, 0) AS repeated_request_id,
    toDate(parseDateTimeBestEffortOrNull(start_date)) AS start_date,
    toDate(parseDateTimeBestEffortOrNull(end_date)) AS end_date,
    coalesce(scheduled_time, '') AS scheduled_time,
    coalesce(days_of_week, '') AS days_of_week,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.repeated_request_schedules_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: repeated_requests  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.repeated_requests
(
    id                        Int64,
    processed_time            Nullable(DateTime),
    status                    String,
    campaign_name             String,
    lists                     String,
    numbers                   String,
    mask_id                   Nullable(Int32),
    billing_account_id        Int32,
    sms_content               String,
    sms_count                 Nullable(Int32),
    is_high_priority          Int8,
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              String,
    default_param1            String,
    default_param2            String,
    default_param3            String,
    default_param4            String,
    default_param5            String,
    default_param6            String,
    default_param7            String,
    default_param8            String,
    default_param9            String,
    is_fallback               Int8,
    mclass                    Int8,
    ip                        String,
    scheduled_time            DateTime,
    sms_validity              Nullable(UInt32),
    created_at                DateTime,
    created_by                Nullable(Int32),
    campaign_type             String,
    file_name                 String,
    url                       String,
    notify_number             Nullable(Int64),
    smpp_msg_id               String,
    channel                   String,
    updated_by                Nullable(Int32),
    updated_at                Nullable(DateTime),
    delivery_report_request   Int8,
    optout                    Int8,
    promo_levy                Int8,
    billing_unit_id           Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.repeated_requests_kafka_queue
(
    id                        Nullable(Int64),
    processed_time            Nullable(String),
    status                    Nullable(String),
    campaign_name             Nullable(String),
    lists                     Nullable(String),
    numbers                   Nullable(String),
    mask_id                   Nullable(Int32),
    billing_account_id        Nullable(Int32),
    sms_content               Nullable(String),
    sms_count                 Nullable(Int32),
    is_high_priority          Nullable(Int8),
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              Nullable(String),
    default_param1            Nullable(String),
    default_param2            Nullable(String),
    default_param3            Nullable(String),
    default_param4            Nullable(String),
    default_param5            Nullable(String),
    default_param6            Nullable(String),
    default_param7            Nullable(String),
    default_param8            Nullable(String),
    default_param9            Nullable(String),
    is_fallback               Nullable(Int8),
    mclass                    Nullable(Int8),
    ip                        Nullable(String),
    scheduled_time            Nullable(String),
    sms_validity              Nullable(UInt32),
    created_at                Nullable(String),
    created_by                Nullable(Int32),
    campaign_type             Nullable(String),
    file_name                 Nullable(String),
    url                       Nullable(String),
    notify_number             Nullable(Int64),
    smpp_msg_id               Nullable(String),
    channel                   Nullable(String),
    updated_by                Nullable(Int32),
    updated_at                Nullable(String),
    delivery_report_request   Nullable(Int8),
    optout                    Nullable(Int8),
    promo_levy                Nullable(Int8),
    billing_unit_id           Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.repeated_requests',
    kafka_group_name           = 'ch-repeated-requests-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.repeated_requests_kafka_mv
TO bsmsc.repeated_requests
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, 'pending') AS status,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(lists, '') AS lists,
    coalesce(numbers, '') AS numbers,
    coalesce(mask_id, 0) AS mask_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(sms_content, '') AS sms_content,
    coalesce(sms_count, 1) AS sms_count,
    coalesce(is_high_priority, 0) AS is_high_priority,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(number_count, 1) AS number_count,
    coalesce(default_name, '') AS default_name,
    coalesce(default_param1, '') AS default_param1,
    coalesce(default_param2, '') AS default_param2,
    coalesce(default_param3, '') AS default_param3,
    coalesce(default_param4, '') AS default_param4,
    coalesce(default_param5, '') AS default_param5,
    coalesce(default_param6, '') AS default_param6,
    coalesce(default_param7, '') AS default_param7,
    coalesce(default_param8, '') AS default_param8,
    coalesce(default_param9, '') AS default_param9,
    coalesce(is_fallback, 0) AS is_fallback,
    coalesce(mclass, 1) AS mclass,
    coalesce(ip, '') AS ip,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    coalesce(sms_validity, 0) AS sms_validity,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(campaign_type, 'single_content') AS campaign_type,
    coalesce(file_name, '') AS file_name,
    coalesce(url, '') AS url,
    coalesce(notify_number, 0) AS notify_number,
    coalesce(smpp_msg_id, '') AS smpp_msg_id,
    coalesce(channel, 'web') AS channel,
    coalesce(updated_by, 0) AS updated_by,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(delivery_report_request, 0) AS delivery_report_request,
    coalesce(optout, 0) AS optout,
    coalesce(promo_levy, 0) AS promo_levy,
    coalesce(billing_unit_id, 0) AS billing_unit_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.repeated_requests_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: reporting_failed_jobs  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.reporting_failed_jobs
(
    id                        UInt64,
    uuid                      String,
    connection                String,
    queue                     String,
    payload                   String,
    exception                 String,
    failed_at                 DateTime,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.reporting_failed_jobs_kafka_queue
(
    id                        Nullable(UInt64),
    uuid                      Nullable(String),
    connection                Nullable(String),
    queue                     Nullable(String),
    payload                   Nullable(String),
    exception                 Nullable(String),
    failed_at                 Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.reporting_failed_jobs',
    kafka_group_name           = 'ch-reporting-failed-jobs-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.reporting_failed_jobs_kafka_mv
TO bsmsc.reporting_failed_jobs
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(uuid, '') AS uuid,
    coalesce(connection, '') AS connection,
    coalesce(queue, '') AS queue,
    coalesce(payload, '') AS payload,
    coalesce(exception, '') AS exception,
    parseDateTimeBestEffortOrNull(failed_at) AS failed_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.reporting_failed_jobs_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: reporting_jobs  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.reporting_jobs
(
    id                        UInt64,
    queue                     String,
    payload                   String,
    attempts                  Int8,
    reserved_at               Nullable(UInt32),
    available_at              UInt32,
    created_at                UInt32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.reporting_jobs_kafka_queue
(
    id                        Nullable(UInt64),
    queue                     Nullable(String),
    payload                   Nullable(String),
    attempts                  Nullable(Int8),
    reserved_at               Nullable(UInt32),
    available_at              Nullable(UInt32),
    created_at                Nullable(UInt32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.reporting_jobs',
    kafka_group_name           = 'ch-reporting-jobs-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.reporting_jobs_kafka_mv
TO bsmsc.reporting_jobs
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(queue, '') AS queue,
    coalesce(payload, '') AS payload,
    coalesce(attempts, 0) AS attempts,
    coalesce(reserved_at, 0) AS reserved_at,
    coalesce(available_at, 0) AS available_at,
    coalesce(created_at, 0) AS created_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.reporting_jobs_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: requests  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.requests
(
    id                        Int64,
    processed_time            Nullable(DateTime),
    status                    String,
    campaign_name             String,
    lists                     String,
    numbers                   String,
    mask_id                   Nullable(Int32),
    billing_account_id        Int32,
    sms_content               String,
    sms_count                 Nullable(Int32),
    is_high_priority          Int8,
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              String,
    default_param1            String,
    default_param2            String,
    default_param3            String,
    default_param4            String,
    default_param5            String,
    default_param6            String,
    default_param7            String,
    default_param8            String,
    default_param9            String,
    is_fallback               Int8,
    mclass                    Int8,
    ip                        String,
    scheduled_time            DateTime,
    sms_validity              Nullable(UInt32),
    created_at                DateTime,
    created_by                Nullable(Int32),
    campaign_type             String,
    file_name                 String,
    url                       String,
    notify_number             Nullable(Int64),
    smpp_msg_id               String,
    channel                   String,
    updated_by                Nullable(Int32),
    updated_at                Nullable(DateTime),
    delivery_report_request   Int8,
    optout                    Int8,
    promo_levy                Int8,
    billing_unit_id           Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.requests_kafka_queue
(
    id                        Nullable(Int64),
    processed_time            Nullable(String),
    status                    Nullable(String),
    campaign_name             Nullable(String),
    lists                     Nullable(String),
    numbers                   Nullable(String),
    mask_id                   Nullable(Int32),
    billing_account_id        Nullable(Int32),
    sms_content               Nullable(String),
    sms_count                 Nullable(Int32),
    is_high_priority          Nullable(Int8),
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              Nullable(String),
    default_param1            Nullable(String),
    default_param2            Nullable(String),
    default_param3            Nullable(String),
    default_param4            Nullable(String),
    default_param5            Nullable(String),
    default_param6            Nullable(String),
    default_param7            Nullable(String),
    default_param8            Nullable(String),
    default_param9            Nullable(String),
    is_fallback               Nullable(Int8),
    mclass                    Nullable(Int8),
    ip                        Nullable(String),
    scheduled_time            Nullable(String),
    sms_validity              Nullable(UInt32),
    created_at                Nullable(String),
    created_by                Nullable(Int32),
    campaign_type             Nullable(String),
    file_name                 Nullable(String),
    url                       Nullable(String),
    notify_number             Nullable(Int64),
    smpp_msg_id               Nullable(String),
    channel                   Nullable(String),
    updated_by                Nullable(Int32),
    updated_at                Nullable(String),
    delivery_report_request   Nullable(Int8),
    optout                    Nullable(Int8),
    promo_levy                Nullable(Int8),
    billing_unit_id           Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.requests',
    kafka_group_name           = 'ch-requests-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.requests_kafka_mv
TO bsmsc.requests
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, 'pending') AS status,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(lists, '') AS lists,
    coalesce(numbers, '') AS numbers,
    coalesce(mask_id, 0) AS mask_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(sms_content, '') AS sms_content,
    coalesce(sms_count, 1) AS sms_count,
    coalesce(is_high_priority, 0) AS is_high_priority,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(number_count, 1) AS number_count,
    coalesce(default_name, '') AS default_name,
    coalesce(default_param1, '') AS default_param1,
    coalesce(default_param2, '') AS default_param2,
    coalesce(default_param3, '') AS default_param3,
    coalesce(default_param4, '') AS default_param4,
    coalesce(default_param5, '') AS default_param5,
    coalesce(default_param6, '') AS default_param6,
    coalesce(default_param7, '') AS default_param7,
    coalesce(default_param8, '') AS default_param8,
    coalesce(default_param9, '') AS default_param9,
    coalesce(is_fallback, 0) AS is_fallback,
    coalesce(mclass, 1) AS mclass,
    coalesce(ip, '') AS ip,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    coalesce(sms_validity, 0) AS sms_validity,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(campaign_type, 'single_content') AS campaign_type,
    coalesce(file_name, '') AS file_name,
    coalesce(url, '') AS url,
    coalesce(notify_number, 0) AS notify_number,
    coalesce(smpp_msg_id, '') AS smpp_msg_id,
    coalesce(channel, 'web') AS channel,
    coalesce(updated_by, 0) AS updated_by,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(delivery_report_request, 0) AS delivery_report_request,
    coalesce(optout, 0) AS optout,
    coalesce(promo_levy, 0) AS promo_levy,
    coalesce(billing_unit_id, 0) AS billing_unit_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.requests_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: requests_20220328  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.requests_20220328
(
    id                        Int64,
    processed_time            Nullable(DateTime),
    status                    String,
    campaign_name             String,
    lists                     String,
    numbers                   String,
    mask_id                   Nullable(Int32),
    billing_account_id        Int32,
    sms_content               String,
    sms_count                 Nullable(Int32),
    is_high_priority          Int8,
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              String,
    default_param1            String,
    default_param2            String,
    default_param3            String,
    default_param4            String,
    default_param5            String,
    default_param6            String,
    default_param7            String,
    default_param8            String,
    default_param9            String,
    is_fallback               Int8,
    mclass                    Int8,
    ip                        String,
    scheduled_time            Nullable(DateTime),
    sms_validity              Nullable(UInt32),
    created_at                DateTime,
    created_by                Nullable(Int32),
    campaign_type             String,
    file_name                 String,
    url                       String,
    notify_number             Nullable(Int64),
    smpp_msg_id               String,
    channel                   String,
    updated_by                Nullable(Int32),
    updated_at                Nullable(DateTime),
    delivery_report_request   Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.requests_20220328_kafka_queue
(
    id                        Nullable(Int64),
    processed_time            Nullable(String),
    status                    Nullable(String),
    campaign_name             Nullable(String),
    lists                     Nullable(String),
    numbers                   Nullable(String),
    mask_id                   Nullable(Int32),
    billing_account_id        Nullable(Int32),
    sms_content               Nullable(String),
    sms_count                 Nullable(Int32),
    is_high_priority          Nullable(Int8),
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              Nullable(String),
    default_param1            Nullable(String),
    default_param2            Nullable(String),
    default_param3            Nullable(String),
    default_param4            Nullable(String),
    default_param5            Nullable(String),
    default_param6            Nullable(String),
    default_param7            Nullable(String),
    default_param8            Nullable(String),
    default_param9            Nullable(String),
    is_fallback               Nullable(Int8),
    mclass                    Nullable(Int8),
    ip                        Nullable(String),
    scheduled_time            Nullable(String),
    sms_validity              Nullable(UInt32),
    created_at                Nullable(String),
    created_by                Nullable(Int32),
    campaign_type             Nullable(String),
    file_name                 Nullable(String),
    url                       Nullable(String),
    notify_number             Nullable(Int64),
    smpp_msg_id               Nullable(String),
    channel                   Nullable(String),
    updated_by                Nullable(Int32),
    updated_at                Nullable(String),
    delivery_report_request   Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.requests_20220328',
    kafka_group_name           = 'ch-requests-20220328-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.requests_20220328_kafka_mv
TO bsmsc.requests_20220328
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, 'pending') AS status,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(lists, '') AS lists,
    coalesce(numbers, '') AS numbers,
    coalesce(mask_id, 0) AS mask_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(sms_content, '') AS sms_content,
    coalesce(sms_count, 1) AS sms_count,
    coalesce(is_high_priority, 0) AS is_high_priority,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(number_count, 1) AS number_count,
    coalesce(default_name, '') AS default_name,
    coalesce(default_param1, '') AS default_param1,
    coalesce(default_param2, '') AS default_param2,
    coalesce(default_param3, '') AS default_param3,
    coalesce(default_param4, '') AS default_param4,
    coalesce(default_param5, '') AS default_param5,
    coalesce(default_param6, '') AS default_param6,
    coalesce(default_param7, '') AS default_param7,
    coalesce(default_param8, '') AS default_param8,
    coalesce(default_param9, '') AS default_param9,
    coalesce(is_fallback, 0) AS is_fallback,
    coalesce(mclass, 1) AS mclass,
    coalesce(ip, '') AS ip,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    coalesce(sms_validity, 0) AS sms_validity,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(campaign_type, 'single_content') AS campaign_type,
    coalesce(file_name, '') AS file_name,
    coalesce(url, '') AS url,
    coalesce(notify_number, 0) AS notify_number,
    coalesce(smpp_msg_id, '') AS smpp_msg_id,
    coalesce(channel, 'web') AS channel,
    coalesce(updated_by, 0) AS updated_by,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(delivery_report_request, 0) AS delivery_report_request,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.requests_20220328_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: resellers  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.resellers
(
    id                        Int32,
    name                      String,
    email                     String,
    phone                     Nullable(Int64),
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    base_url                  String,
    login_logo_path           String,
    favicon_path              String,
    sender_email              String,
    sender_password           String,
    outgoing_server           String,
    outgoing_port             Nullable(Int32),
    theme_color               String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.resellers_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    email                     Nullable(String),
    phone                     Nullable(Int64),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    base_url                  Nullable(String),
    login_logo_path           Nullable(String),
    favicon_path              Nullable(String),
    sender_email              Nullable(String),
    sender_password           Nullable(String),
    outgoing_server           Nullable(String),
    outgoing_port             Nullable(Int32),
    theme_color               Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.resellers',
    kafka_group_name           = 'ch-resellers-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.resellers_kafka_mv
TO bsmsc.resellers
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(email, '') AS email,
    coalesce(phone, 0) AS phone,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(base_url, '') AS base_url,
    coalesce(login_logo_path, '') AS login_logo_path,
    coalesce(favicon_path, '') AS favicon_path,
    coalesce(sender_email, '') AS sender_email,
    coalesce(sender_password, '') AS sender_password,
    coalesce(outgoing_server, '') AS outgoing_server,
    coalesce(outgoing_port, 0) AS outgoing_port,
    coalesce(theme_color, '') AS theme_color,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.resellers_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: role_group_corporations  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.role_group_corporations
(
    id                        Int32,
    role_group_id             Int32,
    corporation_id            Int32,
    created_at                DateTime,
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.role_group_corporations_kafka_queue
(
    id                        Nullable(Int32),
    role_group_id             Nullable(Int32),
    corporation_id            Nullable(Int32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.role_group_corporations',
    kafka_group_name           = 'ch-role-group-corporations-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.role_group_corporations_kafka_mv
TO bsmsc.role_group_corporations
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(role_group_id, 0) AS role_group_id,
    coalesce(corporation_id, 0) AS corporation_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.role_group_corporations_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: role_groups  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.role_groups
(
    id                        Int32,
    name                      String,
    status                    Int8,
    is_global                 Int32,
    level                     Int32,
    created_at                DateTime,
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.role_groups_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    status                    Nullable(Int8),
    is_global                 Nullable(Int32),
    level                     Nullable(Int32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.role_groups',
    kafka_group_name           = 'ch-role-groups-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.role_groups_kafka_mv
TO bsmsc.role_groups
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(status, 1) AS status,
    coalesce(is_global, 0) AS is_global,
    coalesce(level, 0) AS level,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.role_groups_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: role_has_permissions  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.role_has_permissions
(
    permission_id             UInt32,
    role_id                   UInt32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (permission_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.role_has_permissions_kafka_queue
(
    permission_id             Nullable(UInt32),
    role_id                   Nullable(UInt32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.role_has_permissions',
    kafka_group_name           = 'ch-role-has-permissions-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.role_has_permissions_kafka_mv
TO bsmsc.role_has_permissions
AS
SELECT
    coalesce(permission_id, 0) AS permission_id,
    coalesce(role_id, 0) AS role_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.role_has_permissions_kafka_queue
WHERE permission_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: roles  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.roles
(
    id                        UInt32,
    name                      String,
    guard_name                String,
    role_group_id             Nullable(Int32),
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.roles_kafka_queue
(
    id                        Nullable(UInt32),
    name                      Nullable(String),
    guard_name                Nullable(String),
    role_group_id             Nullable(Int32),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.roles',
    kafka_group_name           = 'ch-roles-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.roles_kafka_mv
TO bsmsc.roles
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(guard_name, '') AS guard_name,
    coalesce(role_group_id, 0) AS role_group_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.roles_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: routings  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.routings
(
    id                        UInt32,
    shortcode                 Nullable(UInt64),
    keyword                   String,
    route_action              String,
    corporation_id            Int32,
    imsi                      String,
    update_location           Int8,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.routings_kafka_queue
(
    id                        Nullable(UInt32),
    shortcode                 Nullable(UInt64),
    keyword                   Nullable(String),
    route_action              Nullable(String),
    corporation_id            Nullable(Int32),
    imsi                      Nullable(String),
    update_location           Nullable(Int8),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.routings',
    kafka_group_name           = 'ch-routings-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.routings_kafka_mv
TO bsmsc.routings
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(shortcode, 0) AS shortcode,
    coalesce(keyword, '') AS keyword,
    coalesce(route_action, '') AS route_action,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(imsi, '') AS imsi,
    coalesce(update_location, 1) AS update_location,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.routings_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: running_campaigns  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.running_campaigns
(
    request_id                Int64,
    campaign_name             String,
    campaign_type             String,
    corporation_id            Nullable(Int32),
    billing_account_id        Nullable(Int32),
    department_id             Nullable(Int32),
    created_by                Nullable(Int32),
    scheduled_time            Nullable(DateTime),
    processed_time            Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (request_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.running_campaigns_kafka_queue
(
    request_id                Nullable(Int64),
    campaign_name             Nullable(String),
    campaign_type             Nullable(String),
    corporation_id            Nullable(Int32),
    billing_account_id        Nullable(Int32),
    department_id             Nullable(Int32),
    created_by                Nullable(Int32),
    scheduled_time            Nullable(String),
    processed_time            Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.running_campaigns',
    kafka_group_name           = 'ch-running-campaigns-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.running_campaigns_kafka_mv
TO bsmsc.running_campaigns
AS
SELECT
    coalesce(request_id, 0) AS request_id,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(campaign_type, '') AS campaign_type,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(created_by, 0) AS created_by,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.running_campaigns_kafka_queue
WHERE request_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: smpp_dlr  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.smpp_dlr
(
    id                        Int32,
    message_id                Nullable(Int64),
    date                      String,
    record_reference          Nullable(Int64),
    part                      Int8,
    parts                     Int8,
    dlr_status                String,
    deliver_sm_status         String,
    user_id                   UInt64,
    submitted_time            Nullable(DateTime),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.smpp_dlr_kafka_queue
(
    id                        Nullable(Int32),
    message_id                Nullable(Int64),
    date                      Nullable(String),
    record_reference          Nullable(Int64),
    part                      Nullable(Int8),
    parts                     Nullable(Int8),
    dlr_status                Nullable(String),
    deliver_sm_status         Nullable(String),
    user_id                   Nullable(UInt64),
    submitted_time            Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.smpp_dlr',
    kafka_group_name           = 'ch-smpp-dlr-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.smpp_dlr_kafka_mv
TO bsmsc.smpp_dlr
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(message_id, 0) AS message_id,
    coalesce(date, '') AS date,
    coalesce(record_reference, 0) AS record_reference,
    coalesce(part, 0) AS part,
    coalesce(parts, 0) AS parts,
    coalesce(dlr_status, '') AS dlr_status,
    coalesce(deliver_sm_status, '') AS deliver_sm_status,
    coalesce(user_id, 0) AS user_id,
    parseDateTimeBestEffortOrNull(submitted_time) AS submitted_time,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.smpp_dlr_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: smpp_incoming_sms  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.smpp_incoming_sms
(
    id                        UInt64,
    user_id                   Nullable(Int32),
    oa                        String,
    da                        String,
    received_time             Nullable(DateTime),
    msg                       String,
    processed_time            Nullable(DateTime),
    processed_status          Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.smpp_incoming_sms_kafka_queue
(
    id                        Nullable(UInt64),
    user_id                   Nullable(Int32),
    oa                        Nullable(String),
    da                        Nullable(String),
    received_time             Nullable(String),
    msg                       Nullable(String),
    processed_time            Nullable(String),
    processed_status          Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.smpp_incoming_sms',
    kafka_group_name           = 'ch-smpp-incoming-sms-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.smpp_incoming_sms_kafka_mv
TO bsmsc.smpp_incoming_sms
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(oa, '') AS oa,
    coalesce(da, '') AS da,
    parseDateTimeBestEffortOrNull(received_time) AS received_time,
    coalesce(msg, '') AS msg,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(processed_status, 0) AS processed_status,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.smpp_incoming_sms_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: smpp_temp_pending  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.smpp_temp_pending
(
    id                        Int32,
    uniq_id                   Nullable(Int64),
    recieved_time             Nullable(DateTime),
    user_id                   Nullable(Int32),
    system_id                 String,
    mask                      String,
    number                    String,
    sar_ref                   Nullable(Int32),
    sar_parts                 Nullable(Int32),
    sar_part                  Nullable(Int32),
    message                   String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.smpp_temp_pending_kafka_queue
(
    id                        Nullable(Int32),
    uniq_id                   Nullable(Int64),
    recieved_time             Nullable(String),
    user_id                   Nullable(Int32),
    system_id                 Nullable(String),
    mask                      Nullable(String),
    number                    Nullable(String),
    sar_ref                   Nullable(Int32),
    sar_parts                 Nullable(Int32),
    sar_part                  Nullable(Int32),
    message                   Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.smpp_temp_pending',
    kafka_group_name           = 'ch-smpp-temp-pending-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.smpp_temp_pending_kafka_mv
TO bsmsc.smpp_temp_pending
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(uniq_id, 0) AS uniq_id,
    parseDateTimeBestEffortOrNull(recieved_time) AS recieved_time,
    coalesce(user_id, 0) AS user_id,
    coalesce(system_id, '') AS system_id,
    coalesce(mask, '') AS mask,
    coalesce(number, '') AS number,
    coalesce(sar_ref, 0) AS sar_ref,
    coalesce(sar_parts, 0) AS sar_parts,
    coalesce(sar_part, 0) AS sar_part,
    coalesce(message, '') AS message,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.smpp_temp_pending_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: sms_hourly_summary  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.sms_hourly_summary
(
    summary_id                UInt64,
    hour                      Nullable(DateTime),
    corporation_id            Int32,
    operator                  String,
    sms_submitted             Nullable(UInt32),
    sms_delivered             Nullable(UInt32),
    sms_expired               Nullable(UInt32),
    sms_cancelled             Nullable(UInt32),
    sms_failed_internal       Nullable(UInt32),
    sms_failed_billing        Nullable(UInt32),
    sms_failed_submit         Nullable(UInt32),
    sms_failed_other          Nullable(UInt32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (summary_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.sms_hourly_summary_kafka_queue
(
    summary_id                Nullable(UInt64),
    hour                      Nullable(String),
    corporation_id            Nullable(Int32),
    operator                  Nullable(String),
    sms_submitted             Nullable(UInt32),
    sms_delivered             Nullable(UInt32),
    sms_expired               Nullable(UInt32),
    sms_cancelled             Nullable(UInt32),
    sms_failed_internal       Nullable(UInt32),
    sms_failed_billing        Nullable(UInt32),
    sms_failed_submit         Nullable(UInt32),
    sms_failed_other          Nullable(UInt32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.sms_hourly_summary',
    kafka_group_name           = 'ch-sms-hourly-summary-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.sms_hourly_summary_kafka_mv
TO bsmsc.sms_hourly_summary
AS
SELECT
    coalesce(summary_id, 0) AS summary_id,
    parseDateTimeBestEffortOrNull(hour) AS hour,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(operator, '') AS operator,
    coalesce(sms_submitted, 0) AS sms_submitted,
    coalesce(sms_delivered, 0) AS sms_delivered,
    coalesce(sms_expired, 0) AS sms_expired,
    coalesce(sms_cancelled, 0) AS sms_cancelled,
    coalesce(sms_failed_internal, 0) AS sms_failed_internal,
    coalesce(sms_failed_billing, 0) AS sms_failed_billing,
    coalesce(sms_failed_submit, 0) AS sms_failed_submit,
    coalesce(sms_failed_other, 0) AS sms_failed_other,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.sms_hourly_summary_kafka_queue
WHERE summary_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: sms_summaries  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.sms_summaries
(
    id                        UInt64,
    date                      DateTime,
    billing_account_id        Int32,
    request_id                Int64,
    corporation_id            Int32,
    mask                      String,
    operator                  String,
    submitted                 Nullable(UInt32),
    delivered                 Nullable(UInt32),
    expired                   Nullable(UInt32),
    cancelled                 Nullable(UInt32),
    blocked                   Nullable(UInt32),
    failed                    Nullable(UInt32),
    campaign_name             String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.sms_summaries_kafka_queue
(
    id                        Nullable(UInt64),
    date                      Nullable(String),
    billing_account_id        Nullable(Int32),
    request_id                Nullable(Int64),
    corporation_id            Nullable(Int32),
    mask                      Nullable(String),
    operator                  Nullable(String),
    submitted                 Nullable(UInt32),
    delivered                 Nullable(UInt32),
    expired                   Nullable(UInt32),
    cancelled                 Nullable(UInt32),
    blocked                   Nullable(UInt32),
    failed                    Nullable(UInt32),
    campaign_name             Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.sms_summaries',
    kafka_group_name           = 'ch-sms-summaries-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.sms_summaries_kafka_mv
TO bsmsc.sms_summaries
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(date) AS date,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(request_id, 0) AS request_id,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(mask, '') AS mask,
    coalesce(operator, '') AS operator,
    coalesce(submitted, 0) AS submitted,
    coalesce(delivered, 0) AS delivered,
    coalesce(expired, 0) AS expired,
    coalesce(cancelled, 0) AS cancelled,
    coalesce(blocked, 0) AS blocked,
    coalesce(failed, 0) AS failed,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.sms_summaries_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: smsc_queue  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.smsc_queue
(
    id                        Int32,
    msidn                     Nullable(Int64),
    mask                      String,
    tpdu                      String,
    submit_time               Nullable(DateTime),
    date                      String,
    record_reference          Nullable(Int64),
    billing_number            Nullable(Int64),
    part                      Nullable(Int32),
    request_id                Int64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.smsc_queue_kafka_queue
(
    id                        Nullable(Int32),
    msidn                     Nullable(Int64),
    mask                      Nullable(String),
    tpdu                      Nullable(String),
    submit_time               Nullable(String),
    date                      Nullable(String),
    record_reference          Nullable(Int64),
    billing_number            Nullable(Int64),
    part                      Nullable(Int32),
    request_id                Nullable(Int64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.smsc_queue',
    kafka_group_name           = 'ch-smsc-queue-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.smsc_queue_kafka_mv
TO bsmsc.smsc_queue
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(msidn, 0) AS msidn,
    coalesce(mask, '') AS mask,
    coalesce(tpdu, '') AS tpdu,
    parseDateTimeBestEffortOrNull(submit_time) AS submit_time,
    coalesce(date, '') AS date,
    coalesce(record_reference, 0) AS record_reference,
    coalesce(billing_number, 0) AS billing_number,
    coalesce(part, 0) AS part,
    coalesce(request_id, 0) AS request_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.smsc_queue_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: smsc_subscribers  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.smsc_subscribers
(
    msisdn                    Int64,
    next_retry                Nullable(Int64),
    status                    Nullable(Int32),
    priority                  Nullable(Int32),
    last_retry                Nullable(Int64),
    last_error                Nullable(Int32),
    retry_count               Nullable(Int32),
    absolute_retry_count      Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (msisdn)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.smsc_subscribers_kafka_queue
(
    msisdn                    Nullable(Int64),
    next_retry                Nullable(Int64),
    status                    Nullable(Int32),
    priority                  Nullable(Int32),
    last_retry                Nullable(Int64),
    last_error                Nullable(Int32),
    retry_count               Nullable(Int32),
    absolute_retry_count      Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.smsc_subscribers',
    kafka_group_name           = 'ch-smsc-subscribers-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.smsc_subscribers_kafka_mv
TO bsmsc.smsc_subscribers
AS
SELECT
    coalesce(msisdn, 0) AS msisdn,
    coalesce(next_retry, 0) AS next_retry,
    coalesce(status, 0) AS status,
    coalesce(priority, 0) AS priority,
    coalesce(last_retry, 0) AS last_retry,
    coalesce(last_error, 0) AS last_error,
    coalesce(retry_count, 0) AS retry_count,
    coalesce(absolute_retry_count, 0) AS absolute_retry_count,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.smsc_subscribers_kafka_queue
WHERE msisdn IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: supervisors  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.supervisors
(
    id                        UInt32,
    user_id                   UInt64,
    supervisor_id             Nullable(UInt32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.supervisors_kafka_queue
(
    id                        Nullable(UInt32),
    user_id                   Nullable(UInt64),
    supervisor_id             Nullable(UInt32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.supervisors',
    kafka_group_name           = 'ch-supervisors-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.supervisors_kafka_mv
TO bsmsc.supervisors
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(user_id, 0) AS user_id,
    coalesce(supervisor_id, 0) AS supervisor_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.supervisors_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: system_params  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.system_params
(
    id                        Int32,
    name                      String,
    value                     String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.system_params_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    value                     Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.system_params',
    kafka_group_name           = 'ch-system-params-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.system_params_kafka_mv
TO bsmsc.system_params
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(value, '') AS value,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.system_params_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: tariff_plans  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.tariff_plans
(
    id                        Int32,
    package_id                Int32,
    tariff_zone_id            Int32,
    prefix                    String,
    charge                    Nullable(Float64),
    enabled                   Int8,
    is_default                Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.tariff_plans_kafka_queue
(
    id                        Nullable(Int32),
    package_id                Nullable(Int32),
    tariff_zone_id            Nullable(Int32),
    prefix                    Nullable(String),
    charge                    Nullable(Float64),
    enabled                   Nullable(Int8),
    is_default                Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.tariff_plans',
    kafka_group_name           = 'ch-tariff-plans-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.tariff_plans_kafka_mv
TO bsmsc.tariff_plans
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(package_id, 0) AS package_id,
    coalesce(tariff_zone_id, 0) AS tariff_zone_id,
    coalesce(prefix, '') AS prefix,
    coalesce(charge, 0) AS charge,
    coalesce(enabled, 1) AS enabled,
    coalesce(is_default, 0) AS is_default,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.tariff_plans_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: tariff_zones  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.tariff_zones
(
    id                        Int32,
    name                      String,
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.tariff_zones_kafka_queue
(
    id                        Nullable(Int32),
    name                      Nullable(String),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.tariff_zones',
    kafka_group_name           = 'ch-tariff-zones-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.tariff_zones_kafka_mv
TO bsmsc.tariff_zones
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.tariff_zones_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: temporary_campaign_blacklists  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.temporary_campaign_blacklists
(
    id                        Int32,
    number                    Nullable(Int64),
    request_id                Int64,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.temporary_campaign_blacklists_kafka_queue
(
    id                        Nullable(Int32),
    number                    Nullable(Int64),
    request_id                Nullable(Int64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.temporary_campaign_blacklists',
    kafka_group_name           = 'ch-temporary-campaign-blacklists-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.temporary_campaign_blacklists_kafka_mv
TO bsmsc.temporary_campaign_blacklists
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(request_id, 0) AS request_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.temporary_campaign_blacklists_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: temporary_requests  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.temporary_requests
(
    id                        Int64,
    processed_time            Nullable(DateTime),
    status                    String,
    campaign_name             String,
    lists                     String,
    numbers                   String,
    mask_id                   Nullable(Int32),
    billing_account_id        Int32,
    sms_content               String,
    sms_count                 Nullable(Int32),
    is_high_priority          Int8,
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              String,
    default_param1            String,
    default_param2            String,
    default_param3            String,
    default_param4            String,
    default_param5            String,
    default_param6            String,
    default_param7            String,
    default_param8            String,
    default_param9            String,
    is_fallback               Int8,
    mclass                    Int8,
    ip                        String,
    scheduled_time            DateTime,
    sms_validity              Nullable(UInt32),
    created_at                DateTime,
    created_by                Nullable(Int32),
    campaign_type             String,
    file_name                 String,
    url                       String,
    notify_number             Nullable(Int64),
    smpp_msg_id               String,
    channel                   String,
    updated_by                Nullable(Int32),
    updated_at                Nullable(DateTime),
    delivery_report_request   Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)
PARTITION BY toYYYYMM(created_at)
ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.temporary_requests_kafka_queue
(
    id                        Nullable(Int64),
    processed_time            Nullable(String),
    status                    Nullable(String),
    campaign_name             Nullable(String),
    lists                     Nullable(String),
    numbers                   Nullable(String),
    mask_id                   Nullable(Int32),
    billing_account_id        Nullable(Int32),
    sms_content               Nullable(String),
    sms_count                 Nullable(Int32),
    is_high_priority          Nullable(Int8),
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    number_count              Nullable(Int32),
    default_name              Nullable(String),
    default_param1            Nullable(String),
    default_param2            Nullable(String),
    default_param3            Nullable(String),
    default_param4            Nullable(String),
    default_param5            Nullable(String),
    default_param6            Nullable(String),
    default_param7            Nullable(String),
    default_param8            Nullable(String),
    default_param9            Nullable(String),
    is_fallback               Nullable(Int8),
    mclass                    Nullable(Int8),
    ip                        Nullable(String),
    scheduled_time            Nullable(String),
    sms_validity              Nullable(UInt32),
    created_at                Nullable(String),
    created_by                Nullable(Int32),
    campaign_type             Nullable(String),
    file_name                 Nullable(String),
    url                       Nullable(String),
    notify_number             Nullable(Int64),
    smpp_msg_id               Nullable(String),
    channel                   Nullable(String),
    updated_by                Nullable(Int32),
    updated_at                Nullable(String),
    delivery_report_request   Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.temporary_requests',
    kafka_group_name           = 'ch-temporary-requests-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.temporary_requests_kafka_mv
TO bsmsc.temporary_requests
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(processed_time) AS processed_time,
    coalesce(status, 'pending') AS status,
    coalesce(campaign_name, '') AS campaign_name,
    coalesce(lists, '') AS lists,
    coalesce(numbers, '') AS numbers,
    coalesce(mask_id, 0) AS mask_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(sms_content, '') AS sms_content,
    coalesce(sms_count, 1) AS sms_count,
    coalesce(is_high_priority, 0) AS is_high_priority,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(number_count, 1) AS number_count,
    coalesce(default_name, '') AS default_name,
    coalesce(default_param1, '') AS default_param1,
    coalesce(default_param2, '') AS default_param2,
    coalesce(default_param3, '') AS default_param3,
    coalesce(default_param4, '') AS default_param4,
    coalesce(default_param5, '') AS default_param5,
    coalesce(default_param6, '') AS default_param6,
    coalesce(default_param7, '') AS default_param7,
    coalesce(default_param8, '') AS default_param8,
    coalesce(default_param9, '') AS default_param9,
    coalesce(is_fallback, 0) AS is_fallback,
    coalesce(mclass, 1) AS mclass,
    coalesce(ip, '') AS ip,
    parseDateTimeBestEffortOrNull(scheduled_time) AS scheduled_time,
    coalesce(sms_validity, 0) AS sms_validity,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(campaign_type, 'single_content') AS campaign_type,
    coalesce(file_name, '') AS file_name,
    coalesce(url, '') AS url,
    coalesce(notify_number, 0) AS notify_number,
    coalesce(smpp_msg_id, '') AS smpp_msg_id,
    coalesce(channel, 'web') AS channel,
    coalesce(updated_by, 0) AS updated_by,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    coalesce(delivery_report_request, 0) AS delivery_report_request,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.temporary_requests_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: test  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.test
(
    id                        Int32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.test_kafka_queue
(
    id                        Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.test',
    kafka_group_name           = 'ch-test-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.test_kafka_mv
TO bsmsc.test
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.test_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: test_numbers  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.test_numbers
(
    msisdn                    String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (msisdn)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.test_numbers_kafka_queue
(
    msisdn                    Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.test_numbers',
    kafka_group_name           = 'ch-test-numbers-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.test_numbers_kafka_mv
TO bsmsc.test_numbers
AS
SELECT
    coalesce(msisdn, '') AS msisdn,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.test_numbers_kafka_queue
WHERE msisdn IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: tmp_number_63ff773c4ad23  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.tmp_number_63ff773c4ad23
(
    id                        Int32,
    number                    Nullable(Int64),
    name                      String,
    param1                    String,
    param2                    String,
    param3                    String,
    param4                    String,
    param5                    String,
    param6                    String,
    param7                    String,
    param8                    String,
    param9                    String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.tmp_number_63ff773c4ad23_kafka_queue
(
    id                        Nullable(Int32),
    number                    Nullable(Int64),
    name                      Nullable(String),
    param1                    Nullable(String),
    param2                    Nullable(String),
    param3                    Nullable(String),
    param4                    Nullable(String),
    param5                    Nullable(String),
    param6                    Nullable(String),
    param7                    Nullable(String),
    param8                    Nullable(String),
    param9                    Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.tmp_number_63ff773c4ad23',
    kafka_group_name           = 'ch-tmp-number-63ff773c4ad23-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.tmp_number_63ff773c4ad23_kafka_mv
TO bsmsc.tmp_number_63ff773c4ad23
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(name, '') AS name,
    coalesce(param1, '') AS param1,
    coalesce(param2, '') AS param2,
    coalesce(param3, '') AS param3,
    coalesce(param4, '') AS param4,
    coalesce(param5, '') AS param5,
    coalesce(param6, '') AS param6,
    coalesce(param7, '') AS param7,
    coalesce(param8, '') AS param8,
    coalesce(param9, '') AS param9,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.tmp_number_63ff773c4ad23_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: tmp_numbers  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.tmp_numbers
(
    id                        Int32,
    number                    Nullable(Int64),
    name                      String,
    param1                    String,
    param2                    String,
    param3                    String,
    param4                    String,
    param5                    String,
    param6                    String,
    param7                    String,
    param8                    String,
    param9                    String,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.tmp_numbers_kafka_queue
(
    id                        Nullable(Int32),
    number                    Nullable(Int64),
    name                      Nullable(String),
    param1                    Nullable(String),
    param2                    Nullable(String),
    param3                    Nullable(String),
    param4                    Nullable(String),
    param5                    Nullable(String),
    param6                    Nullable(String),
    param7                    Nullable(String),
    param8                    Nullable(String),
    param9                    Nullable(String),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.tmp_numbers',
    kafka_group_name           = 'ch-tmp-numbers-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.tmp_numbers_kafka_mv
TO bsmsc.tmp_numbers
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(number, 0) AS number,
    coalesce(name, '') AS name,
    coalesce(param1, '') AS param1,
    coalesce(param2, '') AS param2,
    coalesce(param3, '') AS param3,
    coalesce(param4, '') AS param4,
    coalesce(param5, '') AS param5,
    coalesce(param6, '') AS param6,
    coalesce(param7, '') AS param7,
    coalesce(param8, '') AS param8,
    coalesce(param9, '') AS param9,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.tmp_numbers_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: transaction  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.transaction
(
    id                        Int32,
    time                      DateTime,
    start_balance             Nullable(Float64),
    end_balance               Nullable(Float64),
    billing_account_id        Int32,
    description               String,
    charge                    Nullable(Float64),
    request_id                Nullable(Int64),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.transaction_kafka_queue
(
    id                        Nullable(Int32),
    time                      Nullable(String),
    start_balance             Nullable(Float64),
    end_balance               Nullable(Float64),
    billing_account_id        Nullable(Int32),
    description               Nullable(String),
    charge                    Nullable(Float64),
    request_id                Nullable(Int64),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.transaction',
    kafka_group_name           = 'ch-transaction-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.transaction_kafka_mv
TO bsmsc.transaction
AS
SELECT
    coalesce(id, 0) AS id,
    parseDateTimeBestEffortOrNull(time) AS time,
    coalesce(start_balance, 0) AS start_balance,
    coalesce(end_balance, 0) AS end_balance,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(description, '') AS description,
    coalesce(charge, 0) AS charge,
    coalesce(request_id, 0) AS request_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.transaction_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: unit_based_tariff_plans  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.unit_based_tariff_plans
(
    id                        Int64,
    package_id                Int32,
    tariff_zone_id            Int32,
    billing_unit_id           Int64,
    onnet_charge              Nullable(Float64),
    offnet_charge             Nullable(Float64),
    idd_charge                Nullable(Float64),
    enabled                   Int8,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.unit_based_tariff_plans_kafka_queue
(
    id                        Nullable(Int64),
    package_id                Nullable(Int32),
    tariff_zone_id            Nullable(Int32),
    billing_unit_id           Nullable(Int64),
    onnet_charge              Nullable(Float64),
    offnet_charge             Nullable(Float64),
    idd_charge                Nullable(Float64),
    enabled                   Nullable(Int8),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.unit_based_tariff_plans',
    kafka_group_name           = 'ch-unit-based-tariff-plans-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.unit_based_tariff_plans_kafka_mv
TO bsmsc.unit_based_tariff_plans
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(package_id, 0) AS package_id,
    coalesce(tariff_zone_id, 0) AS tariff_zone_id,
    coalesce(billing_unit_id, 0) AS billing_unit_id,
    coalesce(onnet_charge, 0) AS onnet_charge,
    coalesce(offnet_charge, 0) AS offnet_charge,
    coalesce(idd_charge, 0) AS idd_charge,
    coalesce(enabled, 1) AS enabled,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.unit_based_tariff_plans_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: user_connection_types  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.user_connection_types
(
    user_id                   UInt64,
    connection_type_id        Int32,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (user_id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.user_connection_types_kafka_queue
(
    user_id                   Nullable(UInt64),
    connection_type_id        Nullable(Int32),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.user_connection_types',
    kafka_group_name           = 'ch-user-connection-types-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.user_connection_types_kafka_mv
TO bsmsc.user_connection_types
AS
SELECT
    coalesce(user_id, 0) AS user_id,
    coalesce(connection_type_id, 0) AS connection_type_id,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.user_connection_types_kafka_queue
WHERE user_id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: users  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.users
(
    id                        UInt64,
    name                      String,
    email                     String,
    phone                     Nullable(Int64),
    email_verified_at         Nullable(DateTime),
    password                  String,
    remember_token            String,
    skip_signature            Int8,
    skip_blacklist            Int8,
    skip_blackout_time        Int8,
    preferred_language        String,
    is_content_visible        Int8,
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    billing_account_id        Nullable(Int32),
    reseller_id               Nullable(Int32),
    url_campaign              Int8,
    enabled                   Int8,
    is_otp_enabled            Int8,
    read_only                 Int8,
    password_changed_at       DateTime,
    wrong_attempts            Int8,
    last_login                Nullable(DateTime),
    expire_date               Nullable(Date),
    comment                   String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    allow_idd                 Int8,
    allow_legacy_api          Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.users_kafka_queue
(
    id                        Nullable(UInt64),
    name                      Nullable(String),
    email                     Nullable(String),
    phone                     Nullable(Int64),
    email_verified_at         Nullable(String),
    password                  Nullable(String),
    remember_token            Nullable(String),
    skip_signature            Nullable(Int8),
    skip_blacklist            Nullable(Int8),
    skip_blackout_time        Nullable(Int8),
    preferred_language        Nullable(String),
    is_content_visible        Nullable(Int8),
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    billing_account_id        Nullable(Int32),
    reseller_id               Nullable(Int32),
    url_campaign              Nullable(Int8),
    enabled                   Nullable(Int8),
    is_otp_enabled            Nullable(Int8),
    read_only                 Nullable(Int8),
    password_changed_at       Nullable(String),
    wrong_attempts            Nullable(Int8),
    last_login                Nullable(String),
    expire_date               Nullable(String),
    comment                   Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    allow_idd                 Nullable(Int8),
    allow_legacy_api          Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.users',
    kafka_group_name           = 'ch-users-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.users_kafka_mv
TO bsmsc.users
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(email, '') AS email,
    coalesce(phone, 0) AS phone,
    parseDateTimeBestEffortOrNull(email_verified_at) AS email_verified_at,
    coalesce(password, '') AS password,
    coalesce(remember_token, '') AS remember_token,
    coalesce(skip_signature, 0) AS skip_signature,
    coalesce(skip_blacklist, 0) AS skip_blacklist,
    coalesce(skip_blackout_time, 0) AS skip_blackout_time,
    coalesce(preferred_language, '') AS preferred_language,
    coalesce(is_content_visible, 0) AS is_content_visible,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(reseller_id, 0) AS reseller_id,
    coalesce(url_campaign, 0) AS url_campaign,
    coalesce(enabled, 1) AS enabled,
    coalesce(is_otp_enabled, 0) AS is_otp_enabled,
    coalesce(read_only, 0) AS read_only,
    parseDateTimeBestEffortOrNull(password_changed_at) AS password_changed_at,
    coalesce(wrong_attempts, 0) AS wrong_attempts,
    parseDateTimeBestEffortOrNull(last_login) AS last_login,
    toDate(parseDateTimeBestEffortOrNull(expire_date)) AS expire_date,
    coalesce(comment, '') AS comment,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(allow_idd, 0) AS allow_idd,
    coalesce(allow_legacy_api, 0) AS allow_legacy_api,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.users_kafka_queue
WHERE id IS NOT NULL;

-- ═══════════════════════════════════════════════════════════
-- TABLE: users_20251002  
-- ═══════════════════════════════════════════════════════════

-- STEP 1: Real Storage Table
CREATE TABLE IF NOT EXISTS bsmsc.users_20251002
(
    id                        UInt64,
    name                      String,
    email                     String,
    phone                     Nullable(Int64),
    email_verified_at         Nullable(DateTime),
    password                  String,
    remember_token            String,
    skip_signature            Int8,
    skip_blacklist            Int8,
    skip_blackout_time        Int8,
    preferred_language        String,
    is_content_visible        Int8,
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    billing_account_id        Nullable(Int32),
    reseller_id               Nullable(Int32),
    url_campaign              Int8,
    enabled                   Int8,
    is_otp_enabled            Int8,
    read_only                 Int8,
    password_changed_at       DateTime,
    wrong_attempts            Int8,
    last_login                Nullable(DateTime),
    expire_date               Nullable(Date),
    comment                   String,
    created_at                Nullable(DateTime),
    updated_at                Nullable(DateTime),
    deleted_at                Nullable(DateTime),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    allow_idd                 Int8,
    allow_legacy_api          Int8,
    op                        String DEFAULT 'c',
    __ts_ms                   UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(__ts_ms)

ORDER BY (id)
SETTINGS index_granularity = 8192;

-- STEP 2: Kafka Queue Table (buffer - no storage)
CREATE TABLE IF NOT EXISTS bsmsc.users_20251002_kafka_queue
(
    id                        Nullable(UInt64),
    name                      Nullable(String),
    email                     Nullable(String),
    phone                     Nullable(Int64),
    email_verified_at         Nullable(String),
    password                  Nullable(String),
    remember_token            Nullable(String),
    skip_signature            Nullable(Int8),
    skip_blacklist            Nullable(Int8),
    skip_blackout_time        Nullable(Int8),
    preferred_language        Nullable(String),
    is_content_visible        Nullable(Int8),
    corporation_id            Nullable(Int32),
    department_id             Nullable(Int32),
    billing_account_id        Nullable(Int32),
    reseller_id               Nullable(Int32),
    url_campaign              Nullable(Int8),
    enabled                   Nullable(Int8),
    is_otp_enabled            Nullable(Int8),
    read_only                 Nullable(Int8),
    password_changed_at       Nullable(String),
    wrong_attempts            Nullable(Int8),
    last_login                Nullable(String),
    expire_date               Nullable(String),
    comment                   Nullable(String),
    created_at                Nullable(String),
    updated_at                Nullable(String),
    deleted_at                Nullable(String),
    created_by                Nullable(Int32),
    updated_by                Nullable(Int32),
    allow_idd                 Nullable(Int8),
    allow_legacy_api          Nullable(Int8),
    __op                      Nullable(String),
    __ts_ms                   Nullable(UInt64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'dbserver1.bsmsc.users_20251002',
    kafka_group_name           = 'ch-users-20251002-group',
    kafka_format               = 'JSONEachRow',
    kafka_skip_broken_messages = 100;

-- STEP 3: Materialized View (auto-trigger queue → real table)
CREATE MATERIALIZED VIEW IF NOT EXISTS bsmsc.users_20251002_kafka_mv
TO bsmsc.users_20251002
AS
SELECT
    coalesce(id, 0) AS id,
    coalesce(name, '') AS name,
    coalesce(email, '') AS email,
    coalesce(phone, 0) AS phone,
    parseDateTimeBestEffortOrNull(email_verified_at) AS email_verified_at,
    coalesce(password, '') AS password,
    coalesce(remember_token, '') AS remember_token,
    coalesce(skip_signature, 0) AS skip_signature,
    coalesce(skip_blacklist, 0) AS skip_blacklist,
    coalesce(skip_blackout_time, 0) AS skip_blackout_time,
    coalesce(preferred_language, '') AS preferred_language,
    coalesce(is_content_visible, 0) AS is_content_visible,
    coalesce(corporation_id, 0) AS corporation_id,
    coalesce(department_id, 0) AS department_id,
    coalesce(billing_account_id, 0) AS billing_account_id,
    coalesce(reseller_id, 0) AS reseller_id,
    coalesce(url_campaign, 0) AS url_campaign,
    coalesce(enabled, 1) AS enabled,
    coalesce(is_otp_enabled, 0) AS is_otp_enabled,
    coalesce(read_only, 0) AS read_only,
    parseDateTimeBestEffortOrNull(password_changed_at) AS password_changed_at,
    coalesce(wrong_attempts, 0) AS wrong_attempts,
    parseDateTimeBestEffortOrNull(last_login) AS last_login,
    toDate(parseDateTimeBestEffortOrNull(expire_date)) AS expire_date,
    coalesce(comment, '') AS comment,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    parseDateTimeBestEffortOrNull(updated_at) AS updated_at,
    parseDateTimeBestEffortOrNull(deleted_at) AS deleted_at,
    coalesce(created_by, 0) AS created_by,
    coalesce(updated_by, 0) AS updated_by,
    coalesce(allow_idd, 0) AS allow_idd,
    coalesce(allow_legacy_api, 0) AS allow_legacy_api,
    coalesce(__op, 'c') AS op,
    coalesce(__ts_ms, toUInt64(toUnixTimestamp64Milli(now64()))) AS __ts_ms
FROM bsmsc.users_20251002_kafka_queue
WHERE id IS NOT NULL;
