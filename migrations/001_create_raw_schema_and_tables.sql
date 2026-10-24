-- Migration: 001_create_raw_schema_and_tables.sql
-- Description: Initializes raw schema and landing tables matching exact CSV headers

CREATE SCHEMA IF NOT EXISTS raw;

-- Schema migration tracking table
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version VARCHAR(50) PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 1. Raw user profiles table (12 columns)
CREATE TABLE IF NOT EXISTS raw.info_user_data (
    uuid TEXT,
    gender TEXT,
    points TEXT,
    badges_cnt TEXT,
    first_login_date_tw TEXT,
    user_grade TEXT,
    user_city TEXT,
    has_teacher_cnt TEXT,
    is_self_coach TEXT,
    has_student_cnt TEXT,
    belongs_to_class_cnt TEXT,
    has_class_cnt TEXT
);

-- 2. Raw exercise catalog table (10 columns)
CREATE TABLE IF NOT EXISTS raw.info_content (
    ucid TEXT,
    content_pretty_name TEXT,
    content_kind TEXT,
    difficulty TEXT,
    subject TEXT,
    learning_stage TEXT,
    level1_id TEXT,
    level2_id TEXT,
    level3_id TEXT,
    level4_id TEXT
);

-- 3. Raw problem interaction telemetry table (14 columns)
CREATE TABLE IF NOT EXISTS raw.log_problem (
    timestamp_tw TEXT,
    uuid TEXT,
    ucid TEXT,
    upid TEXT,
    problem_number TEXT,
    exercise_problem_repeat_session TEXT,
    is_correct TEXT,
    total_sec_taken TEXT,
    total_attempt_cnt TEXT,
    used_hint_cnt TEXT,
    is_hint_used TEXT,
    is_downgrade TEXT,
    is_upgrade TEXT,
    level TEXT
);