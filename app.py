import streamlit as st
import pandas as pd
import plotly.express as px

# General page configuration
st.set_page_config(
    page_title="Junyi Learning Analytics",
    page_icon="🎓",
    layout="wide"
)

# 1. Native PostgreSQL connection
conn = st.connection("postgresql", type="sql")

st.title("🎓 Junyi Academy — Learning Telemetry")
st.markdown("Analytics dashboard connected directly to the local Data Warehouse (Star Schema).")

# 2. Executive metrics (top KPIs)
kpi_query = """
    SELECT 
        COUNT(*) AS total_attempts,
        ROUND(AVG(time_taken_seconds), 1) AS avg_time_seconds,
        ROUND(AVG(hints_count), 2) AS avg_hints,
        ROUND(AVG(CASE WHEN is_first_attempt_correct THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_first_attempt_correct
    FROM marts.fct_student_problem_attempts;
"""
kpis = conn.query(kpi_query)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Interactions", f"{kpis['total_attempts'][0]:,}")
col2.metric("Average Time", f"{kpis['avg_time_seconds'][0]} s")
col3.metric("Average Hints", f"{kpis['avg_hints'][0]}")
col4.metric("% Correct on 1st Attempt", f"{kpis['pct_first_attempt_correct'][0]}%")

st.divider()

# 3. Analytical charts joining facts and dimensions
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Effort by Exercise Difficulty")
    # Join fct_student_problem_attempts with dim_content
    difficulty_query = """
        SELECT 
            COALESCE(c.difficulty, 'Unclassified') AS difficulty,
            ROUND(AVG(f.attempts_count), 2) AS avg_attempts,
            ROUND(AVG(f.hints_count), 2) AS avg_hints
        FROM marts.fct_student_problem_attempts f
        JOIN marts.dim_content c ON f.content_sk = c.content_sk
        GROUP BY c.difficulty
        ORDER BY avg_attempts DESC;
    """
    df_diff = conn.query(difficulty_query)
    
    fig_diff = px.bar(
        df_diff, 
        x="difficulty", 
        y=["avg_attempts", "avg_hints"],
        barmode="group",
        labels={"value": "Average", "variable": "Metric", "difficulty": "Difficulty"},
        color_discrete_sequence=["#1f77b4", "#ff7f0e"]
    )
    st.plotly_chart(fig_diff)

with col_right:
    st.subheader("Activity Distribution by Hour of Day")
    # Join fct_student_problem_attempts with dim_time
    time_query = """
        SELECT 
            t.hour_24,
            t.hour_12,
            t.day_part,
            COUNT(*) AS total_attempts
        FROM marts.fct_student_problem_attempts f
        JOIN marts.dim_time t ON f.time_sk = t.time_sk
        GROUP BY t.hour_24, t.hour_12, t.day_part
        ORDER BY t.hour_24;
    """
    df_time = conn.query(time_query)
    
    fig_time = px.line(
        df_time, 
        x="hour_12", 
        y="total_attempts",
        markers=True,
        labels={"hour_12": "Hour of Day", "total_attempts": "Interactions"},
        line_shape="spline"
    )
    st.plotly_chart(fig_time)

# 4. Interactive table by learning stage
st.subheader("Performance by Learning Stage")
stage_query = """
    SELECT 
        COALESCE(c.learning_stage, 'Unknown') AS learning_stage,
        COUNT(*) AS total_problems_attempted,
        ROUND(AVG(f.attempts_count), 2) AS avg_attempts,
        ROUND(AVG(f.time_taken_seconds), 1) AS avg_time_seconds,
        ROUND(AVG(CASE WHEN f.is_first_attempt_correct THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_first_attempt_correct
    FROM marts.fct_student_problem_attempts f
    JOIN marts.dim_content c ON f.content_sk = c.content_sk
    GROUP BY c.learning_stage
    ORDER BY total_problems_attempted DESC;
"""
df_stage = conn.query(stage_query)
st.dataframe(df_stage)

st.divider()

# 5. First-attempt success vs. retries per problem
st.subheader("🎯 How often do students get a problem right on the first try, and how many attempts do they make when they don't?")

st.caption(
    "In Junyi, `is_correct` means the student answered **correctly on the first attempt without hints**. "
    "The dataset does not record whether a problem was eventually solved, so retries show effort, not success."
)

col_metric, col_hist = st.columns([1, 3])

# Single scan of the fact table; KPIs are derived from these buckets in pandas.
# Rows with 0 attempts (no answer submitted) are excluded.
attempts_query = """
    SELECT 
        CASE 
            WHEN attempts_count >= 5 THEN '5+'
            ELSE attempts_count::TEXT 
        END AS attempts,
        LEAST(attempts_count, 5) AS sort_order,
        CASE WHEN is_correct THEN 'Correct on 1st attempt' ELSE 'Missed 1st attempt or used hints' END AS outcome,
        COUNT(*) AS total_problems,
        SUM(attempts_count) AS sum_attempts
    FROM marts.fct_student_problem_attempts
    WHERE attempts_count >= 1
    GROUP BY 1, 2, 3
    ORDER BY sort_order, outcome;
"""
df_attempts = conn.query(attempts_query)

is_first_ok = df_attempts["outcome"] == "Correct on 1st attempt"
retries = df_attempts[~is_first_ok & (df_attempts["sort_order"] >= 2)]
pct_first_ok = df_attempts.loc[is_first_ok, "total_problems"].sum() / df_attempts["total_problems"].sum() * 100
pct_retried = retries["total_problems"].sum() / df_attempts["total_problems"].sum() * 100
avg_retry_attempts = retries["sum_attempts"].sum() / retries["total_problems"].sum()

with col_metric:
    st.metric(
        label="Correct on 1st attempt",
        value=f"{pct_first_ok:.1f}%",
        help="Without hints, based on is_correct"
    )
    st.metric(
        label="Problems retried",
        value=f"{pct_retried:.1f}%",
        help="Problems with 2 or more attempts"
    )
    st.metric(
        label="Avg. attempts when retried",
        value=f"{avg_retry_attempts:.2f}",
        help="Problems with 2 or more attempts; the dataset does not say if they were eventually solved"
    )

with col_hist:
    fig_attempts = px.bar(
        df_attempts,
        x="attempts",
        y="total_problems",
        color="outcome",
        text_auto=".2s",
        title="Attempts per problem by first-attempt outcome",
        color_discrete_map={
            "Correct on 1st attempt": "#2ca02c",
            "Missed 1st attempt or used hints": "#d62728"
        }
    )
    fig_attempts.update_layout(
        xaxis_title="Number of attempts",
        yaxis_title="Problems",
        xaxis_type="category",
        legend_title_text=""
    )
    st.plotly_chart(fig_attempts)
