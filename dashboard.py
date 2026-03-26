"""
AI Papers – Streamlit Dashboard

Connects to the Neon PostgreSQL database and displays fundamental insights
about the ingested papers data.

Run:
    streamlit run dashboard.py
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from db import get_connection

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="AI Papers Dashboard",
    page_icon="📄",
    layout="wide",
)

st.title("📄 AI Papers Dashboard")
st.caption("Insights from the OpenAlex ingestion pipeline · Neon PostgreSQL · Published papers only (no future-dated pre-prints)")

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Fetching data from Neon …")
def load_papers() -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    openalex_id,
                    title,
                    publication_date,
                    publication_year,
                    language,
                    type,
                    journal_name,
                    is_oa,
                    oa_status,
                    cited_by_count,
                    fwci,
                    citation_percentile,
                    is_in_top_1_percent,
                    is_in_top_10_percent,
                    referenced_works_count,
                    author_count,
                    countries_distinct_count,
                    institutions_distinct_count,
                    primary_topic,
                    primary_subfield,
                    primary_field,
                    first_author_name,
                    is_retracted,
                    ingested_at
                FROM papers
                WHERE publication_date <= CURRENT_DATE
                ORDER BY publication_date DESC
                """
            )
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=cols)


try:
    df = load_papers()
except Exception as exc:
    st.error(f"Could not connect to the database: {exc}")
    st.stop()

if df.empty:
    st.warning("No papers found in the database. Run the pipeline first.")
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    fields = sorted(df["primary_field"].dropna().unique())
    selected_fields = st.multiselect("Primary field", fields, default=[])
    if selected_fields:
        df = df[df["primary_field"].isin(selected_fields)]

    subfields = sorted(df["primary_subfield"].dropna().unique())
    selected_subfields = st.multiselect("Primary subfield", subfields, default=[])
    if selected_subfields:
        df = df[df["primary_subfield"].isin(selected_subfields)]

    oa_options = sorted(df["oa_status"].dropna().unique())
    selected_oa = st.multiselect("Open-access status", oa_options, default=[])
    if selected_oa:
        df = df[df["oa_status"].isin(selected_oa)]

    st.divider()
    st.metric("Rows after filters", f"{len(df):,}")

# ── KPI metrics ───────────────────────────────────────────────────────────────

st.subheader("At a glance")

total = len(df)
oa_pct = df["is_oa"].mean() * 100 if total else 0
avg_citations = df["cited_by_count"].mean() if total else 0
avg_fwci = df["fwci"].dropna().mean() if total else 0
retracted = int(df["is_retracted"].sum()) if "is_retracted" in df.columns else 0
top10_pct = df["is_in_top_10_percent"].mean() * 100 if total else 0

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total papers", f"{total:,}")
col2.metric("Open access", f"{oa_pct:.1f}%")
col3.metric("Avg citations", f"{avg_citations:.1f}")
col4.metric("Avg FWCI", f"{avg_fwci:.2f}")
col5.metric("Top-10% papers", f"{top10_pct:.1f}%")
col6.metric("Retracted", f"{retracted:,}")

st.divider()

# ── Row 1: Publication trend  &  OA status breakdown ─────────────────────────

left, right = st.columns([2, 1])

with left:
    st.subheader("Papers by publication date")
    daily = (
        df.dropna(subset=["publication_date"])
        .groupby("publication_date")
        .size()
        .reset_index(name="count")
    )
    fig = px.bar(
        daily,
        x="publication_date",
        y="count",
        labels={"publication_date": "Date", "count": "Papers"},
        color_discrete_sequence=["#4f8ef7"],
    )
    fig.update_layout(margin=dict(t=10, b=10), height=300)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Open-access status")
    oa_counts = (
        df["oa_status"]
        .fillna("unknown")
        .value_counts()
        .rename("count")
        .reset_index()
    )
    fig = px.pie(
        oa_counts,
        names="oa_status",
        values="count",
        hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_layout(margin=dict(t=10, b=10), height=300, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 2: Primary field  &  Primary subfield ─────────────────────────────────

left2, right2 = st.columns([1, 1])

with left2:
    st.subheader("Top primary fields (excl. Computer Science)")
    field_counts = (
        df["primary_field"]
        .dropna()
        .loc[lambda s: s.str.lower() != "computer science"]
        .value_counts()
        .head(15)
        .rename("count")
        .reset_index()
    )
    fig = px.bar(
        field_counts.sort_values("count"),
        x="count",
        y="primary_field",
        orientation="h",
        labels={"primary_field": "", "count": "Papers"},
        color_discrete_sequence=["#60a5fa"],
    )
    fig.update_layout(margin=dict(t=10, b=10), height=420)
    st.plotly_chart(fig, use_container_width=True)

with right2:
    st.subheader("Top primary subfields (excl. Artificial Intelligence)")
    subfield_counts = (
        df["primary_subfield"]
        .dropna()
        .loc[lambda s: s.str.lower() != "artificial intelligence"]
        .value_counts()
        .head(15)
        .rename("count")
        .reset_index()
    )
    fig = px.bar(
        subfield_counts.sort_values("count"),
        x="count",
        y="primary_subfield",
        orientation="h",
        labels={"primary_subfield": "", "count": "Papers"},
        color_discrete_sequence=["#6ec6a0"],
    )
    fig.update_layout(margin=dict(t=10, b=10), height=420)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Row 3: Language breakdown  &  International collaboration ────────────────

left3, right3 = st.columns([1, 1])

with left3:
    st.subheader("Language breakdown")
    import langcodes

    def lang_display_name(code: str) -> str:
        code = code.strip()
        if not code or code == "unknown":
            return "Unknown"
        try:
            name = langcodes.Language.get(code).display_name()
            return name if name else code
        except Exception:
            return code

    lang_counts = (
        df["language"]
        .str.strip()
        .fillna("unknown")
        .value_counts()
        .head(10)
        .rename("count")
        .reset_index()
    )
    lang_counts["language"] = lang_counts["language"].apply(lang_display_name)
    fig = px.bar(
        lang_counts,
        x="language",
        y="count",
        labels={"language": "Language", "count": "Papers"},
        color_discrete_sequence=["#a78bfa"],
    )
    fig.update_layout(margin=dict(t=10, b=10), height=320)
    st.plotly_chart(fig, use_container_width=True)

with right3:
    st.subheader("International collaboration (2+ countries)")
    collab = df["countries_distinct_count"].dropna().astype(int)
    collab_counts = (
        collab[collab >= 2]
        .value_counts()
        .sort_index()
        .rename("count")
        .reset_index()
    )
    collab_counts["label"] = collab_counts["countries_distinct_count"].apply(
        lambda n: f"{n} countries"
    )
    fig = px.bar(
        collab_counts,
        x="label",
        y="count",
        labels={"label": "Distinct countries", "count": "Papers"},
        color_discrete_sequence=["#f472b6"],
    )
    fig.update_layout(margin=dict(t=10, b=10), height=320)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Top-cited papers table ────────────────────────────────────────────────────

st.subheader("Top 20 most-cited papers")
top_cited = (
    df[["title", "first_author_name", "publication_date", "journal_name",
        "cited_by_count", "fwci", "is_oa", "primary_subfield"]]
    .sort_values("cited_by_count", ascending=False)
    .head(20)
    .reset_index(drop=True)
)
top_cited.index += 1
top_cited.columns = [
    "Title", "First Author", "Published", "Journal",
    "Citations", "FWCI", "OA", "Subfield",
]
st.dataframe(top_cited, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
last_ingest = pd.to_datetime(df["ingested_at"]).max()
st.caption(f"Last pipeline run: {last_ingest.strftime('%Y-%m-%d %H:%M UTC') if pd.notna(last_ingest) else 'unknown'}  ·  Cache refreshes every 5 minutes.")
