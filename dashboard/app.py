import sqlite3
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import streamlit as st

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "olympics.db")
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "medals_by_year.csv")

st.set_page_config(page_title="Olympic Medals Dashboard", layout="wide")
st.title("Olympic Medals Dashboard")
st.markdown("**Research Question:** Do wealthier countries (GDP), larger countries, or host countries win more Olympic medals?")

sns.set_theme(style="whitegrid")


@st.cache_data
def load_db(query):
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


@st.cache_data
def load_final():
    return load_db("SELECT * FROM olympics_final")


@st.cache_data
def load_spark_csv():
    return pd.read_csv(CSV_PATH)


final_df = load_final()

# Sidebar filters
st.sidebar.header("Filters")
year_min, year_max = int(final_df["year"].min()), int(final_df["year"].max())
year_range = st.sidebar.slider("Year range", year_min, year_max, (year_min, year_max))
min_medals = st.sidebar.slider("Minimum medals (for top-N chart)", 0, 50, 0)
top_n = st.sidebar.slider("Top N countries", 5, 20, 10)

filtered = final_df[
    (final_df["year"] >= year_range[0]) & (final_df["year"] <= year_range[1])
]

# Row 1
col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Top {top_n} Countries by Total Medals")
    top_df = (
        filtered[filtered["medals_total"] >= min_medals]
        .groupby("country")["medals_total"]
        .sum()
        .nlargest(top_n)
        .reset_index()
        .rename(columns={"medals_total": "total_medals"})
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(x="total_medals", y="country", data=top_df, palette="magma", ax=ax)
    ax.set_xlabel("Total Medals")
    ax.set_ylabel("")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

with col2:
    st.subheader("Medal Inflation Over Time (PySpark)")
    spark_df = load_spark_csv()
    spark_filtered = spark_df[
        (spark_df["year"] >= year_range[0]) & (spark_df["year"] <= year_range[1])
    ]
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(x="year", y="total_medals", data=spark_filtered, marker="o", color="steelblue", ax=ax)
    ax.set_xlabel("Year")
    ax.set_ylabel("Total Medals Distributed")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

# Row 2
col3, col4 = st.columns(2)

with col3:
    st.subheader("Economic Factors vs. Medal Success")
    econ_cols = ["medals_total", "log_gdp", "log_population", "gdp_per_capita"]
    econ_data = filtered[econ_cols].dropna()
    if len(econ_data) > 1:
        fig, ax = plt.subplots(figsize=(7, 5))
        sns.heatmap(
            econ_data.corr(),
            annot=True,
            cmap="coolwarm",
            fmt=".2f",
            linewidths=0.5,
            vmin=-1,
            vmax=1,
            ax=ax,
        )
        plt.tight_layout()
        st.pyplot(fig)
        plt.close(fig)
    else:
        st.info("Not enough data for the selected filters.")

with col4:
    st.subheader("Host Country Effect on Average Medals")
    host_comp = (
        filtered.groupby("host_flag")["medals_total"]
        .mean()
        .reset_index()
    )
    host_comp["host_flag"] = host_comp["host_flag"].map({0: "Non-Host", 1: "Host Nation"})
    fig, ax = plt.subplots(figsize=(7, 5))
    sns.barplot(x="host_flag", y="medals_total", data=host_comp, palette="viridis", ax=ax)
    ax.set_ylabel("Average Medals Won")
    ax.set_xlabel("")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

# Summary statistics
st.subheader("Summary Statistics")
st.dataframe(
    filtered[["country", "year", "medals_total", "gdp_per_capita", "host_flag"]]
    .sort_values("medals_total", ascending=False)
    .head(50)
    .reset_index(drop=True),
    use_container_width=True,
)

st.caption("Data sources: Kaggle Olympic Medals Dataset · World Bank Open Data API")
