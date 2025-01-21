import streamlit as st
import pandas as pd
import plotly.express as px
from scripts.db import db_engine

#get table from database
def load_data():
    engine = db_engine()
    query = "SELECT * FROM new_weather"
    df = pd.read_sql(query, engine)
    return df

def main():
    st.title("Weather Data Analysis Dashboard")
    st.write("This dashboard provide quick analysis of weahter data from a specific location")

    # Load data
    df = load_data()
    df_unique = df.drop_duplicates(subset=["day"])

    if st.checkbox("Show raw data"):
        st.write(df)

    st.subheader("Summary Table")
    summary_stats = df_unique[["daily_avg_temp", "daily_avg_humid", "daily_avg_wspeed", "daily_precip"]].describe()
    st.write(summary_stats)

    st.header("How does temperature vary over time?")
    fig = px.line(df, x="date", y="daily_avg_temp", title="Daily average Temperature")
    st.plotly_chart(fig)

    st.header("Which days had the highest rainfall?")
    fig = px.bar(df, x="date", y="daily_precip", title="Daily Precipitation")
    st.plotly_chart(fig)

    st.header("Is there a correlation between humidity and temperature?")
    fig = px.scatter(df_unique, x='daily_avg_humid', y='daily_avg_temp', trendline='ols')
    st.plotly_chart(fig)

    st.header("Which weather variables are strongly correlated?")
    numeric_cols = df.select_dtypes(include=["float", "int"]).columns
    corr = df[numeric_cols].corr()
    fig = px.imshow(corr, text_auto=True, title="Correlation Heatmap")
    st.plotly_chart(fig)

if __name__=="__main__":
    main()
