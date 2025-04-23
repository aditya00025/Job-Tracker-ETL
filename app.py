import streamlit as st
import pandas as pd
import psycopg2

# DB connection
# def get_connection():
#     return psycopg2.connect(
#         host="localhost",
#         database="job_tracker",
#         user="postgres",  
#         password="password" 
#     )

def get_connection():
    return psycopg2.connect(
        host=st.secrets["localhost"],
        dbname=st.secrets["job_tracker"],
        user=st.secrets["postgres"],
        password=st.secrets["password"],
        port=st.secrets["5432"]
    )

@st.cache_data
def fetch_data():
    conn = get_connection()
    query = "SELECT * FROM jobs;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Load data
df = fetch_data()

# Streamlit UI
st.title("💼 Job Tracker Dashboard")

# Filters
position = st.text_input("🔍 Search by Job Title")
tag = st.text_input("🏷️ Filter by Tag")
location = st.selectbox("📍 Choose Location", ["All"] + sorted(df["location"].dropna().unique().tolist()))

# Filter logic
if position:
    df = df[df["position"].str.contains(position, case=False)]

if tag:
    df = df[df["tags"].str.contains(tag, case=False)]

if location != "All":
    df = df[df["location"] == location]

st.write(f"Total jobs found: {len(df)}")
st.dataframe(df[["company", "position", "location", "tags", "salary", "date_posted", "url"]])

# Download option
st.download_button(
    label="⬇️ Download CSV",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="filtered_jobs.csv",
    mime="text/csv"
)




