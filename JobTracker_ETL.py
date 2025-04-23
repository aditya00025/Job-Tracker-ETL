import psycopg2
import pandas as pd
import requests

# Step 1: Extract data from the API
url = "https://remoteok.com/api"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
data = response.json()[1:]  # Skipping the metadata at index 0

# Step 2: Transform data into a list of dictionaries
jobs = []
for job in data:
    jobs.append({
        "job_id": str(job.get("id")),
        "date_posted": job.get("date"),
        "company": job.get("company"),
        "position": job.get("position"),
        "location": job.get("location", "").strip() or "not mentioned",
        "tags": ", ".join(job.get("tags", [])),
        "salary": job.get("salary", "").strip() or "Not Disclosed",
        "url": job.get("url")
    })

# Step 3: Load data into a pandas DataFrame
df = pd.DataFrame(jobs)

# Step 4: Connect to PostgreSQL and insert data
conn = psycopg2.connect(
    dbname="job_tracker", user="postgres", password="password",
    host="localhost", port="5432"
)
cursor = conn.cursor()

for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO jobs (job_id, date_posted, company, position, location, tags, salary, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_id) DO NOTHING;
    """, (row['job_id'], row['date_posted'], row['company'], row['position'],
          row['location'], row['tags'], row['salary'], row['url']))

conn.commit()
cursor.close()
conn.close()

print("ETL process completed successfully!")
print("Data loaded into PostgreSQL database successfully!")


# This code extracts job data from the RemoteOK API, transforms it into a structured format, and loads it into a PostgreSQL database.
# It uses the requests library to fetch data from the API, pandas for data manipulation, and psycopg2 for database interaction.