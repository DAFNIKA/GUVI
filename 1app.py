import streamlit as st
import sqlite3
import pandas as pd
import requests


def get_connection():
    return sqlite3.connect("artifacts.db")

def run_query(query):
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn)
    return df

def create_tables():
    with get_connection() as conn:
        cur = conn.cursor()
     
        cur.execute('''
            CREATE TABLE IF NOT EXISTS artifact_metadata (
                id INTEGER PRIMARY KEY,
                title TEXT,
                culture TEXT,
                period TEXT,
                century TEXT,
                medium TEXT,
                dimensions TEXT,
                description TEXT,
                department TEXT,
                classification TEXT,
                accessionyear INTEGER,
                accessionmethod TEXT
            )
        ''')
       
        cur.execute('''
            CREATE TABLE IF NOT EXISTS artifact_media (
                objectid INTEGER,
                imagecount INTEGER,
                mediacount INTEGER,
                colorcount INTEGER,
                rank INTEGER,
                datebegin INTEGER,
                dateend INTEGER,
                FOREIGN KEY(objectid) REFERENCES artifact_metadata(id)
            )
        ''')
       
        cur.execute('''
            CREATE TABLE IF NOT EXISTS artifact_colors (
                objectid INTEGER,
                color TEXT,
                spectrum TEXT,
                hue TEXT,
                percent REAL,
                css3 TEXT,
                FOREIGN KEY(objectid) REFERENCES artifact_metadata(id)
            )
        ''')
        conn.commit()

def insert_metadata(records):
    with get_connection() as conn:
        cur = conn.cursor()
        for r in records:
            cur.execute('''
                INSERT OR IGNORE INTO artifact_metadata
                (id, title, culture, period, century, medium, dimensions, description,
                department, classification, accessionyear, accessionmethod)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                r.get('id'),
                r.get('title'),
                r.get('culture'),
                r.get('period'),
                r.get('century'),
                r.get('medium'),
                r.get('dimensions'),
                r.get('description'),
                r.get('department'),
                r.get('classification'),
                r.get('accessionyear'),
                r.get('accessionmethod')
            ))
        conn.commit()

def insert_media(records):
    with get_connection() as conn:
        cur = conn.cursor()
        for r in records:
            cur.execute('''
                INSERT OR IGNORE INTO artifact_media
                (objectid, imagecount, mediacount, colorcount, rank, datebegin, dateend)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                r.get('objectid'),
                r.get('imagecount'),
                r.get('mediacount'),
                r.get('colorcount'),
                r.get('rank'),
                r.get('datebegin'),
                r.get('dateend')
            ))
        conn.commit()

def insert_colors(records):
    with get_connection() as conn:
        cur = conn.cursor()
        for r in records:
            cur.execute('''
                INSERT OR IGNORE INTO artifact_colors
                (objectid, color, spectrum, hue, percent, css3)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                r.get('objectid'),
                r.get('color'),
                r.get('spectrum'),
                r.get('hue'),
                r.get('percent'),
                r.get('css3')
            ))
        conn.commit()



API_KEY = "0ebcef01-70c7-4fc7-ab93-24a406b4fade"  
BASE_URL = "https://api.harvardartmuseums.org/object"

def fetch_artifacts_by_classification(classification, size=100, max_records=2500):
    """
    Fetch artifacts of a given classification using pagination.
    """
    records = []
    page = 1
    while len(records) < max_records:
        params = {
            "apikey": API_KEY,
            "classification": classification,
            "size": size,
            "page": page,
            "hasimage": 1
        }
        resp = requests.get(BASE_URL, params=params)
        if resp.status_code != 200:
            st.error(f"API Error: {resp.status_code}")
            break
        data = resp.json()
        records.extend(data.get('records', []))
        if not data.get('info', {}).get('next'):
           
            break
        page += 1
    return records[:max_records]

def transform_records(records):
    """
    Transform raw API JSON records into three lists for metadata, media, colors.
    """
    metadata_list = []
    media_list = []
    colors_list = []

    for rec in records:
       
        metadata_list.append({
            "id": rec.get('id'),
            "title": rec.get('title'),
            "culture": rec.get('culture'),
            "period": rec.get('period'),
            "century": rec.get('century'),
            "medium": rec.get('medium'),
            "dimensions": rec.get('dimensions'),
            "description": rec.get('description'),
            "department": rec.get('department'),
            "classification": rec.get('classification'),
            "accessionyear": rec.get('accessionyear'),
            "accessionmethod": rec.get('accessionmethod')
        })

        
        media_list.append({
            "objectid": rec.get('id'),
            "imagecount": rec.get('imagecount'),
            "mediacount": rec.get('mediacount'),
            "colorcount": rec.get('colorcount'),
            "rank": rec.get('rank'),
            "datebegin": rec.get('datebegin'),
            "dateend": rec.get('dateend')
        })

       
        colors = rec.get('colors', [])
        for c in colors:
            colors_list.append({
                "objectid": rec.get('id'),
                "color": c.get('color'),
                "spectrum": c.get('spectrum'),
                "hue": c.get('hue'),
                "percent": c.get('percent'),
                "css3": c.get('css3')
            })

    return metadata_list, media_list, colors_list




st.set_page_config(page_title="Harvard Artifacts Explorer", layout="wide")
st.title("🏛️ Harvard Artifacts Explorer")
st.markdown("""
Explore Harvard Museum’s artifact collections using predefined SQL queries.
Select a query and hit 'Run Query' to see results.
""")

create_tables()  


st.sidebar.header("Data Collection")
classification = st.sidebar.selectbox("Choose classification", ["Vessels", "Prints", "Coins", "Paintings", "Drawings"])

if st.sidebar.button("Collect Data from API"):
    st.sidebar.info(f"Fetching up to 2500 records for {classification}...")
    raw_records = fetch_artifacts_by_classification(classification, size=100, max_records=2500)
    st.sidebar.success(f"Fetched {len(raw_records)} records.")
    metadata, media, colors = transform_records(raw_records)

    st.sidebar.info("Inserting records into the database...")
    insert_metadata(metadata)
    insert_media(media)
    insert_colors(colors)
    st.sidebar.success("Data insertion completed!")


st.markdown("## 🔍 Query & Visualization Section")
st.markdown("Use the dropdown to explore the dataset using SQL!")

query_dict = {
    
    "🏺 1. Artifacts from 11th century (Byzantine)": """
        SELECT * FROM artifact_metadata
        WHERE century LIKE '%11th%' AND culture LIKE '%Byzantine%'
        LIMIT 100;
    """,
    "🏺 2. Unique cultures represented": """
        SELECT DISTINCT culture FROM artifact_metadata
        ORDER BY culture;
    """,
    "🏺 3. Artifacts from Archaic Period": """
        SELECT * FROM artifact_metadata
        WHERE period LIKE '%Archaic%'
        LIMIT 100;
    """,
    "🏺 4. Artifact titles by accession year (desc)": """
        SELECT title, accessionyear FROM artifact_metadata
        ORDER BY accessionyear DESC
        LIMIT 100;
    """,
    "🏺 5. Artifact count per department": """
        SELECT department, COUNT(*) AS artifact_count
        FROM artifact_metadata
        GROUP BY department
        ORDER BY artifact_count DESC;
    """,

    
    "🖼️ 6. Artifacts with more than 3 images": """
        SELECT am.objectid, am.imagecount, a.title
        FROM artifact_media am
        JOIN artifact_metadata a ON am.objectid = a.id
        WHERE imagecount > 3
        LIMIT 100;
    """,
    "🖼️ 7. Average rank of all artifacts": """
        SELECT ROUND(AVG(rank), 2) AS avg_rank FROM artifact_media;
    """,
    "🖼️ 8. Media count > Color count": """
        SELECT am.objectid, am.mediacount, am.colorcount, a.title
        FROM artifact_media am
        JOIN artifact_metadata a ON am.objectid = a.id
        WHERE mediacount > colorcount
        LIMIT 100;
    """,
    "🖼️ 9. Artifacts created between 1500 and 1600": """
        SELECT am.objectid, am.datebegin, am.dateend, a.title
        FROM artifact_media am
        JOIN artifact_metadata a ON am.objectid = a.id
        WHERE datebegin >= 1500 AND dateend <= 1600
        LIMIT 100;
    """,
    "🖼️ 10. Artifacts with no media files": """
        SELECT am.objectid, a.title
        FROM artifact_media am
        JOIN artifact_metadata a ON am.objectid = a.id
        WHERE mediacount = 0 OR mediacount IS NULL;
    """,

    
    "🎨 11. Distinct hues used": """
        SELECT DISTINCT hue FROM artifact_colors
        ORDER BY hue;
    """,
    "🎨 12. Top 5 most used colors by frequency": """
        SELECT color, COUNT(*) AS usage_count
        FROM artifact_colors
        GROUP BY color
        ORDER BY usage_count DESC
        LIMIT 5;
    """,
    "🎨 13. Average coverage percentage per hue": """
        SELECT hue, ROUND(AVG(percent), 2) AS avg_coverage
        FROM artifact_colors
        GROUP BY hue
        ORDER BY avg_coverage DESC;
    """,
    "🎨 14. Colors used for given artifact ID": """
        SELECT objectid, color, hue, percent
        FROM artifact_colors
        WHERE objectid = 201391  -- replace with your ID
        LIMIT 50;
    """,
    "🎨 15. Total number of color entries": """
        SELECT COUNT(*) AS total_colors FROM artifact_colors;
    """,

    
    "🔗 16. Titles and hues for Byzantine culture": """
        SELECT m.title, c.hue
        FROM artifact_metadata m
        JOIN artifact_colors c ON m.id = c.objectid
        WHERE m.culture LIKE '%Byzantine%'
        LIMIT 100;
    """,
    "🔗 17. Each artifact title with associated hues": """
        SELECT m.title, c.hue
        FROM artifact_metadata m
        JOIN artifact_colors c ON m.id = c.objectid
        LIMIT 100;
    """,
    "🔗 18. Titles, cultures, media ranks (period not null)": """
        SELECT m.title, m.culture, am.rank
        FROM artifact_metadata m
        JOIN artifact_media am ON m.id = am.objectid
        WHERE m.period IS NOT NULL
        LIMIT 100;
    """,
    "🔗 19. Top 10 ranked artifacts with hue 'Grey'": """
        SELECT DISTINCT m.title, am.rank
        FROM artifact_metadata m
        JOIN artifact_media am ON m.id = am.objectid
        JOIN artifact_colors c ON m.id = c.objectid
        WHERE c.hue = 'Grey'
        ORDER BY am.rank DESC
        LIMIT 10;
    """,
    "🔗 20. Artifact count & avg media count per classification": """
        SELECT m.classification, COUNT(*) AS artifact_count, ROUND(AVG(am.mediacount),2) AS avg_media
        FROM artifact_metadata m
        JOIN artifact_media am ON m.id = am.objectid
        GROUP BY m.classification
        ORDER BY artifact_count DESC;
    """,
     "  21.Most common mediums used across all artifacts":"""
        SELECT medium, COUNT(*) AS count
        FROM artifact_metadata
        GROUP BY medium
        ORDER BY count DESC
        LIMIT 10;
    """,
    "   22.Departments with most Byzantine artifacts" :"""
        SELECT department, COUNT(*) AS count
        FROM artifact_metadata
        WHERE culture LIKE '%Byzantine%'
        GROUP BY department
        ORDER BY count DESC;
    """,
    "  23.How many artifacts were acquired as 'Gift'?"  :"""
       SELECT COUNT(*) AS gift_artifact_count
       FROM artifact_metadata
       WHERE accessionmethod LIKE '%Gift%';
    """,
    "  24.Hue distribution for 'Sculpture' classification" :"""
       SELECT c.hue, COUNT(*) AS count
       FROM artifact_metadata m
       JOIN artifact_colors c ON m.id = c.objectid
       WHERE m.classification LIKE '%Sculpture%'
    """,
    "  25.media data":"""
       SELECT * from artifact_media
       limit 10;
    """ 

}

selected_sql = st.selectbox("🔽 Select a predefined query", list(query_dict.keys()))
run = st.button("▶️ Run Query")

if run:
    st.markdown(f"### 📄 Results for: {selected_sql}")
    query = query_dict[selected_sql]
    df = run_query(query)

    if df.empty:
        st.warning("No results found.")
    else:
        st.dataframe(df)

      
        if selected_sql == "🏺 5. Artifact count per department":
            st.bar_chart(df.set_index("department")["artifact_count"])

        elif selected_sql == "🎨 12. Top 5 most used colors by frequency":
            st.bar_chart(df.set_index("color")["usage_count"])

        elif selected_sql == "🎨 13. Average coverage percentage per hue":
            st.bar_chart(df.set_index("hue")["avg_coverage"])
