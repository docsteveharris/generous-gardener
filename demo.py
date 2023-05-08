"""
Demonstrate how data is available to the developer
"""
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

from app.utils.db import get_sqlalchemy_engine

load_dotenv("./.env.dev")
engine = get_sqlalchemy_engine()

# Raw queries must be wrapped as sa.text() from v2.0
# https://stackoverflow.com/a/75464429/992999
query = sa.text("""
SELECT TOP (10) [horizon_datetime]
      ,[csn]
      ,[location_admission_datetime]
      ,[hl7_location]
      ,[log_datetime]
  FROM [dbo].[location_v1]
  ORDER BY [dbo].[location_v1].[horizon_datetime] DESC
""")

with engine.connect() as conn:
    df = pd.read_sql(query, conn)
df.head()
