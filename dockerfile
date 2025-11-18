FROM python:3.11-slim

ENV DUCKDB_PATH=/app/data_warehouse/job_ads.duckdb
WORKDIR /app/dashboard

COPY dashboard/ /app/dashboard/
COPY data_warehouse/ /app/data_warehouse/

RUN pip install streamlit duckdb pandas

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501"]
