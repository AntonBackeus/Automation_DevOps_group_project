import duckdb
import sys

"""
DuckDB Validation Script
------------------------
This script validates the structure and content of a DuckDB database produced by your dbt models.
It includes mart_IT, mart_economics, and mart_construction views.
Only structural and mart referential integrity checks are kept, since dbt handles other data correctness.

Exit codes:
0 = Success
1 = Validation failure
2 = Unexpected error (DB missing/unreadable)
"""

DB_PATH = "./data_warehouse/job_ads.duckdb"

import os

assert os.path.exists(DB_PATH), f"Database not found at {DB_PATH}"

EXPECTED_SCHEMAS = {
    "fct_job_ads": {
        "job_description_id": "INTEGER",
        "auxilliary_id": "VARCHAR",
        "employer_id": "VARCHAR",
        "job_details_id": "VARCHAR",
        "occupation_id": "VARCHAR",
        "vacancies": "INTEGER",
        "relevance": "DOUBLE",
        "application_deadline": "TIMESTAMP",
    },
    "dim_occupation": {
        "occupation_id": "VARCHAR",
        "occupation": "VARCHAR",
        "occupation_group": "VARCHAR",
        "occupation_field": "VARCHAR",
    },
    "dim_job_details": {
        "job_details_id": "VARCHAR",
        "employment_type": "VARCHAR",
        "salary_type": "VARCHAR",
        "duration": "VARCHAR",
        "scope_of_work_min": "INTEGER",
        "scope_of_work_max": "INTEGER",
    },
    "dim_job_description": {
        "job_description_id": "INTEGER",
        "headline": "VARCHAR",
        "description_text": "VARCHAR",
        "description_html": "VARCHAR",
    },
    "dim_employer": {
        "employer_id": "VARCHAR",
        "employer_name": "VARCHAR",
        "employer_workplace": "VARCHAR",
        "employer_organization_number": "VARCHAR",
        "workplace_street_address": "VARCHAR",
        "workplace_region": "VARCHAR",
        "workplace_postcode": "VARCHAR",
        "workplace_city": "VARCHAR",
        "workplace_country": "VARCHAR",
    },
    "dim_auxilliary_attributes": {
        "auxilliary_id": "VARCHAR",
        "experience_required": "VARCHAR",
        "access_to_own_car": "VARCHAR",
        "driving_license_required": "VARCHAR",
    },
    "mart_it": {
        "vacancies": "INTEGER",
        "occupation": "VARCHAR",
        "occupation_field": "VARCHAR",
        "application_deadline": "TIMESTAMP",
        "headline": "VARCHAR",
        "employer_name": "VARCHAR",
        "employment_type": "VARCHAR",
        "salary_type": "VARCHAR",
        "duration": "VARCHAR",
        "workplace_region": "VARCHAR",
        "job_description_id": "INTEGER",
        "description_html": "VARCHAR",
        "occupation_group": "VARCHAR",
    },
    "mart_economics": {
        "vacancies": "INTEGER",
        "occupation": "VARCHAR",
        "occupation_field": "VARCHAR",
        "application_deadline": "TIMESTAMP",
        "headline": "VARCHAR",
        "employer_name": "VARCHAR",
        "employment_type": "VARCHAR",
        "salary_type": "VARCHAR",
        "duration": "VARCHAR",
        "workplace_region": "VARCHAR",
        "job_description_id": "INTEGER",
        "description_html": "VARCHAR",
        "occupation_group": "VARCHAR",
    },
    "mart_construction": {
        "vacancies": "INTEGER",
        "occupation": "VARCHAR",
        "occupation_field": "VARCHAR",
        "application_deadline": "TIMESTAMP",
        "headline": "VARCHAR",
        "employer_name": "VARCHAR",
        "employment_type": "VARCHAR",
        "salary_type": "VARCHAR",
        "duration": "VARCHAR",
        "workplace_region": "VARCHAR",
        "job_description_id": "INTEGER",
        "description_html": "VARCHAR",
        "occupation_group": "VARCHAR",
    },
}

# Build DATA_TESTS dynamically
DATA_TESTS = []

# Not-empty checks for all tables/views defined in EXPECTED_SCHEMAS
for table in EXPECTED_SCHEMAS.keys():
    DATA_TESTS.append({
        "description": f"{table} should not be empty",
        "query": f"SELECT COUNT(*) > 0 FROM {table};",
        "expected": True,
    })

# Keep only mart referential integrity checks; remove dim_job_description -> fct_job_ads
DATA_TESTS.extend([
    {
        "description": "All job_description_ids in mart_IT must exist in fct_job_ads",
        "query": "SELECT COUNT(*) = 0 FROM mart_it m LEFT JOIN fct_job_ads f USING(job_description_id) WHERE f.job_description_id IS NULL;",
        "expected": True,
    },
    {
        "description": "All job_description_ids in mart_economics must exist in fct_job_ads",
        "query": "SELECT COUNT(*) = 0 FROM mart_economics m LEFT JOIN fct_job_ads f USING(job_description_id) WHERE f.job_description_id IS NULL;",
        "expected": True,
    },
    {
        "description": "All job_description_ids in mart_construction must exist in fct_job_ads",
        "query": "SELECT COUNT(*) = 0 FROM mart_construction m LEFT JOIN fct_job_ads f USING(job_description_id) WHERE f.job_description_id IS NULL;",
        "expected": True,
    },
])

# FUNCTIONS

def validate_table_exists(con, table):
    tables = {t[0] for t in con.execute("SHOW TABLES").fetchall()}
    if table not in tables:
        print(f"ERROR: Missing table '{table}'")
        return False
    return True


def validate_schema(con, table, expected_cols):
    try:
        info = con.execute(f"DESCRIBE {table}").fetchall()
        actual = {col[0]: col[1] for col in info}
    except Exception as e:
        print(f"ERROR: Could not DESCRIBE table '{table}': {e}")
        return False

    ok = True
    for col, expected_type in expected_cols.items():
        if col not in actual:
            print(f"ERROR: Column '{col}' missing from table '{table}'")
            ok = False
        else:
            if expected_type.lower() not in actual[col].lower():
                print(f"ERROR: Column '{col}' in '{table}' expected type '{expected_type}' but got '{actual[col]}'")
                ok = False
    return ok


def run_data_tests(con):
    success = True
    for test in DATA_TESTS:
        try:
            result = con.execute(test["query"]).fetchone()[0]
            if result != test["expected"]:
                print(f"DATA TEST FAILED: {test['description']}")
                print(f"Expected {test['expected']} but got {result}")
                success = False
        except Exception as e:
            print(f"ERROR running data test '{test['description']}': {e}")
            success = False
    return success


def main():
    try:
        con = duckdb.connect(DB_PATH)
    except Exception as e:
        print(f"ERROR: Could not open DuckDB at {DB_PATH}: {e}")
        sys.exit(2)

    success = True

    for table, schema in EXPECTED_SCHEMAS.items():
        if not validate_table_exists(con, table):
            success = False
            continue
        if not validate_schema(con, table, schema):
            success = False

    if not run_data_tests(con):
        success = False

    con.close()

    if success:
        print("DuckDB validation succeeded.")
        sys.exit(0)
    else:
        print("DuckDB validation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
