import os
import duckdb
import sys

# Expected tables/views by schema
warehouse = {
    "fct_job_ads": ["job_description_id", "occupation_id", "auxilliary_id", "employer_id", "job_details_id"],
    "dim_occupation": ["occupation_id", "occupation", "occupation_group", "occupation_field"],
    "dim_job_details": ["job_details_id", "employment_type", "salary_type", "duration", "scope_of_work_min", "scope_of_work_max"],
    "dim_job_description": ["job_description_id", "headline", "description_text", "description_html"],
    "dim_employer": ["employer_id", "employer_name", "employer_workplace", "employer_organization_number",
                     "workplace_street_address", "workplace_region", "workplace_postcode", "workplace_city", "workplace_country"],
    "dim_auxilliary_attributes": ["auxilliary_id", "experience_required", "access_to_own_car", "driving_license_required"]
}

marts = {
    "mart_IT": ["job_description_id", "vacancies", "occupation", "occupation_field", "application_deadline",
                "headline", "employer_name", "employment_type", "salary_type", "duration", "workplace_region", "description_html", "occupation_group"],
    "mart_economics": ["job_description_id", "vacancies", "occupation", "occupation_field", "application_deadline",
                       "headline", "employer_name", "employment_type", "salary_type", "duration", "workplace_region", "description_html", "occupation_group"],
    "mart_construction": ["job_description_id", "vacancies", "occupation", "occupation_field", "application_deadline",
                          "headline", "employer_name", "employment_type", "salary_type", "duration", "workplace_region", "description_html", "occupation_group"]
}


def get_existing_objects(con, schema_name: str, object_type="table"):
    """Return a list of table or view names in the schema."""
    if object_type == "table":
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='{schema_name}' AND table_type='BASE TABLE'
        """
    else:
        query = f"""
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema='{schema_name}'
        """
    return [t[0] for t in con.execute(query).fetchall()]


def get_existing_columns(con, schema_name: str, table_name: str):
    """Return a list of column names for a given table or view."""
    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='{schema_name}' AND table_name='{table_name}'
    """
    return [c[0] for c in con.execute(query).fetchall()]


def check_object_exists(name: str, existing_objects: list, object_type="table"):
    """Check if the table/view exists."""
    if name not in existing_objects:
        print(f"ERROR: Missing {object_type} '{name}'")
        return False
    return True


def check_not_empty(con, schema_name: str, name: str, object_type="table"):
    """Check if the table/view is not empty."""
    count = con.execute(f"SELECT COUNT(*) FROM {schema_name}.{name}").fetchone()[0]
    if count == 0:
        print(f"ERROR: {object_type} '{name}' is empty")
        return False
    return True


def check_columns(con, schema_name: str, name: str, expected_columns: list):
    """Check for missing or unexpected columns."""
    ok = True
    existing_columns = get_existing_columns(con, schema_name, name)

    # Missing columns
    for col in expected_columns:
        if col not in existing_columns:
            print(f"ERROR: Missing column '{col}' in '{name}'")
            ok = False

    # Extra columns
    for col in existing_columns:
        if col not in expected_columns:
            print(f"ERROR: Unexpected column '{col}' in '{name}'")
            ok = False

    return ok


def validate_tables(con, schema_name: str, objects: dict, object_type="table"):
    """Main validation function that checks existence, non-empty, and columns."""
    ok = True
    existing_objects = get_existing_objects(con, schema_name, object_type)
    
    for name, expected_columns in objects.items():
        if not check_object_exists(name, existing_objects, object_type):
            ok = False
            continue
        if not check_not_empty(con, schema_name, name, object_type):
            ok = False
        if not check_columns(con, schema_name, name, expected_columns):
            ok = False

    return ok


def run_validations(con):
    ok_warehouse = validate_tables(con, "warehouse", warehouse, object_type="table")
    ok_marts = validate_tables(con, "marts", marts, object_type="view")

    if not (ok_warehouse and ok_marts):
        print("DuckDB validation FAILED.")
        sys.exit(1)

    print("DuckDB validation PASSED.")


if __name__ == "__main__":
    DB_PATH = "./data_warehouse/job_ads.duckdb"

    if not os.path.exists(DB_PATH):
        print(f"ERROR: DuckDB file not found at {DB_PATH}")
        sys.exit(1)

    con = duckdb.connect(DB_PATH)
    run_validations(con)
    con.close()
