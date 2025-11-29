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


def validate_tables(con, schema_name: str, objects: dict, object_type="table"):
    """
    Validate that all tables or views exist and are not empty.
    object_type: 'table' or 'view'
    """
    ok = True
    for name, columns in objects.items():
        if object_type == "table":
            existing = [t[0] for t in con.execute(f"SHOW TABLES IN {schema_name}").fetchall()]
        else:
            existing = [v[0] for v in con.execute(f"SHOW VIEWS IN {schema_name}").fetchall()]

        if name not in existing:
            print(f"ERROR: Missing {object_type} '{name}' in schema '{schema_name}'")
            ok = False
            continue

        result = con.execute(f"SELECT COUNT(*) FROM {schema_name}.{name}").fetchone()[0]
        if result == 0:
            print(f"ERROR: {object_type} '{name}' in schema '{schema_name}' is empty")
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
