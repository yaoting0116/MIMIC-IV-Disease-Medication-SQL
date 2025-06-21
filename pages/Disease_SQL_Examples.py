import streamlit as st
import os
import re
import pandas as pd
from pandasql import sqldf

# --- Helper Functions (Common) ---

# Clean whitespace characters from all object columns in DataFrame
def clean_df(df):
    # Remove extra whitespace from string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: re.sub(r'\s+', '', x) if isinstance(x, str) else x)
    return df

# Convert only non-numeric columns with time/date keywords to datetime
def convert_time_columns(df):
    time_keywords = ['time', 'date', 'datetime']
    for col in df.columns:
        if any(keyword in col.lower() for keyword in time_keywords) and not pd.api.types.is_numeric_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                st.write(f"Warning: Failed to convert column '{col}': {e}")
    return df

# Convert specific columns (with_psychosis and E) to boolean type
def convert_bool_columns(df):
    if 'with_psychosis' in df.columns:
        df['with_psychosis'] = df['with_psychosis'].replace({'TRUE': True, 'FALSE': False})
        try:
            df['with_psychosis'] = df['with_psychosis'].astype(bool)
        except Exception as e:
            st.write(f"Warning: Cannot convert with_psychosis to boolean: {e}")
    if 'E' in df.columns:
        df['E'] = df['E'].replace({'TRUE': True, 'FALSE': False})
        try:
            df['E'] = df['E'].astype(bool)
        except Exception as e:
            st.write(f"Warning: Cannot convert E to boolean: {e}")
    return df

# --- Original simulate_delete (unchanged) ---
def simulate_delete(q, data_dict):
    q_upper = q.upper()
    if "DELETE FROM TEMP_EIGHT" in q_upper and "JOIN TEMP_NINE" in q_upper:
        if "temp_eight" in data_dict and "temp_nine" in data_dict:
            df_temp_eight = data_dict["temp_eight"]
            df_temp_nine = data_dict["temp_nine"]
            merged = pd.merge(df_temp_eight, df_temp_nine, on="subject_id", suffixes=("", "_nine"))
            to_delete = merged.loc[
                (merged["last_date"] == merged["index_date"]) & (merged["with_psychosis"].isin([True, 'TRUE'])),
                "subject_id"
            ].unique()
            filtered_df = df_temp_eight[~df_temp_eight["subject_id"].isin(to_delete)]
            data_dict["temp_eight"] = filtered_df
            st.write("✅ DELETE complete: Deleted subject_ids meeting the condition from temp_eight")
        else:
            st.write("Warning: Cannot find temp_eight or temp_nine in data_dict")
    elif "DELETE FROM TEMP_ELEVEN" in q_upper and "JOIN TEMP_TEN" in q_upper:
        if "temp_eleven" in data_dict and "temp_ten" in data_dict:
            df_temp_eleven = data_dict["temp_eleven"]
            df_temp_ten = data_dict["temp_ten"]
            merged = pd.merge(df_temp_eleven, df_temp_ten, on="subject_id", suffixes=("", "_ten"))
            to_delete = merged.loc[
                merged["first_date_ischemic_stroke"] == merged["index_date"],
                "subject_id"
            ].unique()
            filtered_df = df_temp_eleven[~df_temp_eleven["subject_id"].isin(to_delete)]
            data_dict["temp_eleven"] = filtered_df
            st.write("✅ DELETE complete: Deleted subject_ids meeting the condition from temp_eleven")
        else:
            st.write("Warning: Cannot find temp_eleven or temp_ten in data_dict")
    elif "DELETE FROM TEMP_EIGHTEEN" in q_upper and "NOT EXISTS" in q_upper:
        if "temp_eighteen" in data_dict:
            df_temp_eighteen = data_dict["temp_eighteen"]
            def should_delete(row):
                if row["with_psychosis"] in [False, 'FALSE']:
                    sub = df_temp_eighteen[
                        (df_temp_eighteen["gender"] == row["gender"]) &
                        (df_temp_eighteen["age"] == row["age"]) &
                        (df_temp_eighteen["with_psychosis"].isin([True, 'TRUE']))
                    ]
                    return sub.empty
                return False
            mask = df_temp_eighteen.apply(should_delete, axis=1)
            filtered_df = df_temp_eighteen[~mask]
            data_dict["temp_eighteen"] = filtered_df
            st.write("✅ DELETE complete: Deleted rows meeting the condition from temp_eighteen")
        else:
            st.write("Warning: Cannot find temp_eighteen in data_dict")
    else:
        pattern = r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s+IN\s+\(SELECT\s+(\w+)\s+FROM\s+(\w+)\)"
        match = re.match(pattern, q, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            col = match.group(2)
            inner_col = match.group(3)
            inner_table = match.group(4)
            if table_name in data_dict and inner_table in data_dict:
                df_table = data_dict[table_name]
                df_inner = data_dict[inner_table]
                filtered_df = df_table[~df_table[col].isin(df_inner[inner_col])]
                data_dict[table_name] = filtered_df
                st.write(f"✅ DELETE complete: Deleted rows from {table_name} where {col} exists in {inner_table}")
            else:
                st.write(f"Warning: Cannot find {table_name} or {inner_table} in data_dict")
        else:
            pattern_simple = r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+)"
            match_simple = re.match(pattern_simple, q, re.IGNORECASE)
            if match_simple:
                table_name = match_simple.group(1)
                condition = match_simple.group(2)
                if table_name in data_dict:
                    df_table = data_dict[table_name]
                    try:
                        mask = df_table.eval(condition)
                        filtered_df = df_table[~mask]
                        data_dict[table_name] = filtered_df
                        st.write(f"✅ DELETE complete: Deleted rows from {table_name} that meet the condition '{condition}'")
                    except Exception as e:
                        st.write(f"Warning: Delete operation failed, condition parsing error: {e}")
                else:
                    st.write(f"Warning: Cannot find {table_name} in data_dict")
            else:
                st.write("Warning: DELETE statement format not supported, currently only supports specific formats.")

# --- Disease-Specific SQL Execution Function ---
def execute_all_all(indices, alias_map, data_dict):
    """
    Execute SQL queries for disease-specific operations.
    This function captures messages printed during SQL execution (including from simulate_delete)
    by temporarily overriding st.write. The captured messages and data output are stored in session_state.
    """
    for i in indices:
        # Initialize log for this query
        st.session_state[f"query_message_{i}"] = []
        # Capture messages by overriding st.write locally
        original_write = st.write
        captured_messages = []
        def capture_write(*args, **kwargs):
            # Capture messages as strings
            msg = " ".join(str(arg) for arg in args)
            captured_messages.append(msg)
        st.write = capture_write

        sql_query = st.session_state[f"last_query_{i}"]
        # Replace table names using alias_map
        for original, alias in alias_map.items():
            sql_query = sql_query.replace(original, alias)
        queries = [q.strip() for q in sql_query.split(";") if q.strip()]
        for q in queries:
            q_upper = q.upper()
            if q_upper.startswith("DROP TABLE IF EXISTS"):
                drop_table_name = q.split()[-1]
                if drop_table_name in data_dict:
                    del data_dict[drop_table_name]
                    st.write(f"🗑️ `{drop_table_name}` DROP.")
            elif q_upper.startswith("CREATE TEMP TABLE") or q_upper.startswith("CREATE TABLE"):
                temp_table_name = q.split("AS")[0].split()[-1]
                temp_query = q.split("AS", 1)[1].strip()
                if temp_table_name in data_dict:
                    del data_dict[temp_table_name]
                result_df = sqldf(temp_query, {**locals(), **data_dict})
                result_df = convert_time_columns(result_df)
                data_dict[temp_table_name] = result_df
                st.write(f"✅ CREATE complete: Table {temp_table_name} created.")
            elif q_upper.startswith("ALTER TABLE"):
                tokens = q.split()
                table_name = tokens[2]
                col_match = re.search(r'ADD COLUMN\s+["\']?(\w+)["\']?\s+(\w+)', q, re.IGNORECASE)
                if col_match:
                    col_name = col_match.group(1)
                    if table_name in data_dict:
                        df = data_dict[table_name]
                        if col_name not in df.columns:
                            df[col_name] = None
                            data_dict[table_name] = df
                            st.write(f"📝 ALTER TABLE: Added column {col_name} to {table_name}.")
                    else:
                        st.write(f"Warning: Table {table_name} not found in data_dict.")
                else:
                    st.write("Warning: ALTER TABLE statement format not supported.")
            elif q_upper.startswith("UPDATE"):
                if "SET EVENT_DATE =" in q_upper and "FROM TEMP_FIFTEEN" in q_upper:
                    tokens = q.strip().split()
                    table_name = tokens[1]
                    if table_name in data_dict and "event_date" in data_dict[table_name].columns:
                        df = data_dict[table_name]
                        if "temp_fifteen" in data_dict:
                            df_fifteen = data_dict["temp_fifteen"]
                            if "subject_id" in df_fifteen.columns and "admit_year" in df_fifteen.columns:
                                def update_event_date(row):
                                    if pd.isnull(row["event_date"]):
                                        subject = row["subject_id"]
                                        match = df_fifteen[df_fifteen["subject_id"] == subject]
                                        if not match.empty:
                                            year_val = match.iloc[0]["admit_year"]
                                            return pd.to_datetime(f"{year_val}-12-31", errors='coerce')
                                    return row["event_date"]
                                df["event_date"] = df.apply(update_event_date, axis=1)
                                data_dict[table_name] = df
                                st.write(f"📝 UPDATE: event_date in {table_name} updated based on temp_fifteen.admit_year.")
                            else:
                                st.write("Warning: temp_fifteen missing subject_id or admit_year columns.")
                        else:
                            st.write("Warning: temp_fifteen table not found in data_dict.")
                    else:
                        st.write(f"Warning: Table {table_name} or event_date column not found in data_dict.")
                elif "SET EVENT_DATE =" in q_upper and "DEATH_DATE" in q_upper:
                    tokens = q.strip().split()
                    table_name = tokens[1]
                    if table_name in data_dict and "event_date" in data_dict[table_name].columns:
                        df = data_dict[table_name]
                        if "mimiciv_hosp_patients" in data_dict:
                            patients_df = data_dict["mimiciv_hosp_patients"]
                            patients_death = patients_df[pd.notnull(patients_df["dod"])].copy()
                            patients_death["death_date"] = pd.to_datetime(patients_death["dod"], errors='coerce').dt.date
                            def update_event_date(row):
                                if pd.isnull(row["event_date"]):
                                    subj = row["subject_id"]
                                    match = patients_death[patients_death["subject_id"] == subj]
                                    if not match.empty:
                                        return pd.to_datetime(match.iloc[0]["death_date"])
                                return row["event_date"]
                            df["event_date"] = df.apply(update_event_date, axis=1)
                            data_dict[table_name] = df
                            st.write(f"📝 UPDATE: event_date in {table_name} updated based on mimiciv_hosp_patients.dod.")
                        else:
                            st.write("Warning: mimiciv_hosp_patients table not found in data_dict.")
                    else:
                        st.write(f"Warning: Table {table_name} or event_date column not found in data_dict.")
                else:
                    tokens = q.strip().split()
                    table_name = tokens[1]
                    if table_name in data_dict:
                        df = data_dict[table_name]
                        if "event_date" in df.columns and "index_date" in df.columns:
                            df["event_date"] = pd.to_datetime(df["event_date"], errors='coerce')
                            df["index_date"] = pd.to_datetime(df["index_date"], errors='coerce')
                            diff = (df["event_date"] - df["index_date"]).dt.days
                            df["T"] = diff  # keep NA if calculation fails
                            data_dict[table_name] = df
                            st.write(f"📝 UPDATE: T column in {table_name} updated, type: {df['T'].dtype}.")
                        else:
                            st.write(f"Warning: event_date or index_date columns not found in {table_name}.")
                    else:
                        st.write(f"Warning: Table {table_name} not found in data_dict.")
            elif q_upper.startswith("DELETE"):
                # Call the original simulate_delete (which uses st.write)
                simulate_delete(q, data_dict)
            elif q_upper.startswith("SELECT"):
                result_df = sqldf(q, {**locals(), **data_dict})
                result_df = convert_time_columns(result_df)
                result_df = convert_bool_columns(result_df)
                if i == 27 and "admit_year" in result_df.columns:
                    try:
                        result_df["admit_year"] = result_df["admit_year"].astype(float)
                    except Exception as e:
                        st.write(f"Warning: Converting admit_year to float64 failed: {e}")
                result_df.index = range(1, len(result_df) + 1)
                st.session_state[f"query_result_{i}"] = result_df
                st.write("✅ SELECT complete: Query executed successfully.")
            else:
                st.write("Warning: Statement not supported. Only DROP, CREATE, ALTER, UPDATE, DELETE, and SELECT are supported.")

        # Restore original st.write and save captured messages
        st.write = original_write
        st.session_state[f"query_message_{i}"] = captured_messages
    st.rerun()

# --- Disease-Specific Web Display Function ---
def show():
    # Custom CSS for button styling
    st.markdown("""
    <style>
    div.stButton > button {
        width: auto !important;
        display: inline-block;
    }
    </style>
    """, unsafe_allow_html=True)

    # Directory containing the PKL disease data files
    PKL_DIR = f"MIMIC_IV_data/diseases_data"
    
    # Initialize session_state data dictionary if not present
    if "data_dict" not in st.session_state:
        st.session_state["data_dict"] = {}
    data_dict = st.session_state["data_dict"]

    # List all PKL files in the specified directory
    pkl_files = [f for f in os.listdir(PKL_DIR) if f.endswith(".pkl")]
    if not pkl_files:
        st.error(f"❌ No PKL files were found in `{PKL_DIR}`.")
        return

    # Define query names and detailed subtitles for each SQL query step
    query_names = [
        "SQL statement for the admissions table",
        "SQL statement for the diagnoses_icd table",
        "SQL statement for the patients table",
        "SQL statement for the diagnosis table",
        "SQL statement for the edstays table",
        "SQL statement for the diabetes_icd_codes table",
        "SQL statement for the heart_type_disease_icd_codes table",
        "SQL statement for the hemorrhagic_stroke_icd_codes table",
        "SQL statement for the hyperlipidemia_icd_codes table",
        "SQL statement for the hypertension_icd_codes table",
        "SQL statement for the ischemic_stroke_icd_codes table",
        "SQL statement for the neurological_type_disease_icd_codes table",
        "SQL statement for the psychosis_icd_codes table",
        "Step 1",
        "Step 2",
        "Step 3",
        "Step 4",
        "Step 5",
        "Step 6",
        "Step 7",
        "Step 8",
        "Step 9",
        "Step 10",
        "Step 11",
        "Step 12",
        "Step 13",
        "Step 14",
        "Step 15",
        "Step 16",
        "Step 17",
        "Step 18",
        "Step 19",
        "Step 20",
        "Step 21",
        "Step 22",
        "Step 23"
    ]

    query_names_subtitle = [
        # SQL Step One.
        """Use NATURAL JOIN to combine hosp (diagnoses_icd, admissions). 
        &#35; Produces the complete hosp table (IDs not deduplicated).""",
        # SQL Step Two.
        "Use DISTINCT to filter out duplicate subject_id. # Save hosp's subject_id to temp_two (extract required IDs).",
        # SQL Step Three.
        "Use NATURAL JOIN to combine ed (diagnosis, edstays). # Produces the complete ed table (IDs not deduplicated).",
        # SQL Step Four.
        """Use subject_id from temp_two (hosp) as a query condition for temp_three (ed). 
        &#35; Find in ed those with the same subject_id as in hosp.""",
        # SQL Step Five.
        """Use UNION ALL to merge temp_one (hosp) + temp_four (ed). 
        &#35; Produces the complete combined hosp + ed table (IDs not deduplicated).""",
        # SQL Step Six.
        """1.Use psychosis_icd_codes to query temp_five (case) for records of patients with psychosis.
            \n2.Use GROUP BY subject_id to group records, ensuring no duplicate subject_id.
            \n3.Use MIN(admit_date) to find the earliest admission date.""",
        # SQL Step Seven.
        """1.Use GROUP BY subject_id to group records, ensuring no duplicate subject_id.
            \n2.Use MIN(admit_date) to find the earliest admission date.
            \n3.Execute a DELETE FROM command to expunge all patient records related to psychiatric disorders.""",
        # SQL Step Eight.
        """1.Use SELECT *, 'TRUE' AS with_psychosis to add a with_psychosis column with value TRUE (for case group).
            \n2.Use SELECT *, 'FALSE' AS with_psychosis to add a with_psychosis column with value FALSE (for control group).
            \n3.Use UNION ALL to combine hosp + ed.""",
        # SQL Step Nine.
        """1.Use GROUP BY subject_id to group records, ensuring no duplicate subject_id.
            \n2.Use MAX(admit_date) to find the last admission date.
            \n3.Use DELETE FROM to remove records where psychosis is TRUE
            and the earliest admission date equals the last admission date.""",
        # SQL Step Ten.
        """1.Use ischemic_stroke_icd_codes to query temp_five (entire) for records of patients with ischemic stroke.
            \n2.Use GROUP BY subject_id to group records, ensuring no duplicate subject_id.
            \n3.Use MIN(admit_date) to find the earliest admission date.""",
        # SQL Step Eleven.
        """1.Create table temp_eleven by importing data from temp_nine using CREATE TABLE.
            \n2.Use DELETE FROM temp_nine to remove records where the earliest hospital admission date equals
            the ischemic stroke earliest hospital admission date.""",
        # SQL Step Twelve.
        """1.Use the ID column from temp_eleven to fetch records from temp_ten with matching IDs 
            and where the event date (first occurrence) is closest to the earliest admission date.
            \n2.Use LEFT JOIN to retain all records from temp_eleven; if there is no corresponding event_date, display as NULL.""",
        # SQL Step Thirteen.
        "Check if the event_date column is NULL; if not NULL, set the new column E to TRUE, otherwise FALSE.",
        # SQL Step Fourteen.
        """1.For records in temp_thirteen where event_date is NULL, fill in with the patient's death date.
            \n2.Use UPDATE to change the event_date in temp_fourteen from NULL to the patients' death_date.""",
        # SQL Step Fifteen.
        """1.Create table temp_fifteen containing the latest admission year for each subject_id.
            \n2.Use UNION ALL to combine hosp + ed.
            \n3.Use GROUP BY subject_id to group records, ensuring no duplicate subject_id.""",
        # SQL Step Sixteen.
        """1.Create table temp_sixteen to read all records from temp_fourteen.
            \n2.Use UPDATE to set the event_date in temp_sixteen (where it is NULL) to last_year/12/31.""",
        # SQL Step Seventeen.
        """1.Create table temp_seventeen to read all records from temp_sixteen.
            \n2.Use a subquery to calculate gender (male = 1, female = 0) by matching subject_id.
            \n3.Use a subquery to calculate age (based on index_date) by matching subject_id.""",
        # SQL Step Eighteen.
        """1.Create table temp_eighteen to read all records from temp_seventeen.
            \n2.Delete entire rows from the control group (with_psychosis = FALSE) 
            that do not have a matching gender and age in the case group (with_psychosis = TRUE).""",
        # SQL Step Nineteen.
        """1.Create table temp_nineteen to read all records from temp_eighteen.
            \n2.Use ALTER TABLE to modify the structure of the existing temp_nineteen table (add column T).
            \n3.Use UPDATE to update the T column in temp_nineteen.""",
        # SQL Step Twenty.
        """1.Create table temp_twenty to read all records from temp_nineteen.
            \n2.Use DELETE to remove records from temp_twenty where the T column is less than or equal to 0.""",
        # SQL Step Twenty-One.
        """1.Create table temp_twenty_one to store the subject_id, icd_code, and icd_version columns from hosp + ED.
            \n2.Use UNION ALL to vertically merge the hosp and ED tables.""",
        # SQL Step Twenty-Two.
        """1.Create table temp_twenty_two by reading from temp_twenty_one and performing a new SELECT query.
            \n2.Use the CASE statement to determine whether the patient has a specific disease 
            and set the value to TRUE or FALSE based on the result.
            \n3.Use the COALESCE function to replace NULL values in disease counts with 0,
            preventing errors in subsequent calculations.
            \n4.Use the SUM function to add up the values returned by the CASE statement, 
            counting how many times each patient has been diagnosed with a specific disease.
            \n5.Use the CASE WHEN SUM(disease filtering condition) > 0 THEN TRUE ELSE FALSE 
            statement for classification, where a result greater than 0 will return TRUE, otherwise FALSE.
            \n6.Use GROUP BY subject_id to group records.
            \n7.Use LEFT JOIN to merge the records of each patient,
            ensuring that even if there is no matching disease record, the patient’s data is retained.""",
        # SQL Step Twenty-Three.
        """1.Create table temp_twenty_three to store the merged data from temp_twenty and temp_twenty_two.
            \n2.Use LEFT JOIN to merge temp_twenty and temp_twenty_two based on subject_id."""
    ]

    default_sql_queries = [
        "SELECT subject_id, admittime FROM mimiciv_hosp.admissions;",
        "SELECT subject_id, seq_num, icd_code, icd_version FROM mimiciv_hosp.diagnoses_icd;",
        "SELECT subject_id, gender, anchor_age, anchor_year, dod FROM mimiciv_hosp.patients;",
        "SELECT subject_id, seq_num, icd_code, icd_version FROM mimic_ed.diagnosis;",
        "SELECT subject_id, intime FROM mimic_ed.edstays;",
        "SELECT icd_code, icd_version FROM diabetes_icd_codes;",
        "SELECT icd_code, icd_version FROM heart_type_disease_icd_codes;",
        "SELECT icd_code, icd_version FROM hemorrhagic_stroke_icd_codes;",
        "SELECT icd_code, icd_version FROM hyperlipidemia_icd_codes;",
        "SELECT icd_code, icd_version FROM hypertension_icd_codes;",
        "SELECT icd_code, icd_version FROM ischemic_stroke_icd_codes;",
        "SELECT icd_code, icd_version FROM neurological_type_disease_icd_codes;",
        "SELECT icd_code, icd_version FROM psychosis_icd_codes;",
        # SQL Step One
        f"""DROP TABLE IF EXISTS temp_one;
CREATE TEMP TABLE temp_one AS
SELECT subject_id, DATE(admittime) AS admit_date, icd_code, icd_version
FROM mimiciv_hosp.diagnoses_icd
NATURAL JOIN mimiciv_hosp.admissions;
SELECT * FROM temp_one;""",
        # SQL Step Two
        f"""DROP TABLE IF EXISTS temp_two;
CREATE TEMP TABLE temp_two AS
SELECT DISTINCT subject_id
FROM temp_one;
SELECT * FROM temp_two;""",
        # SQL Step Three
        f"""DROP TABLE IF EXISTS temp_three;
CREATE TEMP TABLE temp_three AS
SELECT subject_id, DATE(intime) AS admit_date, icd_code, icd_version
FROM mimic_ed.diagnosis
NATURAL JOIN mimic_ed.edstays;
SELECT * FROM temp_three;""",
        # SQL Step Four
        f"""DROP TABLE IF EXISTS temp_four;
CREATE TABLE temp_four AS
SELECT * FROM temp_three
WHERE subject_id IN (SELECT subject_id FROM temp_two);
SELECT * FROM temp_four;""",
        # SQL Step Five
        f"""DROP TABLE IF EXISTS temp_five;
CREATE TABLE temp_five AS
SELECT * FROM temp_one
UNION ALL
SELECT * FROM temp_four;
SELECT * FROM temp_five;""",
        # SQL Step Six
        f"""DROP TABLE IF EXISTS temp_six;
CREATE TABLE temp_six AS
SELECT subject_id, MIN(admit_date) AS index_date
FROM (
SELECT * FROM temp_five
WHERE (icd_version = 10
AND icd_code IN (SELECT icd_code FROM psychosis_icd_codes WHERE icd_version = 10))
OR (icd_version = 9
AND icd_code IN (SELECT icd_code FROM psychosis_icd_codes WHERE icd_version = 9))
) AS all_diagnoses
GROUP BY subject_id;
SELECT * FROM temp_six;""",
        # SQL Step Seven #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_seven;
CREATE TABLE temp_seven AS
SELECT subject_id, MIN(admit_date) AS index_date
FROM temp_five
GROUP BY subject_id;
DELETE FROM temp_seven
WHERE subject_id IN (SELECT subject_id FROM temp_six);
DELETE FROM temp_seven
WHERE subject_id IN (
SELECT DISTINCT subject_id
FROM temp_five
WHERE (icd_version = 10
AND icd_code IN (
SELECT icd_code
FROM all_psychiatric_disorders_icd_codes
WHERE icd_version = 10))
OR (icd_version = 9
AND icd_code IN (
SELECT icd_code
FROM all_psychiatric_disorders_icd_codes
WHERE icd_version = 9)));
SELECT * FROM temp_seven;""",
        # SQL Step Eight #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_eight;
CREATE TEMP TABLE temp_eight AS
SELECT *, 'TRUE' AS with_psychosis FROM temp_six
UNION ALL
SELECT *, 'FALSE' AS with_psychosis FROM temp_seven;
SELECT * FROM temp_eight;""",
        # SQL Step Nine #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_nine;
CREATE TABLE temp_nine AS
SELECT subject_id, MAX(admit_date) AS last_date
FROM temp_five
GROUP BY subject_id;
DELETE FROM temp_eight
WHERE subject_id IN (
SELECT temp_eight.subject_id
FROM temp_eight
JOIN temp_nine 
ON temp_eight.subject_id = temp_nine.subject_id
WHERE temp_nine.last_date = temp_eight.index_date
AND temp_eight.with_psychosis = 'TRUE');
DROP TABLE IF EXISTS temp_nine;
CREATE TABLE temp_nine AS
SELECT * FROM temp_eight;
SELECT * FROM temp_nine;""",
        # SQL Step Ten
        f"""DROP TABLE IF EXISTS temp_ten;
CREATE TABLE temp_ten AS
SELECT subject_id, MIN(admit_date) AS first_date_ischemic_stroke
FROM (
SELECT * FROM temp_five
WHERE (icd_version = 10
AND icd_code IN (SELECT icd_code FROM ischemic_stroke_icd_codes WHERE icd_version = 10))
OR (icd_version = 9
AND icd_code IN (SELECT icd_code FROM ischemic_stroke_icd_codes WHERE icd_version = 9))
) AS all_diagnoses_first_date_ischemic_stroke
GROUP BY subject_id;
SELECT * FROM temp_ten;""",
        # SQL Step Eleven
        f"""DROP TABLE IF EXISTS temp_eleven;
CREATE TABLE temp_eleven AS
SELECT * FROM temp_nine;
DELETE FROM temp_eleven
WHERE subject_id IN (
SELECT temp_eleven.subject_id
FROM temp_eleven
JOIN temp_ten 
ON temp_eleven.subject_id = temp_ten.subject_id
WHERE temp_ten.first_date_ischemic_stroke = temp_eleven.index_date);
SELECT * FROM temp_eleven;""",
        # SQL Step Twelve
        f"""DROP TABLE IF EXISTS temp_twelve;
CREATE TABLE temp_twelve AS
SELECT 
temp_eleven.subject_id,
temp_eleven.with_psychosis,
temp_eleven.index_date,
IS_after_index_date.first_date_ischemic_stroke AS event_date
FROM temp_eleven
LEFT JOIN (
SELECT 
temp_ten.subject_id, 
temp_ten.first_date_ischemic_stroke
FROM temp_ten
JOIN temp_eleven ON temp_ten.subject_id = temp_eleven.subject_id
WHERE temp_ten.first_date_ischemic_stroke > temp_eleven.index_date
) AS IS_after_index_date
ON temp_eleven.subject_id = IS_after_index_date.subject_id;
SELECT * FROM temp_twelve;""",
        # SQL Step Thirteen
        f"""DROP TABLE IF EXISTS temp_thirteen;
CREATE TABLE temp_thirteen AS
SELECT *, CASE 
WHEN event_date IS NOT NULL THEN 'TRUE' 
ELSE 'FALSE' END AS "E"
FROM temp_twelve;
SELECT * FROM temp_thirteen;""",
        # SQL Step Fourteen
        f"""DROP TABLE IF EXISTS temp_fourteen;
CREATE TABLE temp_fourteen AS
SELECT * FROM temp_thirteen;
UPDATE temp_fourteen
SET event_date = patients_death_date.death_date
FROM (
SELECT 
mimiciv_hosp.patients.subject_id, 
DATE(mimiciv_hosp.patients.dod) AS death_date
FROM mimiciv_hosp.patients 
WHERE mimiciv_hosp.patients.dod IS NOT NULL
) patients_death_date
WHERE temp_fourteen.subject_id = patients_death_date.subject_id
AND temp_fourteen.event_date IS NULL;
SELECT * FROM temp_fourteen;""",
        # SQL Step Fifteen #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_fifteen;
CREATE TABLE temp_fifteen AS
SELECT 
subject_id, 
MAX(admit_year) AS admit_year
FROM (
SELECT subject_id, strftime('%Y', admittime) AS admit_year
FROM mimiciv_hosp.diagnoses_icd NATURAL JOIN mimiciv_hosp.admissions
UNION ALL
SELECT 
subject_id, 
strftime('%Y', intime) AS admit_year
FROM mimic_ed.diagnosis NATURAL JOIN mimic_ed.edstays
) AS all_diagnoses
GROUP BY subject_id;
SELECT * FROM temp_fifteen;""",
        # SQL Step Sixteen #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_sixteen;
CREATE TABLE temp_sixteen AS
SELECT * FROM temp_fourteen;
UPDATE temp_sixteen
SET event_date = (
SELECT date(temp_fifteen.admit_year || '-12-31')
FROM temp_fifteen
WHERE temp_fifteen.subject_id = temp_sixteen.subject_id)
WHERE event_date IS NULL;
SELECT * FROM temp_sixteen;""",
        # SQL Step Seventeen #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_seventeen;
CREATE TABLE temp_seventeen AS
SELECT temp_sixteen.*, (SELECT CASE WHEN patients.gender = 'M' THEN 1 ELSE 0 END
FROM mimiciv_hosp.patients AS patients
WHERE patients.subject_id = temp_sixteen.subject_id) AS gender,
(SELECT (CAST(strftime('%Y', temp_sixteen.index_date) AS INTEGER) - (patients.anchor_year - patients.anchor_age))
FROM mimiciv_hosp.patients AS patients
WHERE patients.subject_id = temp_sixteen.subject_id) AS age
FROM temp_sixteen;
SELECT subject_id, gender, event_date, index_date, with_psychosis, "E", age
FROM temp_seventeen;""",
        # SQL Step Eighteen
        f"""DROP TABLE IF EXISTS temp_eighteen;
CREATE TABLE temp_eighteen AS
SELECT * FROM temp_seventeen;
DELETE FROM temp_eighteen
WHERE with_psychosis = FALSE
AND NOT EXISTS (
SELECT 1
FROM temp_eighteen temp_eighteen_case
WHERE with_psychosis = TRUE
AND temp_eighteen.gender = temp_eighteen_case.gender
AND temp_eighteen.age = temp_eighteen_case.age);
SELECT subject_id, gender, event_date, index_date, with_psychosis, "E", age 
FROM temp_eighteen;""",
        # SQL Step Nineteen - Update T column (if calculation fails, keep NA)
        f"""DROP TABLE IF EXISTS temp_nineteen;
CREATE TABLE temp_nineteen AS
SELECT * FROM temp_eighteen;
ALTER TABLE temp_nineteen
ADD COLUMN "T" INTEGER;
UPDATE temp_nineteen
SET "T" = (event_date - index_date);
SELECT subject_id, gender, event_date, index_date, with_psychosis, "E", age, "T"
FROM temp_nineteen;""",
        # SQL Step Twenty - Delete rows with T column value <= 0 #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_twenty;
CREATE TABLE temp_twenty AS
SELECT subject_id, gender, event_date, index_date, with_psychosis, "E", age, "T"
FROM temp_nineteen;
DELETE FROM temp_twenty
WHERE T <= 0;
SELECT * FROM temp_twenty;""",
        # SQL Step Twenty-One
        f"""DROP TABLE IF EXISTS temp_twenty_one;
CREATE TABLE temp_twenty_one AS
SELECT subject_id, icd_code, icd_version
FROM mimiciv_hosp.diagnoses_icd
UNION ALL
SELECT subject_id, icd_code, icd_version
FROM mimic_ed.diagnosis;
SELECT * FROM temp_twenty_one;""",
        # SQL Step Twenty-Two - Use WITH and LEFT JOIN to consolidate disease counts #pgadmin 4 SQL differences
        f"""DROP TABLE IF EXISTS temp_twenty_two;
CREATE TABLE temp_twenty_two AS
WITH t AS (
SELECT subject_id, icd_code, icd_version 
FROM temp_twenty_one),
h AS (
SELECT subject_id, CAST(COUNT(*) AS INTEGER) AS hypertension_times
FROM t
WHERE icd_code IN (SELECT icd_code FROM hypertension_icd_codes)
AND icd_version IN (9,10)
GROUP BY subject_id),
htd AS (
SELECT subject_id, CAST(COUNT(*) AS INTEGER) AS heart_type_disease_times
FROM t
WHERE icd_code IN (SELECT icd_code FROM heart_type_disease_icd_codes)
AND icd_version IN (9,10)
GROUP BY subject_id),
n AS (
SELECT subject_id, CAST(COUNT(*) AS INTEGER) AS neurological_type_disease_times
FROM t
WHERE icd_code IN (SELECT icd_code FROM neurological_type_disease_icd_codes)
AND icd_version IN (9,10)
GROUP BY subject_id),
d AS (
SELECT subject_id, CAST(COUNT(*) AS INTEGER) AS diabetes_times
FROM t
WHERE icd_code IN (SELECT icd_code FROM diabetes_icd_codes)
AND icd_version IN (9,10)
GROUP BY subject_id
),
l AS (
SELECT subject_id, CAST(COUNT(*) AS INTEGER) AS hyperlipidemia_times
FROM t
WHERE icd_code IN (SELECT icd_code FROM hyperlipidemia_icd_codes)
AND icd_version IN (9,10)
GROUP BY subject_id)
SELECT 
tt.subject_id,
COALESCE(h.hypertension_times,0) AS hypertension_times,
CASE WHEN COALESCE(h.hypertension_times,0) > 0 THEN 'TRUE' ELSE 'FALSE' END AS with_hypertension,
COALESCE(htd.heart_type_disease_times,0) AS heart_type_disease_times,
CASE WHEN COALESCE(htd.heart_type_disease_times,0) > 0 THEN 'TRUE' ELSE 'FALSE' END AS with_heart_type_disease,
COALESCE(n.neurological_type_disease_times,0) AS neurological_type_disease_times,
CASE WHEN COALESCE(n.neurological_type_disease_times,0) > 0 THEN 'TRUE' ELSE 'FALSE' END AS with_neurological_type_disease,
COALESCE(d.diabetes_times,0) AS diabetes_times,
CASE WHEN COALESCE(d.diabetes_times,0) > 0 THEN 'TRUE' ELSE 'FALSE' END AS with_diabetes,
COALESCE(l.hyperlipidemia_times,0) AS hyperlipidemia_times,
CASE WHEN COALESCE(l.hyperlipidemia_times,0) > 0 THEN 'TRUE' ELSE 'FALSE' END AS with_hyperlipidemia
FROM (SELECT subject_id FROM temp_twenty_one GROUP BY subject_id) tt
LEFT JOIN h  ON tt.subject_id = h.subject_id
LEFT JOIN htd ON tt.subject_id = htd.subject_id
LEFT JOIN n  ON tt.subject_id = n.subject_id
LEFT JOIN d  ON tt.subject_id = d.subject_id
LEFT JOIN l  ON tt.subject_id = l.subject_id;
SELECT * FROM temp_twenty_two;""",
        # SQL Step Twenty-Three
        f"""DROP TABLE IF EXISTS temp_twenty_three;
CREATE TABLE temp_twenty_three AS
SELECT
temp_twenty.subject_id,
temp_twenty.gender,
temp_twenty.age,
temp_twenty.with_psychosis,
temp_twenty.index_date,
temp_twenty.event_date,
temp_twenty."T",
temp_twenty."E",
temp_twenty_two.with_hypertension,
temp_twenty_two.with_heart_type_disease,
temp_twenty_two.with_neurological_type_disease,
temp_twenty_two.with_diabetes,
temp_twenty_two.with_hyperlipidemia,
temp_twenty_two.hypertension_times,
temp_twenty_two.heart_type_disease_times,
temp_twenty_two.neurological_type_disease_times,
temp_twenty_two.diabetes_times,
temp_twenty_two.hyperlipidemia_times
FROM temp_twenty
LEFT JOIN temp_twenty_two
ON temp_twenty.subject_id = temp_twenty_two.subject_id;
SELECT * FROM temp_twenty_three;"""
    ]

    # Initialize session state keys for each query if not already set
    for i in range(36):
        if f"query_result_{i}" not in st.session_state:
            st.session_state[f"query_result_{i}"] = None
        if f"last_query_{i}" not in st.session_state:
            st.session_state[f"last_query_{i}"] = default_sql_queries[i]

    # Build alias mapping for PKL files (replace '.' with '_' in table names)
    alias_map = {}
    table_info = []
    for pkl_file in os.listdir(PKL_DIR):
        if pkl_file.endswith(".pkl"):
            original_table_name = os.path.splitext(pkl_file)[0]
            alias_table_name = original_table_name.replace(".", "_")
            if alias_table_name not in data_dict:
                try:
                    df = pd.read_pickle(os.path.join(PKL_DIR, pkl_file))
                    if isinstance(df, pd.DataFrame):
                        df = clean_df(df)
                        for col in df.columns:
                            if any(keyword in col.lower() for keyword in ['time', 'date', 'datetime']):
                                try:
                                    df[col] = pd.to_datetime(df[col], errors='coerce').astype("datetime64[ns]")
                                except Exception as e:
                                    st.write(f"Warning: Failed to convert {col}: {e}")
                        data_dict[alias_table_name] = df
                except Exception as e:
                    st.error(f"❌ Unable to read {pkl_file}: {e}")
            alias_map[original_table_name] = alias_table_name
            table_info.append({"Table Name": original_table_name, "Record Count": len(data_dict[alias_table_name])})

    def sort_tables(table):
        name = table["Table Name"]
        if "mimiciv_hosp" in name:
            return (0, name)
        elif "mimic_ed" in name:
            return (1, name)
        else:
            return (2, name)
    table_info_sorted = sorted(table_info, key=sort_tables)
    table_info_df = pd.DataFrame(table_info_sorted)
    table_info_df.index = range(1, len(table_info_df) + 1)

    st.title("📊 Disease-Specific SQL Queries")
    if data_dict:
        st.success(f"✅ Successfully loaded {len(table_info)} tables!")
        st.subheader("📋 Queryable Tables")
        st.dataframe(table_info_df, use_container_width=True)
        # Create an HTML anchor for "Back to Top"
        st.markdown("<a name='top'></a>", unsafe_allow_html=True)
        st.markdown("")
        st.markdown("")
        # Two main tabs: one for Table Test and one for Specific Diseases Query Steps
        tab1, tab2 = st.tabs(["Specific Diseases Query Steps", "Table Test"])
        with tab2:
            if st.button("▶️ Execute All SQL Sequentially 📚"):
                execute_all_all(range(0, 13), alias_map, data_dict)
            for i in range(13):
                st.subheader(f"🔎 {query_names[i]}")
                num_lines = st.session_state[f"last_query_{i}"].count("\n") + 1
                input_height = max(100, num_lines * 25)
                sql_query = st.text_area(
                    f"Please enter {query_names[i]}",
                    st.session_state[f"last_query_{i}"],
                    height=input_height
                )
                if sql_query != st.session_state[f"last_query_{i}"]:
                    st.session_state[f"last_query_{i}"] = sql_query
                if st.button(f"👉 Execute SQL", key=f"btn_{i}"):
                    execute_all_all([i], alias_map, data_dict)
                # Display two sub-tabs: Data output and Messages
                sub_tabs = st.tabs(["Data output", "Messages"])
                with sub_tabs[0]:
                    if isinstance(st.session_state[f"query_result_{i}"], pd.DataFrame):
                        st.dataframe(st.session_state[f"query_result_{i}"], use_container_width=True)
                    # else:
                    #     st.write("No data output available.")
                with sub_tabs[1]:
                    for msg in st.session_state.get(f"query_message_{i}", []):
                        st.write(msg)
        
        with tab1:
            if st.button("▶️ Execute All SQL Sequentially 📗"):
                execute_all_all(range(13, 36), alias_map, data_dict)
            for i in range(13, 36):
                st.subheader(f"🔎 {query_names[i]}")
                num_lines = st.session_state[f"last_query_{i}"].count("\n") + 1
                input_height = max(100, num_lines * 25)
                sql_query = st.text_area(
                    f"{query_names_subtitle[i-13]}",
                    st.session_state[f"last_query_{i}"],
                    height=input_height
                )
                if sql_query != st.session_state[f"last_query_{i}"]:
                    st.session_state[f"last_query_{i}"] = sql_query
                if st.button(f"👉 Execute SQL", key=f"btn_{i}"):
                    execute_all_all([i], alias_map, data_dict)
                sub_tabs = st.tabs(["Data output", "Messages"])
                with sub_tabs[0]:
                    if isinstance(st.session_state[f"query_result_{i}"], pd.DataFrame):
                        st.dataframe(st.session_state[f"query_result_{i}"], use_container_width=True)
                    # else:
                    #     st.write("No data output available.")
                with sub_tabs[1]:
                    for msg in st.session_state.get(f"query_message_{i}", []):
                        st.write(msg)

    st.markdown("""
    <style>
    .back-to-top {
        position: fixed;
        bottom: 60px;
        right: 20px;
        background-color: #007bff;
        color: white !important;
        width: 40px;
        height: 40px;
        border-radius: 5px;
        text-decoration: none;
        font-size: 24px;
        z-index: 100;
        display: flex;
        justify-content: center;
        align-items: center;
    }
    .back-to-top:hover {
        background-color: #0056b3;
    }
    </style>
    <a class="back-to-top" href="#top">&#8593;</a>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    show()
