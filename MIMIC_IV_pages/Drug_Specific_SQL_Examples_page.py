import streamlit as st
import os
import re
import pandas as pd
from pandasql import sqldf

# --- Logging Function ---
def log_message(query_index, message):
    key = f"drug_message_{query_index}"
    if key not in st.session_state:
        st.session_state[key] = []
    st.session_state[key].append(message)

# --- Helper Functions (Common) ---

# Convert only non-numeric columns to datetime
def convert_time_columns(df):
    time_keywords = ['time', 'date', 'datetime']
    for col in df.columns:
        if any(keyword in col.lower() for keyword in time_keywords) and not pd.api.types.is_numeric_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                log_message(-1, f"Warning: Failed to convert column '{col}': {e}")
    return df

# Simulate DELETE statements
# 目前僅支援 "DELETE FROM <table> WHERE <condition>" 格式
def simulate_delete(q, data_dict, query_index):
    pattern_simple = r"DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+)"
    match_simple = re.match(pattern_simple, q, re.IGNORECASE)
    if match_simple:
        table_name = match_simple.group(1)
        condition = match_simple.group(2)
        # 將 SQL 的 "=" 轉換成 Pandas 的 "==" 語法
        condition = condition.replace(" = ", " == ")
        if table_name in data_dict:
            df_table = data_dict[table_name]
            try:
                mask = df_table.eval(condition)
                filtered_df = df_table[~mask]
                data_dict[table_name] = filtered_df
                log_message(query_index, f"✅ DELETE complete: Deleted rows from {table_name} meeting condition '{condition}'")
            except Exception as e:
                log_message(query_index, f"Warning: Delete operation failed, condition parsing error: {e}")
        else:
            log_message(query_index, f"Warning: Cannot find {table_name} in data_dict")
    else:
        log_message(query_index, "Warning: DELETE statement format not supported, currently only supports DELETE FROM <table> WHERE <condition> format.")

# --- Drug-Specific SQL Execution Function ---
def drug_execute_all_all(indices, alias_map, data_dict):
    for i in indices:
        # 初始化此查詢的訊息記錄
        st.session_state[f"drug_message_{i}"] = []
        sql_query = st.session_state[f"drug_last_query_{i}"]
        for original, alias in alias_map.items():
            sql_query = sql_query.replace(original, alias)
        queries = [q.strip() for q in sql_query.split(";") if q.strip()]
        for q in queries:
            if q.upper().startswith("DROP TABLE IF EXISTS"):
                drop_table_name = q.split()[-1]
                if drop_table_name in data_dict:
                    del data_dict[drop_table_name]
                    log_message(i, f"🗑️ `{drop_table_name}` DROP.")
            elif q.upper().startswith("CREATE TEMP TABLE") or q.upper().startswith("CREATE TABLE"):
                temp_table_name = q.split("AS")[0].split()[-1]
                temp_query = q.split("AS", 1)[1].strip()
                if temp_table_name in data_dict:
                    del data_dict[temp_table_name]
                data_dict[temp_table_name] = sqldf(temp_query, locals() | data_dict)
                log_message(i, f"✅ CREATE complete: Table {temp_table_name} created.")
            elif q.upper().startswith("DELETE"):
                simulate_delete(q, data_dict, i)
            elif q.upper().startswith("SELECT"):
                result_df = sqldf(q, locals() | data_dict)
                if i == 0:
                    if "starttime" in result_df.columns:
                        result_df["starttime"] = pd.to_datetime(result_df["starttime"], errors="coerce")
                    if "stoptime" in result_df.columns:
                        result_df["stoptime"] = pd.to_datetime(result_df["stoptime"], errors="coerce")
                result_df = convert_time_columns(result_df)
                result_df.index = range(1, len(result_df) + 1)
                st.session_state[f"drug_query_result_{i}"] = result_df
                log_message(i, "✅ SELECT complete: Query executed successfully.")
    st.rerun()

# --- Drug-Specific Web Display Function ---
def show():
    pd.set_option('future.no_silent_downcasting', True)
    st.markdown("<a name='top'></a>", unsafe_allow_html=True)
    st.markdown("")
    st.markdown("")
    st.markdown("""
    <style>
    div.stButton > button {
        width: auto !important;
        display: inline-block;
    }
    </style>
    """, unsafe_allow_html=True)

    PKL_DIR = "MIMIC_IV_data/drugs_data"

    if "drug_data_dict" not in st.session_state:
        st.session_state["drug_data_dict"] = {}
    data_dict = st.session_state["drug_data_dict"]

    pkl_files = [f for f in os.listdir(PKL_DIR) if f.endswith(".pkl")]
    if not pkl_files:
        st.error(f"❌ No PKL files were found in `{PKL_DIR}`.")
        return

    query_names = [
        "SQL Statement for the prescriptions table",  
        "Step 1",
        "Step 2",
        "Step 3",
        "Step 4",
        "Step 5",
        "Step 6",
        "Step 7"
    ]

    query_names_subtitle = [
        "Uniform casing for drug names and extended search for identical names (Using the mimic_hosp.prescriptions table for querying).",
        "The drug unit is MG (using the table from Step 1 for querying).",
        "The drug dose is not NULL (using the table from Step 2 for querying).",
        "The start and end times of drug usage are not NULL (using the table from Step 3 for querying).",
        """1.Add the hours_diff column to store the duration from the start to the end of drug usage (in hours).
            \n2.Use ABS to ensure that the values in the hours_diff column are absolute, preventing negative values (Using the table from Step 4 for querying).""",
        "Change all 0 values in the hours_diff column to 1 (Using the table from Step 5 for querying).",
        "Delete entire rows where the dose_val_rx column contains a value of 0 (Using the table from Step 6 for querying)."
    ]

    default_sql_queries = [
        "SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime FROM mimiciv_hosp.prescriptions;",
        """DROP TABLE IF EXISTS temp_one;
CREATE TEMP TABLE temp_one AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM mimiciv_hosp.prescriptions
WHERE LOWER(drug) LIKE LOWER('aspirin%')
OR LOWER(drug) LIKE LOWER('warfarin%')
OR LOWER(drug) LIKE LOWER('clopidogrel%')
OR LOWER(drug) LIKE LOWER('apixaban%')
OR LOWER(drug) LIKE LOWER('rivaroxaban%')
OR LOWER(drug) LIKE LOWER('dabigatran etexilate%')
OR LOWER(drug) LIKE LOWER('cilostazol%')
OR LOWER(drug) LIKE LOWER('enoxaparin%');
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_one;""",
        """DROP TABLE IF EXISTS temp_two;
CREATE TEMP TABLE temp_two AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_one
WHERE dose_unit_rx = 'mg';
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_one
WHERE dose_unit_rx = 'mg';""",
        """DROP TABLE IF EXISTS temp_three;
CREATE TEMP TABLE temp_three AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_two
WHERE dose_val_rx IS NOT NULL;
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_two
WHERE dose_val_rx IS NOT NULL;""",
        """DROP TABLE IF EXISTS temp_four;
CREATE TEMP TABLE temp_four AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_three
WHERE (starttime IS NOT NULL AND stoptime IS NOT NULL);
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime
FROM temp_three
WHERE (starttime IS NOT NULL AND stoptime IS NOT NULL);""",
        """DROP TABLE IF EXISTS temp_five;
CREATE TEMP TABLE temp_five AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime,
ABS((julianday(stoptime) - julianday(starttime)) * 24) AS hours_diff
FROM temp_four;
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime,
ABS((julianday(stoptime) - julianday(starttime)) * 24) AS hours_diff
FROM temp_four;""",
        """DROP TABLE IF EXISTS temp_six;
CREATE TEMP TABLE temp_six AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime,
CASE 
WHEN ABS((julianday(stoptime) - julianday(starttime)) * 24) = 0 
THEN 1 
ELSE ABS((julianday(stoptime) - julianday(starttime)) * 24)
END AS hours_diff
FROM temp_five;
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime,
CASE 
WHEN ABS((julianday(stoptime) - julianday(starttime)) * 24) = 0 
THEN 1 
ELSE ABS((julianday(stoptime) - julianday(starttime)) * 24)
END AS hours_diff
FROM temp_five;""",
        """DROP TABLE IF EXISTS temp_seven;
CREATE TEMP TABLE temp_seven AS
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime, hours_diff
FROM temp_six;
DELETE FROM temp_seven
WHERE dose_val_rx = '0';
SELECT subject_id, drug, dose_val_rx, dose_unit_rx, starttime, stoptime, hours_diff
FROM temp_seven;"""
    ]

    # Initialize drug-specific session state keys
    for i in range(8):
        if f"drug_query_result_{i}" not in st.session_state:
            st.session_state[f"drug_query_result_{i}"] = None
        if f"drug_last_query_{i}" not in st.session_state:
            st.session_state[f"drug_last_query_{i}"] = default_sql_queries[i]

    alias_map = {}
    table_info = []
    for pkl_file in os.listdir(PKL_DIR):
        if pkl_file.endswith(".pkl"):
            original_table_name = os.path.splitext(pkl_file)[0]
            alias_table_name = original_table_name.replace(".", "_dot_") if "." in original_table_name else original_table_name
            if alias_table_name not in data_dict:
                try:
                    df = pd.read_pickle(os.path.join(PKL_DIR, pkl_file))
                    if isinstance(df, pd.DataFrame):
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

    st.title("📊 Drug-Specific SQL Test Queries")
    if data_dict:
        st.subheader("📋 Queryable Tables")
        st.dataframe(table_info_df, use_container_width=True)

        main_tab1, main_tab2 = st.tabs(["Table Test", "Drug-Specific Query Steps"])
        
        with main_tab1:
            for i in range(0, 1):
                st.subheader(f"🔎 {query_names[i]}")
                num_lines = st.session_state[f"drug_last_query_{i}"].count("\n") + 1
                input_height = max(100, num_lines * 25)
                sql_query = st.text_area(
                    f"Please enter {query_names[i]}",
                    st.session_state[f"drug_last_query_{i}"],
                    height=input_height
                )
                if sql_query != st.session_state[f"drug_last_query_{i}"]:
                    st.session_state[f"drug_last_query_{i}"] = sql_query
                if st.button(f"👉 Execute SQL", key=f"drug_btn_{i}"):
                    drug_execute_all_all([i], alias_map, data_dict)
                # 顯示兩個子標籤頁：Data output 與 Messages
                tabs = st.tabs(["Data output", "Messages"])
                with tabs[0]:
                    if isinstance(st.session_state[f"drug_query_result_{i}"], pd.DataFrame):
                        st.dataframe(st.session_state[f"drug_query_result_{i}"], use_container_width=True)
                with tabs[1]:
                    for msg in st.session_state.get(f"drug_message_{i}", []):
                        st.write(msg)
        
        with main_tab2:
            if st.button("▶️ Execute All SQL Sequentially 📙"):
                drug_execute_all_all(range(1, 8), alias_map, data_dict)
            for i in range(1, 8):
                st.subheader(f"🔎 {query_names[i]}")
                num_lines = st.session_state[f"drug_last_query_{i}"].count("\n") + 1
                input_height = max(100, num_lines * 25)
                sql_query = st.text_area(
                    f"{query_names_subtitle[i-1]}",
                    st.session_state[f"drug_last_query_{i}"],
                    height=input_height
                )
                if sql_query != st.session_state[f"drug_last_query_{i}"]:
                    st.session_state[f"drug_last_query_{i}"] = sql_query
                if st.button(f"👉 Execute SQL", key=f"drug_btn_{i}"):
                    drug_execute_all_all([i], alias_map, data_dict)
                tabs = st.tabs(["Data output", "Messages"])
                with tabs[0]:
                    if isinstance(st.session_state[f"drug_query_result_{i}"], pd.DataFrame):
                        st.dataframe(st.session_state[f"drug_query_result_{i}"], use_container_width=True)
                with tabs[1]:
                    for msg in st.session_state.get(f"drug_message_{i}", []):
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
