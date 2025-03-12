import streamlit as st

redirect_url = "https://mimic-iv-drug-data-analysis-0--introduction-uwu-ting.streamlit.app/?embedded=true"

st.markdown(
    f"""
    <meta http-equiv="refresh" content="0; URL={redirect_url}" />
    """,
    unsafe_allow_html=True
)

st.write("Redirecting to the new website...")
