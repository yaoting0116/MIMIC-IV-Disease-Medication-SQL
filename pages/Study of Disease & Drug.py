import streamlit as st

# Specify the URL you want to redirect to
redirect_url = "https://mimic-iv-drug-data-analysis-0--introduction-uwu-ting.streamlit.app/"

# Use an HTML meta tag to perform the redirect
st.markdown(
    f"""
    <meta http-equiv="refresh" content="0; URL={redirect_url}" />
    """,
    unsafe_allow_html=True
)

st.write("Redirecting to the new website...")
