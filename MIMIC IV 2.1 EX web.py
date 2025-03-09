import streamlit as st
import importlib

def main():
    st.set_page_config(page_title="MIMIC-IV 2.1 Disease & Medication Data Description", layout="wide")
    
    Introduction = importlib.import_module("MIMIC_IV_pages.Introduction")
    Disease_Specific_SQL_Examples_page = importlib.import_module("MIMIC_IV_pages.Disease_Specific_SQL_Examples_page")
    Drug_Specific_SQL_Examples_page = importlib.import_module("MIMIC_IV_pages.Drug_Specific_SQL_Examples_page")

    # Initialize the 'page' parameter in session state
    if "page" not in st.session_state:
        st.session_state.page = "Introduction"
    
    # Custom CSS: adjust button style to look like text links,
    # and increase the clickable area of the buttons as well as add a hover shadow effect
    st.markdown(
        """
        <style>
        /* Hide default button style and increase clickable area */
        div.stButton > button {
            background-color: transparent;
            border: none;
            padding: 10px 20px; /* Increase clickable area for top/bottom and left/right */
            margin: 5px 0;
            font-size: 18px;
            color: #333;
            text-align: left;
            cursor: pointer;
            transition: box-shadow 0.3s, color 0.3s;
            display: block;
            width: 100%;
        }
        /* When hovering over the button, change text color and add a rectangular shadow centered on the text */
        div.stButton > button:hover {
            color: #4CAF50;
            box-shadow: 0 0 8px rgba(0, 0, 0, 0.2);
        }
        div.stButton > button:focus {
            outline: none;
        }
        </style>
        """, unsafe_allow_html=True
    )
    
    with st.sidebar:
        st.markdown("###  MIMIC IV 2.1 SQL MENU")
        pages = ["Introduction", "Disease-Specific SQL Examples", "Drug-Specific SQL Examples"]
        for p in pages:
            if st.button(f"{p}", key=p):
                st.session_state.page = p

    # Display page content based on the session state's 'page' parameter
    if st.session_state.page == "Introduction":
        Introduction.show()
    elif st.session_state.page == "Disease-Specific SQL Examples":
        Disease_Specific_SQL_Examples_page.show()
    elif st.session_state.page == "Drug-Specific SQL Examples":
        Drug_Specific_SQL_Examples_page.show()

if __name__ == "__main__":
    main()
