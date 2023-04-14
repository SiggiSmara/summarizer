import streamlit as st
from summarize_csv import pd_read_csv_file

st.title("Siggi and Lisa's finances")

df = pd_read_csv_file("20230414-101179311-umsatz.CSV")
st.write(df)