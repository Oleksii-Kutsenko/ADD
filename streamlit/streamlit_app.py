import pandas as pd
import requests

import streamlit as st

API = "http://presenter:8000"

st.set_page_config(page_title="NYC Taxi Dashboard", layout="centered")
st.title("🚕  NYC Taxi Explorer")

dt_from = st.date_input("From", value=pd.Timestamp("2017-12-01"))
dt_to = st.date_input("To", value=pd.Timestamp("2020-01-31"))
borough = st.selectbox(
    "Borough", ["", "Manhattan", "Brooklyn", "Queens", "Bronx", "EWR"]
)

params = {"from": dt_from, "to": dt_to}
if borough:
    params["borough"] = borough

data = requests.get(f"{API}/stats/daily_fare", params=params).json()
df = pd.DataFrame(data)
if df.empty:
    st.info("No data for chosen filters.")
    st.stop()

st.subheader("Trips per day")
st.bar_chart(df.set_index("pickup_date")["trip_count"])

st.subheader("Average fare per day")
st.line_chart(df.set_index("pickup_date")["avg_fare"])
