# canvassing_app.py
import streamlit as st
import pandas as pd
import pandas_gbq
import pydeck as pdk
from gsheetsdb import connect

from google.oauth2 import service_account
from geopy.geocoders import Nominatim

#### INITIALIZE ####
credentials_bq = service_account.Credentials.from_service_account_info(
    st.secrets["tdp_service_account_bq"]
)
credentials_gs = service_account.Credentials.from_service_account_info(
    st.secrets["tdp_service_account_gs"],
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
    ],
)

conn = connect(credentials=credentials_gs)

#### APP AND DISPLAY SETTINGS ####
@st.cache(ttl=600)
def run_query(query):
    rows = conn.execute(query, headers=1)
    rows = rows.fetchall()
    return rows

sheet_url = st.secrets["private_gsheets_url"]
rows = run_query(f'SELECT * FROM "{sheet_url}"')

ls = []
for row in rows:
    ls.append(row[0])

def check_email():
    """Returns `True` if the user is with an email we are aware of."""

    def email_entered():
        """Checks whether a email entered by the user is correct."""
        if st.session_state["email"] in ls:
            st.session_state["email_correct"] = True
            del st.session_state["email"]  # don't store password
        else:
            st.session_state["email_correct"] = False

    if "email_correct" not in st.session_state:
        st.text_input(
            "Email", on_change=email_entered, key='email'
        )
        return False
    elif not st.session_state["email_correct"]:
        st.text_input(
            "Email", on_change=email_entered, key='email'
        )
        st.error("Not authorized to enter. Please contact organizer.")
        return False
    else:
        return True

if check_email():
    app_title = st.title('Which address will you start canvassing from?')
    data_load_state = st.text("Please enter an address in the sidebar.\nClick the arrow in the top left, if necessary, to show the sidebar.")

    def load_data(lon, lat):
        unreg_query = f"""
        WITH addresses AS (SELECT
          p.unit_acct_id,
          l.latitude as lat,
          l.longitude as lon,
          appraisal_addr_parcel as address,
          unit_type,
          unit,
          city,
          zip,
          ST_DISTANCE(ST_GEOGPOINT({lon}, {lat}), ST_GEOGPOINT(CAST(l.longitude AS NUMERIC), CAST(l.latitude AS NUMERIC))) AS distance
        FROM `demstxsp.vr_data.harris_parcel_partisanship_predictions` p
        JOIN `demstxsp.vr_data.harris_parcel_lat_lng` l
          USING (unit_acct_id)
        JOIN `demstxsp.vr_data.harris_address_parcel` a
          USING (unit_acct_id)
        WHERE
            p.predicted_tdp_partisanship_range = "70-100"
            AND l.latitude IS NOT NULL
            AND ST_DWITHIN(ST_GEOGPOINT({lon}, {lat}), ST_GEOGPOINT(CAST(l.longitude AS NUMERIC), CAST(l.latitude AS NUMERIC)), 10000)
        )
        SELECT *
        FROM addresses
        ORDER BY distance
        LIMIT 50
        """

        data = (
            pandas_gbq
            .read_gbq(unreg_query, credentials=credentials_bq)
            .assign(
                lat=lambda df: df['lat'].astype(float),
                lon=lambda df: df['lon'].astype(float)
            )
        )

        return data

    def geocode_add(address,city,zip):
        '''This function takes an address in street number + street format, a city,
        and a zip, and returns the geocoded latitude and longitude.
        '''
        addr = f'{address + " " + city + ", TX " + zip}'
        locator = Nominatim(user_agent="address_nearby")
        location = locator.geocode(f"{addr}")

        return location.latitude, location.longitude, addr

    def check_zip(zip):
        try:
            int(zip)
            result = True
        except ValueError:
            st.text('Please input only numeric values for zip code.')
            result = False

        return result

    with st.sidebar.form("start_address"):
        address = st.text_input(label='Street Address, excluding unit/apartment', placeholder='ex: 927 Dart St')
        city = st.text_input(label='City', placeholder='ex: Houston')
        zip = st.text_input(label='Zip5 Code', placeholder='ex: 77001', max_chars=5, help='5 digit zip code, must be numeric')

        # Every form must have a submit button.
        submitted = st.form_submit_button("Submit")
        if submitted:
            if check_zip(zip):
                try:
                    lat, lon, addr = geocode_add(address=address, city=city, zip=zip)
                except AttributeError:
                    st.text("Address not found. Try again.")
                    submitted = False
                    st.write(submitted)

    if submitted:
        app_title.title('Addresses and Locations to canvass')
        data_load_state.text('Loading data...')
        df_canvass = load_data(lon=lon, lat=lat)
        data_load_state.text("")

        map_title = st.subheader(f'50 closest addresses to {addr}')
        map_text = st.text('Addresses are indicated by purple dots on the map.\nYou may need to zoom in/out to get a better view.')

        if not df_canvass.empty:
            map = st.pydeck_chart(pdk.Deck(
                 map_style='mapbox://styles/mapbox/streets-v11',
                 initial_view_state=pdk.ViewState(
                     latitude=df_canvass['lat'].mean(),
                     longitude=df_canvass['lon'].mean(),
                     zoom=14,
                     pitch=0,
                 ),
                 layers=[
                     pdk.Layer(
                         'ScatterplotLayer',
                         data=df_canvass,
                         get_position='[lon, lat]',
                         get_color='[69, 47, 110]',
                         get_radius=20,
                         opacity=0.75,
                     ),
                 ],
            ))
            raw_title = st.subheader('Raw data, closest to furthest')
            df_canvass_sort = df_canvass.sort_values(by=['distance']).drop(columns=['unit_acct_id','lat','lon','distance']).reset_index(drop=True)
            df = st.dataframe(df_canvass_sort)

        else:
            map_title.subheader('No addresses found nearby. Please re-submit with a new address.')
            map_text.text('')
