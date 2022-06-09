# canvassing_app.py
import streamlit as st
import pandas as pd
import pandas_gbq

from google.oauth2 import service_account
from geopy.geocoders import Nominatim


#### INITIALIZE ####
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["tdp_service_account"]
)

#### APP AND DISPLAY SETTINGS ####
st.title('Addresses to canvass')
st.header('Which address will you start canvassing from?')
address = st.text_input(label='Street Address, excluding unit/apartment', placeholder='ex: 927 Dart St')
city = st.text_input(label='City', placeholder='ex: Houston')
zip = st.text_input(label='Zip5 Code', placeholder='ex: 77001', max_chars=5, help='5 digit zip code, must be numeric')

@st.experimental_memo()
# Rerun only if query changes
def load_data(lon, lat):
    unreg_query = f"""
    SELECT
      p.unit_acct_id,
      l.latitude as lat,
      l.longitude as lon,
      appraisal_addr_parcel as address,
      unit_type,
      unit,
      city,
      zip
    FROM `demstxsp.vr_data.harris_parcel_partisanship_predictions` p
    JOIN `demstxsp.vr_data.harris_parcel_lat_lng` l
      USING (unit_acct_id)
    JOIN `demstxsp.vr_data.harris_address_parcel` a
      USING (unit_acct_id)
    WHERE
        p.predicted_tdp_partisanship_range = "70-100"
        AND l.latitude IS NOT NULL
        AND ST_DWITHIN(ST_GEOGPOINT({lon}, {lat}), ST_GEOGPOINT(CAST(l.longitude AS NUMERIC), CAST(l.latitude AS NUMERIC)), 802)
        LIMIT 50
    """

    data = pandas_gbq.read_gbq(unreg_query, credentials=credentials)
    data = data.assign(
        lat=lambda df: df['lat'].astype(float),
        lon=lambda df: df['lon'].astype(float)
    )

    return data

def geocode_add(address,city,zip):
    addr = f'{address + " " + city + ", TX " + zip}'
    locator = Nominatim(user_agent="address_nearby")
    location = locator.geocode(f"{addr}")

    return location.latitude, location.longitude, addr

def check_zip(zip):
    try:
        int(zip)
    except ValueError:
        st.text('Please input only numeric values for zip code.')

if address:
    if city:
        if zip:
            check_zip(zip)
            try:
                lat, lon, addr = geocode_add(address=address, city=city, zip=zip)

                st.header('Addresses and Locations to canvass')
                data_load_state = st.text('Loading data...')
                df_canvass = load_data(lon=lon, lat=lat)
                data_load_state.text("")

                st.subheader(f'Addresses within 0.5 miles of {addr}')
                # st.map(df_canvass)

                st.pydeck_chart(pdk.Deck(
                     map_style='mapbox://styles/mapbox/light-v9',
                     # initial_view_state=pdk.ViewState(
                     #     latitude=37.76,
                     #     longitude=-122.4,
                     #     zoom=11,
                     #     pitch=50,
                     # ),
                     layers=[
                         pdk.Layer(
                            'HexagonLayer',
                            data=df,
                            get_position='[lon, lat]',
                            radius=200,
                            elevation_scale=4,
                            elevation_range=[0, 1000],
                            pickable=True,
                            extruded=True,
                         ),
                         pdk.Layer(
                             'ScatterplotLayer',
                             data=df,
                             get_position='[lon, lat]',
                             get_color='[200, 30, 0, 160]',
                             get_radius=200,
                         ),
                     ],
                 ))



                st.subheader('Raw data')
                st.write(df_canvass.sort_values(by=['lat','address','unit']).drop(columns=['unit_acct_id','lat','lon']).reset_index(drop=True))
            except AttributeError:
                st.text("Address not found. Please check address and/or enter another.")