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
@st.cache(ttl=600, allow_output_mutation=True)
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
    """Returns `True` if the user is with an email we are aware of.
    This was created as a quick workaround for a private/public request
    from the team.
    """

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

    def load_data(lon, lat, house_type):
        if house_type == 'Single Family':
            house_tuple = ('single', '')
        elif house_type == 'Mulitunit Complexes':
            house_tuple = ('multiunit', '')
        else:
            house_tuple = ('single','multiunit')

        unreg_query = f"""
        WITH addresses AS (SELECT
          p.unit_acct_id,
          l.latitude as lat,
          l.longitude as lon,
          appraisal_addr_parcel as address,
          unit,
          city,
          zip,
          CASE WHEN unit IS NULL OR unit = '' THEN 'single' ELSE 'multiunit' END AS house_type,
          ST_DISTANCE(ST_GEOGPOINT({lon}, {lat}), ST_GEOGPOINT(CAST(l.longitude AS NUMERIC), CAST(l.latitude AS NUMERIC))) AS distance
        FROM `demstxsp.vr_data.harris_parcel_partisanship_predictions` p
        JOIN (
          SELECT
            ll.unit_acct_id,
            ll.latitude,
            ll.longitude,
            LTRIM(p.PREC,'0') as precinct
          FROM
            `demstxsp.vr_data.harris_parcel_lat_lng` ll,
            `demstxsp.geo.precincts_2022` p
          WHERE
            ST_CONTAINS(SAFE.ST_GEOGFROM(p.geometry), ST_GEOGPOINT(CAST(ll.longitude AS NUMERIC), CAST(ll.latitude AS NUMERIC)))
            AND p.CNTY = 201
        ) l
          USING (unit_acct_id)
        JOIN `demstxsp.vr_data.harris_address_parcel` a
          USING (unit_acct_id)
        WHERE
            p.predicted_tdp_partisanship_range = "70-100"
            AND l.latitude IS NOT NULL
            AND ST_DWITHIN(ST_GEOGPOINT({lon}, {lat}), ST_GEOGPOINT(CAST(l.longitude AS NUMERIC), CAST(l.latitude AS NUMERIC)), 16000)
        )
        (SELECT * EXCEPT (house_type)
        FROM addresses
        WHERE house_type IN {house_tuple}
        )
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
        """This function takes an address in street number + street format, a city,
        and a zip, and returns the geocoded latitude and longitude.
        """
        addr = f'{address + " " + city + ", TX " + zip}'
        locator = Nominatim(user_agent="address_nearby")
        location = locator.geocode(f"{addr}")

        return location.latitude, location.longitude, addr

    def check_zip(zip):
        """This function checks to ensure that the zip is an integer, 
        returning False if it is not.
        """
        try:
            int(zip)
        except:
            result = False
        else:
            result = True

        return result

    with st.sidebar.form("start_address"):
        address = st.text_input(label='Street Address, excluding unit/apartment', placeholder='ex: 927 Dart St')
        city = st.text_input(label='City', placeholder='ex: Houston')
        zip = st.text_input(label='Zip5 Code', placeholder='ex: 77001', max_chars=5, help='5 digit zip code, must be numeric')
        # radius_size = st.slider('size of dots', 0, 25, 9)
        housing_type = st.selectbox('Housing Type', ['Single Family', 'Mulitunit Complexes', 'Both'])

        # Every form must have a submit button.
        submitted = st.form_submit_button("Submit")
        if submitted:
            if city:
                if check_zip(zip):
                    try:
                        geocode_add(address=address, city=city, zip=zip)
                    except:
                        st.text("Address not found. Try again.")
                        submitted = False
                    else:
                        lat, lon, addr = geocode_add(address=address, city=city, zip=zip)
                else:
                    st.text('Zip must be numeric.')
                    submitted = False
            else:
                st.text('Please enter a city')
                submitted = False

    if submitted:
        app_title.title('Addresses and Locations to canvass')
        data_load_state.text('Loading data...')
        df_canvass = load_data(lon=lon, lat=lat, house_type=housing_type)
        data_load_state.text("")

        map_title = st.subheader(f'50 closest addresses to {addr}')
        map_text = st.text('Addresses are indicated by purple dots on the map.\nYou may need to zoom in/out to get a better view,\nespecially for areas that may be less densely populated.')

        if not df_canvass.empty:
            map = st.pydeck_chart(pdk.Deck(
                 map_style='mapbox://styles/mapbox/streets-v11',
                 initial_view_state=pdk.ViewState(
                     latitude=df_canvass.iloc[0]['lat'],
                     longitude=df_canvass.iloc[0]['lon'],
                     zoom=16,
                     pitch=0,
                 ),
                 layers=[
                     pdk.Layer(
                         'ScatterplotLayer',
                         data=df_canvass,
                         pickable=False,
                         opacity=0.5,
                         stroked=True,
                         filled=True,
                         radius_scale=2,
                         radius_min_pixels=0,
                         radius_max_pixels=30,
                         line_width_min_pixels=1,
                         get_position='[lon, lat]',
                         get_radius=30,
                         get_color='[69, 47, 110]',
                     ),
                 ],
            ))
            raw_title = st.subheader('Raw data, closest to furthest')
            df_canvass_sort = df_canvass.sort_values(by=['distance', 'address', 'unit']).drop(columns=['unit_acct_id','lat','lon','distance']).reset_index(drop=True)
            df = st.dataframe(df_canvass_sort)

        else:
            map_title.subheader('No addresses found nearby. Please re-submit with a new address.')
            map_text.text('')
