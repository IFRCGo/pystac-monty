def rename_columns(df):
    """Rename columns of a pandas DataFrame"""
    return df.rename(
        columns={
            "disno": "DisNo.",
            "admin_units": "Admin Units",
            "latitude": "Latitude",
            "longitude": "Longitude",
            "country": "Country",
            "classif_key": "Classification Key",
            "iso": "ISO",
            "total_deaths": "Total Deaths",
            "no_injured": "No Injured",
            "no_affected": "No Affected",
            "no_homeless": "No Homeless",
            "total_affected": "Total Affected",
            "total_dam": "Total Damages ('000 US$)",
            "start_year": "Start Year",
            "start_month": "Start Month",
            "start_day": "Start Day",
            "end_year": "End Year",
            "end_month": "End Month",
            "end_day": "End Day",
            "magnitude": "Magnitude",
            "magnitude_scale": "Magnitude Sacle",
            "name": "Event Name",
            "type": "Disaster Type",
            "subtype": "Disaster Subtype",
            "location": "Location",
        }
    )
