glide_event_collection = {
    "stac_version": "1.0.0",
    "stac_extensions": ["https://ifrcgo.github.io/monty/v0.1.0/schema.json"],
    "type": "Collection",
    "id": "glide-events",
    "title": "Glide Source Events",
    "description": "GLobal IDEntifier Number (GLIDE)",
    "license": "MIT",
    "roles": ["event", "source"],
    "providers": [{"name": "Glide", "roles": ["producer"], "url": "https://www.glidenumber.net/", "email": "gliderep@adrc.asia"}],
    "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2000-01-01T00:00:00Z", ""]]}},
    "summaries": {
        "datetime": {"minimum": "2015-06-23T00:00:00Z", "maximum": "2019-07-10T13:44:56Z"},
        "roles": ["event", "source"],
        "monty:country_codes": [],
        "monty:hazard_codes": [],
    },
    "links": [],
}
