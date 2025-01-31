# from pystac_monty.sources.common import MontyDataTransformer
# from pystac_monty.sources.common import MontyDataSource

STAC_EVENT_ID_PREFIX = "ifrc-event-"
STAC_IMPACT_ID_PREFIX = "ifrc-impact-"


class IfrcEventsDataSource():
    event_url: str

    def __init__(self, event_url: str):
        self.event_url = event_url


class IfrcEventsTransformer():
    data_source: IfrcEventsDataSource

    def __init__(self, data_source: IfrcEventsDataSource):
        self.data_source = data_source

    def get_items(self):
        print("\n", self.data_source.event_url)
        return []
