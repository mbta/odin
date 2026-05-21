from odin.utils.instance import get_odin_instance


TABLES_ALPHA = [
    "retail.account_actions",
    "retail.activations",
    "retail.rider_entitlement_events",
    "retail.ticket_purchases",
    "retail.tickets",
    "validation.scans",
    "validation.telemetry",
    # "view.hub_search_account",
    # "view.hub_search_guest_account",
    # "view.hub_search_vendor_sale",
    # "view.validators",
]

TABLES_BETA: list[str] = []

TABLES = TABLES_ALPHA + TABLES_BETA

_ODIN_INSTANCE = get_odin_instance()
TABLES_INSTANCE = TABLES_ALPHA if _ODIN_INSTANCE == "alpha" else TABLES_BETA
