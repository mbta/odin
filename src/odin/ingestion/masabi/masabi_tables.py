from odin.utils.instance import get_odin_instance


TABLES_ALPHA = [
    "retail.rider_entitlement_events",
    "validation.telemetry",
    # "view.hub_search_account",
    # "view.hub_search_guest_account",
    # "view.hub_search_vendor_sale",
    # "view.validators",
    "retail.account_actions",
    "retail.tickets",
    "retail.ticket_refunds",
    "validation.scans",
]

TABLES_BETA: list[str] = [
    "retail.ticket_purchases",
    "retail.activations",
]

TABLES_GAMMA: list[str] = [
    "validation.scans" # DUPLICATED FROM ABOVE, REMOVE ONCE DATA IS BACKFILLED (see https://github.com/mbta/odin/pull/204)
]

TABLES_BY_INSTANCE = {
    "alpha": TABLES_ALPHA,
    "beta": TABLES_BETA,
    "gamma": TABLES_GAMMA,
}

TABLES = TABLES_ALPHA + TABLES_BETA + TABLES_GAMMA

_ODIN_INSTANCE = get_odin_instance()
TABLES_INSTANCE = TABLES_BY_INSTANCE[_ODIN_INSTANCE]
