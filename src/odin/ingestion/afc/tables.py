from odin.utils.instance import get_odin_instance


# Table returned by `tableinfos` endpoint as of October 20, 2025
API_TABLES_ALPHA = [
    "v_business_entities",
    "v_ca_legal_relations",
    "v_deviceclass",
    "v_eventgroup",
    "v_eventhistory",
    "v_eventtext",
    "v_legal_persons",
    "v_mainshift",
    "v_medium_types",
    "v_person",
    "v_product_templates",
    "v_routes",
    "v_sales_txns",
    "v_shiftevent",
    "v_stop_points",
    "v_ta_ca_relations",
    "v_ta_legal_relations",
    "v_trips",
    "v_tvmstation",
    "v_tvmtable",
    "v_validation_taps",
    "v_cashless_payments",
    "v_inspections",
    "v_tsmstatus",
    "v_salesdetail",
    "v_salestransaction",
    "v_lines",
    "v_users",
    "v_groups_roles",
    "v_user_group_relations",
    "v_cashtype",
    "v_cashboxmovement",
    "v_cashboxmovementmoneydetails",
    "v_moneycontainersum",
    "v_moneycontainercontentsum",
    "v_accesslevel",
    "v_svw_balance_changes",
    "v_transit_accounts",
    "v_payment_methods",
    "v_entitlements",
    "v_entitlements_full",  # temporary full snapshot export containing older data
    "v_payment_method_instances",
    "v_products",
    "v_versions",
]

API_TABLES_BETA: list[str] = []

API_TABLES = API_TABLES_ALPHA + API_TABLES_BETA

_ODIN_INSTANCE = get_odin_instance()
API_TABLES_INSTANCE = API_TABLES_ALPHA if _ODIN_INSTANCE == "alpha" else API_TABLES_BETA
