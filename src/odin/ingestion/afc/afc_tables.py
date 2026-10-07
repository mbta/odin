from odin.utils.instance import get_odin_instance

# Table returned by `tableinfos` endpoint as of October 2, 2026
API_TABLES_ALPHA = [
    "v_accesslevel",
    "v_audits",
    "v_business_entities",
    "v_ca_legal_relations",
    "v_cashboxmovement",
    "v_cashboxmovementmoneydetails",
    "v_cashtype",
    "v_deviceclass",
    "v_entitlements",
    "v_entitlements_full",
    "v_eventgroup",
    "v_eventtext",
    "v_fraud_events",
    "v_groups_roles",
    "v_inspections",
    "v_legal_persons",
    "v_lines",
    "v_medium_types",
    "v_moneycontainercontentsum",
    "v_moneycontainersum",
    "v_payment_method_instances",
    "v_payment_methods",
    "v_person",
    "v_product_templates",
    "v_rms",
    "v_rms_ledgers",
    "v_routes",
    "v_salesdetail",
    "v_salestransaction",
    "v_shiftevent",
    "v_stop_points",
    "v_ta_ca_relations",
    "v_ta_legal_relations",
    "v_transit_accounts",
    "v_tsmstatus",
    "v_tvmstation",
    "v_tvmtable",
    "v_user_group_relations",
    "v_users",
    "v_versions",
]

API_TABLES_BETA: list[str] = [
    "v_validation_taps",
    "v_sales_txns",
    "v_products",
    "v_svw_balance_changes",
    "v_eventhistory",
    "v_mainshift",
    "v_trips",
    "v_cashless_payments",
]

API_TABLES_GAMMA: list[str] = []

API_TABLES_DELTA: list[str] = []

API_TABLES_BY_INSTANCE = {
    "alpha": API_TABLES_ALPHA,
    "beta": API_TABLES_BETA,
    "gamma": API_TABLES_GAMMA,
    "delta": API_TABLES_DELTA,
}

API_TABLES = API_TABLES_ALPHA + API_TABLES_BETA + API_TABLES_GAMMA + API_TABLES_DELTA

_ODIN_INSTANCE = get_odin_instance()
API_TABLES_INSTANCE = API_TABLES_BY_INSTANCE[_ODIN_INSTANCE]
