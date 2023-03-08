import os

MIDDLEWARE_SECRET_KEY = os.environ.get("MIDDLEWARE_SECRET_KEY", "nav")
# MIDDLEWARE_SECRET_KEY = "1fc!(_dr4g6+mwdf#m8z%t%zq_t*x(7=7hi%(_3(3-%zqxjvk_"
MIDDLEWARE_URL = os.environ.get("MIDDLEWARE_URL", "http://127.0.0.1:8000/api/v1/")
# MIDDLEWARE_URL = "http://127.0.0.1:8000/api/v1/"
# MIDDLEWARE_URL = os.environ.get("MIDDLEWARE_URL", "https://nav.api.ratechcompany.com/api/v1/")

HEADERS = {
    'Accept': 'text/plain, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8,ar;q=0.7,es;q=0.6,nl;q=0.5',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                  ' (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
}

MIDDLEWARE_ROUTES = {
    "company_list": "admin/company/",
    "company_detail": "admin/company/{}/",
    "update_prices": "admin/company/update-prices/",
    "balance_sheet": "balance-sheet/",
    "cash_flow": "cash-flows/",
    "profit_and_loss": "income-statement/",
    "summary_investment": "summary-investment/",
    "summary_portfolio_approved": "summary-portfolio-approved/",
    "summary_portfolio_extern": "admin/summary-portfolio-external/",
    "summary_trade_earned": "summary-trade-earned/",
    "summary_trade_reassigned": "summary-trade-reassigned/",
    "summary_stk_prof_realized": "summary-stkprof-realized/",
    "update_create_market_data": "admin/market-data/",
    "update_create_historical_data": "admin/company/historical-data/",
    "assembly_report": "assembly/",
    "update_create_companies": "admin/company/create-companies/",
    "update_create_industries": "company/create-industries",
    "create_log": "report/logs/",
    "invalid_reports": "report/invalid-reports/",
    "monthly_activity": "monthly-activity/admin/",
    "update_create_industry_index": "admin/company/update-industry-index/",
    "update_create_historical_industry_index": "admin/company/update-historical-industry-index/",
    "get_create_descriptive_one": "descriptive-one/admin/",
    "get_create_descriptive_two": "descriptive-two/admin/",
    "get_create_descriptive_three": "descriptive-three/admin/",
    "get_create_descriptive_four": "descriptive-four/admin/",
    "get_create_descriptive_five": "descriptive-five/admin/",
}

PERIOD = {
    "1": "1 MONTH",
    "3": "3 MONTHS",
    "6": "6 MONTHS",
    "9": "9 MONTHS",
    "12": "12 MONTHS",
}

FIRST_DESCRIPTIVE_FINAL_COST_ADDRESS = {
    'report': "",
    'direct_consumables': "",
    'direct_wages_of_prod': "",
    'production_over_head': "",
    'total': "",
    'unabs_cost_in_prod': "",
    'total_prod_cost': "",
    'inv_goods_prog_beg_period': "",
    'inv_goods_prog_end_period': "",
    'net_inv_goods_prog': "",
    'abnormal_wastes': "",
    'cost_of_man_goods': "",
    'num_of_prod_goods_beg_period': "",
    'num_of_prod_goods_end_period': "",
    'cost_of_goods_sold': "",
    'total_price_service': "",
    'total_price': ""
}
FIRST_DESCRIPTIVE_FINAL_COST_TITLES = [
    'report',
    'direct_consumables',
    'direct_wages_of_prod',
    'production_over_head',
    'total',
    'unabs_cost_in_prod',
    'total_prod_cost',
    'inv_goods_prog_beg_period',
    'inv_goods_prog_end_period',
    'net_inv_goods_prog',
    'abnormal_wastes',
    'cost_of_man_goods',
    'num_of_prod_goods_beg_period',
    'num_of_prod_goods_end_period',
    'cost_of_goods_sold',
    'total_price_service',
    'total_price'
]
SALES_AND_COST_OVER_5_YEARS_ADDRESS = {
    'report': "",
    'sales_amount': "",
    'final_price': ""
}
SALES_AND_COST_OVER_5_YEARS_TITLES = [
    "report",
    "sales_amount",
    "final_price"
]

CEMENT_COMPANIES = []

DESCRIPTIVE_TWO_TYPES = {
    'NORMAL': 'normal',
    'EST1': 'est1',
    'EST2': 'est2'
}

DESCRIPTIVE_THREE_OVER_HEAD = [
    "salary_cost",
    "depreciation_cost",
    "energy_cost",
    "consumables_cost",
    "ads_cost",
    "royalties_and_sales_commissions",
    "after_sale_service_fee",
    "doubtful_claims_cost",
    "transport_cost",
    "other_costs"
]
