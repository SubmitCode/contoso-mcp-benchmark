"""Generate benchmark/ground_truth.json by running DAX queries against the semantic model.

Run once (requires az login with Power BI scope):
    python -m benchmark.ground_truth
"""
import json
from pathlib import Path

from fabric_client.dax import execute_dax

_OUT_PATH = Path(__file__).parent / "ground_truth.json"

_MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


def _date_filter(y1: int, m1: int, d1: int, y2: int, m2: int, d2: int) -> str:
    return (
        f"FILTER(ALL('Date'), 'Date'[Date] >= DATE({y1},{m1},{d1}) "
        f"&& 'Date'[Date] <= DATE({y2},{m2},{d2}))"
    )


def _scalar(measure: str, y1: int, m1: int, d1: int, y2: int, m2: int, d2: int, **extra_filters) -> float:
    filter_parts = [_date_filter(y1, m1, d1, y2, m2, d2)]
    for col, val in extra_filters.items():
        filter_parts.append(f'{col} = "{val}"')
    filter_str = ", ".join(filter_parts)
    # For simple scalar with extra dimension filters, use CALCULATE with direct filter
    if extra_filters:
        dim_filters = ", ".join(f'{col} = "{val}"' for col, val in extra_filters.items())
        dax = (
            f'EVALUATE ROW("v", CALCULATE([{measure}], '
            f'{_date_filter(y1, m1, d1, y2, m2, d2)}, {dim_filters}))'
        )
    else:
        dax = (
            f'EVALUATE ROW("v", CALCULATE([{measure}], '
            f'{_date_filter(y1, m1, d1, y2, m2, d2)}))'
        )
    rows = execute_dax(dax)
    return float(rows[0]["v"])


def _top_n_by(n: int, group_col: str, measure: str, y1: int, m1: int, d1: int, y2: int, m2: int, d2: int,
              extra_filter: str = None) -> list:
    filter_parts = [_date_filter(y1, m1, d1, y2, m2, d2)]
    if extra_filter:
        filter_parts.append(extra_filter)
    filters_str = ",\n        ".join(filter_parts)
    dax = (
        f"EVALUATE\n"
        f"TOPN({n},\n"
        f"    SUMMARIZECOLUMNS(\n"
        f"        {group_col},\n"
        f"        {filters_str},\n"
        f'        "{measure}", [{measure}]\n'
        f"    ),\n"
        f"    [{measure}], DESC\n"
        f")\n"
        f'ORDER BY [{measure}] DESC'
    )
    rows = execute_dax(dax)
    col_name = group_col.split("[")[-1].rstrip("]")
    return [r[col_name].strip() if isinstance(r[col_name], str) else r[col_name] for r in rows]


def generate() -> dict:
    gt: dict = {}

    print("Computing scalar queries...")

    v = _scalar("Net Sales", 2024, 1, 1, 2024, 12, 31)
    gt["net_sales_2024"] = {"type": "scalar", "value": v}
    print(f"  net_sales_2024: {v:,.2f}")

    v = _scalar("Total Quantity",2024, 1, 1, 2024, 12, 31)
    gt["quantity_2024"] = {"type": "scalar", "value": v}
    print(f"  quantity_2024: {v:,.0f}")

    v = _scalar("Net Sales", 2024, 1, 1, 2024, 12, 31, **{"Stores[CountryName]": "Germany"})
    gt["net_sales_germany_2024"] = {"type": "scalar", "value": v}
    print(f"  net_sales_germany_2024: {v:,.2f}")

    # Margin % is stored as 0–1 fraction; multiply by 100 for percentage display
    v = _scalar("Margin %", 2024, 1, 1, 2024, 12, 31) * 100
    gt["margin_pct_2024"] = {"type": "scalar", "value": v}
    print(f"  margin_pct_2024: {v:.2f}%")

    print("Computing ranked queries...")

    vals = _top_n_by(1, "Stores[CountryName]", "Net Sales", 2024, 1, 1, 2024, 12, 31)
    gt["top_country_net_sales_2024"] = {"type": "top_1", "value": vals[0]}
    print(f"  top_country_net_sales_2024: {vals[0]}")

    vals = _top_n_by(1, "Products[CategoryName]", "Net Sales", 2024, 1, 1, 2024, 12, 31)
    gt["top_category_net_sales_2024"] = {"type": "top_1", "value": vals[0]}
    print(f"  top_category_net_sales_2024: {vals[0]}")

    vals = _top_n_by(3, "Products[CategoryName]", "Net Sales", 2024, 1, 1, 2024, 12, 31)
    gt["top3_categories_net_sales_2024"] = {"type": "ranked_list", "value": vals}
    print(f"  top3_categories_net_sales_2024: {vals}")

    vals = _top_n_by(1, "Products[Brand]", "Margin %", 2024, 1, 1, 2024, 12, 31)
    gt["top_brand_margin_pct_2024"] = {"type": "top_1", "value": vals[0]}
    print(f"  top_brand_margin_pct_2024: {vals[0]}")

    vals = _top_n_by(5, "Products[Brand]", "Net Sales", 2024, 1, 1, 2024, 12, 31)
    gt["top5_brands_net_sales_2024"] = {"type": "ranked_list", "value": vals}
    print(f"  top5_brands_net_sales_2024: {vals}")

    vals = _top_n_by(1, "Customers[Continent]", "Total Quantity", 2024, 1, 1, 2024, 12, 31)
    gt["top_continent_quantity_2024"] = {"type": "top_1", "value": vals[0]}
    print(f"  top_continent_quantity_2024: {vals[0]}")

    vals = _top_n_by(1, "Products[SubCategoryName]", "Total Quantity", 2024, 1, 1, 2024, 12, 31)
    gt["top_subcategory_quantity_2024"] = {"type": "top_1", "value": vals[0]}
    print(f"  top_subcategory_quantity_2024: {vals[0]}")

    vals = _top_n_by(
        3, "Products[Brand]", "Margin %", 2024, 1, 1, 2024, 12, 31,
        extra_filter='FILTER(ALL(Products), Products[CategoryName] = "Computers")',
    )
    gt["top3_brands_margin_pct_computers_2024"] = {"type": "ranked_list", "value": vals}
    print(f"  top3_brands_margin_pct_computers_2024: {vals}")

    vals = _top_n_by(3, "Stores[CountryName]", "Net Sales", 2023, 1, 1, 2023, 12, 31)
    gt["top3_countries_net_sales_2023"] = {"type": "ranked_list", "value": vals}
    print(f"  top3_countries_net_sales_2023: {vals}")

    # Best year margin % (among 2021, 2022, 2023)
    dax = (
        "EVALUATE\n"
        "TOPN(1,\n"
        "    SUMMARIZECOLUMNS(\n"
        "        'Date'[Year],\n"
        "        FILTER(ALL('Date'), 'Date'[Year] IN {2021, 2022, 2023}),\n"
        '        "Margin %", [Margin %]\n'
        "    ),\n"
        "    [Margin %], DESC\n"
        ")"
    )
    rows = execute_dax(dax)
    best_year = str(int(rows[0]["Year"]))
    gt["best_year_margin_pct"] = {"type": "top_1", "value": best_year}
    print(f"  best_year_margin_pct: {best_year}")

    print("Computing custom queries...")

    # Net Sales by quarter 2023 (ordered Q1–Q4)
    dax = (
        "EVALUATE\n"
        "SUMMARIZECOLUMNS(\n"
        "    'Date'[Quarter],\n"
        f"    {_date_filter(2023,1,1,2023,12,31)},\n"
        '    "Net Sales", [Net Sales]\n'
        ")\n"
        "ORDER BY 'Date'[Quarter] ASC"
    )
    rows = execute_dax(dax)
    quarters = [r["Quarter"] for r in rows]
    gt["net_sales_by_quarter_2023"] = {"type": "ranked_list", "value": quarters}
    print(f"  net_sales_by_quarter_2023: {quarters}")

    # Germany vs USA net sales 2024
    dax = (
        "EVALUATE\n"
        "SUMMARIZECOLUMNS(\n"
        "    Stores[CountryName],\n"
        f"    {_date_filter(2024,1,1,2024,12,31)},\n"
        '    FILTER(ALL(Stores), Stores[CountryName] IN {"Germany", "United States"}),\n'
        '    "Net Sales", [Net Sales]\n'
        ")"
    )
    rows = execute_dax(dax)
    totals = {r["CountryName"]: float(r["Net Sales"]) for r in rows}
    winner = "Germany" if totals.get("Germany", 0) > totals.get("United States", 0) else "United States"
    gt["germany_vs_usa_net_sales_2024"] = {"type": "comparison", "value": winner}
    print(f"  germany_vs_usa_net_sales_2024: {winner}")

    # H1 vs H2 net sales 2023
    h1 = _scalar("Net Sales", 2023, 1, 1, 2023, 6, 30)
    h2 = _scalar("Net Sales", 2023, 7, 1, 2023, 12, 31)
    winner = "H1" if h1 > h2 else "H2"
    gt["h1_vs_h2_net_sales_2023"] = {"type": "comparison", "value": winner}
    print(f"  h1_vs_h2_net_sales_2023: {winner} (H1={h1:,.0f}, H2={h2:,.0f})")

    # USA net sales trend 2022→2023
    dax = (
        "EVALUATE\n"
        "SUMMARIZECOLUMNS(\n"
        "    'Date'[Year],\n"
        "    FILTER(ALL('Date'), 'Date'[Year] IN {2022, 2023}),\n"
        '    FILTER(ALL(Stores), Stores[CountryName] = "United States"),\n'
        '    "Net Sales", [Net Sales]\n'
        ")\n"
        "ORDER BY 'Date'[Year] ASC"
    )
    rows = execute_dax(dax)
    by_year = {int(r["Year"]): float(r["Net Sales"]) for r in rows}
    direction = "up" if by_year.get(2023, 0) > by_year.get(2022, 0) else "down"
    gt["usa_net_sales_trend_2022_2023"] = {"type": "trend", "value": direction}
    print(f"  usa_net_sales_trend_2022_2023: {direction}")

    # Top month net sales 2023 (as integer 1–12)
    dax = (
        "EVALUATE\n"
        "TOPN(1,\n"
        "    SUMMARIZECOLUMNS(\n"
        "        'Date'[Month],\n"
        f"        {_date_filter(2023,1,1,2023,12,31)},\n"
        '        "Net Sales", [Net Sales]\n'
        "    ),\n"
        "    [Net Sales], DESC\n"
        ")"
    )
    rows = execute_dax(dax)
    month_name = rows[0]["Month"]
    month_num = _MONTH_MAP[month_name]
    gt["top_month_net_sales_2023"] = {"type": "month", "value": month_num}
    print(f"  top_month_net_sales_2023: {month_name} ({month_num})")

    # Most improved category margin % from 2022 to 2023
    def _margin_by_category(y: int) -> dict[str, float]:
        dax = (
            "EVALUATE\n"
            "SUMMARIZECOLUMNS(\n"
            "    Products[CategoryName],\n"
            f"    {_date_filter(y,1,1,y,12,31)},\n"
            '    "Margin %", [Margin %]\n'
            ")"
        )
        rows = execute_dax(dax)
        return {r["CategoryName"]: float(r["Margin %"]) for r in rows}

    mp_2022 = _margin_by_category(2022)
    mp_2023 = _margin_by_category(2023)
    improvements = {cat: mp_2023[cat] - mp_2022.get(cat, 0) for cat in mp_2023}
    best = max(improvements, key=improvements.get)
    gt["most_improved_category_margin_pct_2022_2023"] = {"type": "top_1", "value": best}
    print(f"  most_improved_category_margin_pct_2022_2023: {best}")

    return gt


if __name__ == "__main__":
    gt = generate()
    _OUT_PATH.write_text(json.dumps(gt, indent=2, default=str))
    print(f"\nWrote {len(gt)} entries to {_OUT_PATH}")
