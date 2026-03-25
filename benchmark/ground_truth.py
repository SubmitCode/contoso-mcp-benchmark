"""Generate benchmark/ground_truth.json by running SQL queries against the Fabric Lakehouse.

Run once (requires .env with Fabric credentials):
    python -m benchmark.ground_truth
"""
import json
from pathlib import Path
from fabric_client.sql import execute_sql

_OUT_PATH = Path(__file__).parent / "ground_truth.json"

# (answer_type, sql) — each query must return a single row
_SCALAR_QUERIES: dict[str, str] = {
    "net_sales_2024": (
        "scalar",
        "SELECT SUM(s.Quantity * s.NetPrice) AS v FROM dbo.sales s "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31'"
    ),
    "quantity_2024": (
        "scalar",
        "SELECT SUM(s.Quantity) AS v FROM dbo.sales s "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31'"
    ),
    "net_sales_germany_2024": (
        "scalar",
        "SELECT SUM(s.Quantity * s.NetPrice) AS v FROM dbo.sales s "
        "LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "AND st.CountryName = 'Germany'"
    ),
    "margin_pct_2024": (
        "scalar",
        "SELECT CASE WHEN SUM(s.Quantity * s.NetPrice) = 0 THEN NULL "
        "ELSE SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 100.0 / SUM(s.Quantity * s.NetPrice) END AS v "
        "FROM dbo.sales s "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31'"
    ),
}

# (answer_type, sql returning ranked rows) — top_1 uses first row, ranked_list collects all
_RANKED_QUERIES: dict[str, tuple[str, str, str]] = {
    # key: (type, sql, column_name)
    "top_country_net_sales_2024": (
        "top_1",
        "SELECT TOP 1 st.CountryName AS v FROM dbo.sales s "
        "LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY st.CountryName ORDER BY SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "top_category_net_sales_2024": (
        "top_1",
        "SELECT TOP 1 p.CategoryName AS v FROM dbo.sales s "
        "LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY p.CategoryName ORDER BY SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "top3_categories_net_sales_2024": (
        "ranked_list",
        "SELECT TOP 3 p.CategoryName AS v FROM dbo.sales s "
        "LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY p.CategoryName ORDER BY SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "top_brand_margin_pct_2024": (
        "top_1",
        "SELECT TOP 1 p.Brand AS v FROM dbo.sales s "
        "LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY p.Brand "
        "HAVING SUM(s.Quantity * s.NetPrice) > 0 "
        "ORDER BY SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 1.0 / SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "top5_brands_net_sales_2024": (
        "ranked_list",
        "SELECT TOP 5 p.Brand AS v FROM dbo.sales s "
        "LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY p.Brand ORDER BY SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "top_continent_quantity_2024": (
        "top_1",
        "SELECT TOP 1 c.Continent AS v FROM dbo.sales s "
        "LEFT JOIN dbo.customer c ON s.CustomerKey = c.CustomerKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY c.Continent ORDER BY SUM(s.Quantity) DESC",
        "v",
    ),
    "top_subcategory_quantity_2024": (
        "top_1",
        "SELECT TOP 1 p.SubCategoryName AS v FROM dbo.sales s "
        "LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "GROUP BY p.SubCategoryName ORDER BY SUM(s.Quantity) DESC",
        "v",
    ),
    "top3_brands_margin_pct_computers_2024": (
        "ranked_list",
        "SELECT TOP 3 p.Brand AS v FROM dbo.sales s "
        "LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
        "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
        "AND p.CategoryName = 'Computers' "
        "GROUP BY p.Brand "
        "HAVING SUM(s.Quantity * s.NetPrice) > 0 "
        "ORDER BY SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 1.0 / SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "top3_countries_net_sales_2023": (
        "ranked_list",
        "SELECT TOP 3 st.CountryName AS v FROM dbo.sales s "
        "LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey "
        "WHERE s.OrderDate >= '2023-01-01' AND s.OrderDate <= '2023-12-31' "
        "GROUP BY st.CountryName ORDER BY SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
    "best_year_margin_pct": (
        "top_1",
        "SELECT TOP 1 CAST(d.Year AS VARCHAR) AS v FROM dbo.sales s "
        "LEFT JOIN dbo.date d ON CAST(s.OrderDate AS DATE) = CAST(d.Date AS DATE) "
        "WHERE d.Year IN (2021, 2022, 2023) "
        "GROUP BY d.Year "
        "HAVING SUM(s.Quantity * s.NetPrice) > 0 "
        "ORDER BY SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 1.0 / SUM(s.Quantity * s.NetPrice) DESC",
        "v",
    ),
}

# Handled with custom logic
_CUSTOM_KEYS = {
    "net_sales_by_quarter_2023",
    "germany_vs_usa_net_sales_2024",
    "h1_vs_h2_net_sales_2023",
    "usa_net_sales_trend_2022_2023",
    "top_month_net_sales_2023",
    "most_improved_category_margin_pct_2022_2023",
}


def _compute_custom(key: str) -> dict:
    if key == "net_sales_by_quarter_2023":
        rows = execute_sql(
            "SELECT d.Quarter, SUM(s.Quantity * s.NetPrice) AS ns FROM dbo.sales s "
            "LEFT JOIN dbo.date d ON CAST(s.OrderDate AS DATE) = CAST(d.Date AS DATE) "
            "WHERE s.OrderDate >= '2023-01-01' AND s.OrderDate <= '2023-12-31' "
            "GROUP BY d.Quarter ORDER BY d.Quarter"
        )
        ordered = [r['Quarter'] for r in sorted(rows, key=lambda r: r["Quarter"])]
        return {"type": "ranked_list", "value": ordered}

    if key == "germany_vs_usa_net_sales_2024":
        rows = execute_sql(
            "SELECT st.CountryName, SUM(s.Quantity * s.NetPrice) AS ns FROM dbo.sales s "
            "LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey "
            "WHERE s.OrderDate >= '2024-01-01' AND s.OrderDate <= '2024-12-31' "
            "AND st.CountryName IN ('Germany', 'United States') "
            "GROUP BY st.CountryName"
        )
        totals = {r["CountryName"]: float(r["ns"]) for r in rows}
        winner = "Germany" if totals.get("Germany", 0) > totals.get("United States", 0) else "United States"
        return {"type": "comparison", "value": winner}

    if key == "h1_vs_h2_net_sales_2023":
        h1 = execute_sql(
            "SELECT SUM(s.Quantity * s.NetPrice) AS v FROM dbo.sales s "
            "WHERE s.OrderDate >= '2023-01-01' AND s.OrderDate <= '2023-06-30'"
        )[0]["v"]
        h2 = execute_sql(
            "SELECT SUM(s.Quantity * s.NetPrice) AS v FROM dbo.sales s "
            "WHERE s.OrderDate >= '2023-07-01' AND s.OrderDate <= '2023-12-31'"
        )[0]["v"]
        winner = "H1" if float(h1) > float(h2) else "H2"
        return {"type": "comparison", "value": winner}

    if key == "usa_net_sales_trend_2022_2023":
        rows = execute_sql(
            "SELECT d.Year, SUM(s.Quantity * s.NetPrice) AS ns FROM dbo.sales s "
            "LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey "
            "LEFT JOIN dbo.date d ON CAST(s.OrderDate AS DATE) = CAST(d.Date AS DATE) "
            "WHERE st.CountryName = 'United States' AND d.Year IN (2022, 2023) "
            "GROUP BY d.Year ORDER BY d.Year"
        )
        by_year = {int(r["Year"]): float(r["ns"]) for r in rows}
        direction = "up" if by_year.get(2023, 0) > by_year.get(2022, 0) else "down"
        return {"type": "trend", "value": direction}

    if key == "top_month_net_sales_2023":
        rows = execute_sql(
            "SELECT TOP 1 MONTH(CAST(s.OrderDate AS DATE)) AS month_num, "
            "SUM(s.Quantity * s.NetPrice) AS ns FROM dbo.sales s "
            "WHERE s.OrderDate >= '2023-01-01' AND s.OrderDate <= '2023-12-31' "
            "GROUP BY MONTH(CAST(s.OrderDate AS DATE)) ORDER BY ns DESC"
        )
        return {"type": "month", "value": int(rows[0]["month_num"])}

    if key == "most_improved_category_margin_pct_2022_2023":
        rows_prev = execute_sql(
            "SELECT p.CategoryName, "
            "SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 1.0 / SUM(s.Quantity * s.NetPrice) AS mp "
            "FROM dbo.sales s LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
            "WHERE s.OrderDate >= '2022-01-01' AND s.OrderDate <= '2022-12-31' "
            "GROUP BY p.CategoryName HAVING SUM(s.Quantity * s.NetPrice) > 0"
        )
        rows_curr = execute_sql(
            "SELECT p.CategoryName, "
            "SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 1.0 / SUM(s.Quantity * s.NetPrice) AS mp "
            "FROM dbo.sales s LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey "
            "WHERE s.OrderDate >= '2023-01-01' AND s.OrderDate <= '2023-12-31' "
            "GROUP BY p.CategoryName HAVING SUM(s.Quantity * s.NetPrice) > 0"
        )
        mp_prev = {r["CategoryName"]: float(r["mp"]) for r in rows_prev}
        mp_curr = {r["CategoryName"]: float(r["mp"]) for r in rows_curr}
        improvements = {cat: mp_curr[cat] - mp_prev.get(cat, 0) for cat in mp_curr}
        best = max(improvements, key=improvements.get)
        return {"type": "top_1", "value": best}

    raise ValueError(f"Unknown custom key: {key}")


def generate() -> dict:
    ground_truth = {}

    print("Computing scalar queries...")
    for key, (atype, sql) in _SCALAR_QUERIES.items():
        rows = execute_sql(sql)
        val = float(list(rows[0].values())[0])
        ground_truth[key] = {"type": atype, "value": val}
        print(f"  {key}: {val:.2f}")

    print("Computing ranked queries...")
    for key, (atype, sql, col) in _RANKED_QUERIES.items():
        rows = execute_sql(sql)
        values = [r[col] for r in rows]
        if atype == "top_1":
            ground_truth[key] = {"type": "top_1", "value": values[0]}
        else:
            ground_truth[key] = {"type": "ranked_list", "value": values}
        print(f"  {key}: {values}")

    print("Computing custom queries...")
    for key in _CUSTOM_KEYS:
        result = _compute_custom(key)
        ground_truth[key] = result
        print(f"  {key}: {result['value']}")

    return ground_truth


if __name__ == "__main__":
    gt = generate()
    _OUT_PATH.write_text(json.dumps(gt, indent=2, default=str))
    print(f"\nWrote {len(gt)} entries to {_OUT_PATH}")
