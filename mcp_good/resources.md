# Contoso 1M — Retail Sales Data Model

## Dataset

Contoso 1M: ~1 million retail sales orders across 5 tables: `sales`, `product`, `customer`, `store`, `date`.

## Measures

| Measure | Description |
|---------|-------------|
| `Net Sales` | Sum of Quantity × NetPrice |
| `Margin` | Sum of (NetPrice − UnitCost) × Quantity |
| `Margin %` | Margin / Net Sales (NULL if Net Sales = 0) |
| `Total Quantity` | Sum of units sold |

## Dimensions

| Dimension | Description |
|-----------|-------------|
| `ProductName` | Individual product name — **high-cardinality**, always filter by Brand or Category first |
| `Brand` | Product brand |
| `Category` / `CategoryName` | Product category |
| `Subcategory` / `SubCategoryName` | Product sub-category |
| `Country` / `CountryName` | Country where the sale occurred |
| `Continent` | Customer continent |
| `Year` | Order year (integer) |
| `Quarter` | Order quarter (integer 1–4) |
| `Month` | Order month (integer 1–12) |

## Sample Queries

**Net Sales by Country for 2024**
```
get_kpi("Net Sales", ["Country"], {"from": "2024-01-01", "to": "2024-12-31"})
```

**Margin % by Category and Year (2023–2024)**
```
get_kpi("Margin %", ["CategoryName", "Year"], {"from": "2023-01-01", "to": "2024-12-31"})
```

**Top 10 product SKUs by Margin in Q1 2024**
```
get_top_product_skus("Margin", {"from": "2024-01-01", "to": "2024-03-31"}, n=10)
```

**Top 10 Computers by Net Sales in 2023**
```
get_top_product_skus("Net Sales", {"from": "2023-01-01", "to": "2023-12-31"}, n=10, category="Computers")
```

**Net Sales by Country and Year (2022–2024)**
```
get_kpi("Net Sales", ["Country", "Year"], {"from": "2022-01-01", "to": "2024-12-31"})
```

## Anti-Patterns

- **Do NOT** query `ProductName` without a `Brand` or `Category` filter — produces thousands of rows.
- `date_range` is required for all measures. Omitting it raises an error.
- `date_range.from` must be ≤ `date_range.to`; both must be valid ISO dates (`YYYY-MM-DD`).
- `date_range.from` must not be a future date — the dataset contains historical sales only.
- Maximum 100 rows returned per query. Add filters or narrow the date range to reduce results.
- Use `get_dimension_values()` to discover valid filter values before filtering.
