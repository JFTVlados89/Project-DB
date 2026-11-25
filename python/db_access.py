from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


LOG = logging.getLogger("project_db")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
load_dotenv(Path(__file__).with_name(".env"))

# Table and column names are intentionally allow-listed. CSV input can supply
# values, but it cannot choose SQL identifiers.
TABLE_SPECS: dict[str, dict[str, Any]] = {
    "category": {"columns": ("category_id", "category_name"), "key": "category_id", "unique": ("category_name",)},
    "supplier": {"columns": ("supplier_id",), "key": "supplier_id", "unique": ()},
    "location": {"columns": ("location_id", "city", "country"), "key": "location_id", "unique": ()},
    "age_group": {"columns": ("age_group_id", "range_code"), "key": "age_group_id", "unique": ("range_code",)},
    "gender": {"columns": ("gender_id", "gender_name"), "key": "gender_id", "unique": ("gender_name",)},
    "shipping_method": {
        "columns": ("shipping_method_id", "method_name", "base_cost"),
        "key": "shipping_method_id",
        "unique": ("method_name",),
    },
    "product": {
        "columns": ("product_sk", "product_id", "product_name", "category_id", "supplier_id", "price", "stock_level", "tax_rate", "return_rate"),
        "key": "product_sk",
        "unique": ("product_id",),
    },
    "sales_record": {
        "columns": ("sale_id", "product_sk", "location_id", "age_group_id", "gender_id", "shipping_method_id", "discount", "shipping_cost", "seasonality", "popularity_index"),
        "key": "sale_id",
        "unique": (),
    },
}


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Не задана обязательная переменная окружения {name}.")
    return value


def _schema() -> str:
    value = os.getenv("DB_SCHEMA", "public").strip()
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError("DB_SCHEMA должен быть простым именем схемы PostgreSQL.")
    return value


def make_engines() -> tuple[Engine, Engine]:
    """Return (write_engine, read_engine); reads fall back to the primary."""
    write_url = _required_env("DATABASE_URL")
    read_url = os.getenv("DATABASE_READ_URL", "").strip() or write_url
    return (
        create_engine(write_url, pool_pre_ping=True),
        create_engine(read_url, pool_pre_ping=True),
    )


def get_product(engine: Engine, product_id: str) -> dict[str, Any] | None:
    """Parameterized SELECT by the natural product identifier from the CSV."""
    statement = text(
        f"SELECT product_sk, product_id, product_name, category_id, supplier_id, "
        f"price, stock_level, tax_rate, return_rate "
        f"FROM {_schema()}.product WHERE product_id = :product_id"
    )
    with engine.connect() as connection:
        row = connection.execute(statement, {"product_id": product_id}).mappings().first()
        return dict(row) if row else None


def upsert_product(
    engine: Engine,
    *,
    product_id: str,
    product_name: str,
    category_id: int,
    supplier_id: str,
    price: float,
    stock_level: int,
    tax_rate: int,
    return_rate: float | None,
) -> None:
    """Insert/update one product atomically; engine.begin rolls back on error."""
    statement = text(
        f"""INSERT INTO {_schema()}.product
            (product_id, product_name, category_id, supplier_id, price,
             stock_level, tax_rate, return_rate)
            VALUES (:product_id, :product_name, :category_id, :supplier_id,
                    :price, :stock_level, :tax_rate, :return_rate)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category_id = EXCLUDED.category_id,
                supplier_id = EXCLUDED.supplier_id,
                price = EXCLUDED.price,
                stock_level = EXCLUDED.stock_level,
                tax_rate = EXCLUDED.tax_rate,
                return_rate = EXCLUDED.return_rate"""
    )
    values = {
        "product_id": product_id,
        "product_name": product_name,
        "category_id": category_id,
        "supplier_id": supplier_id,
        "price": price,
        "stock_level": stock_level,
        "tax_rate": tax_rate,
        "return_rate": return_rate,
    }
    with engine.begin() as connection:
        connection.execute(statement, values)


def _clean_cell(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str | None]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"В файле {path} отсутствует строка заголовков.")
        fields = [field.strip() for field in reader.fieldnames]
        rows = [{key.strip(): _clean_cell(value) for key, value in row.items() if key is not None} for row in reader]
    return fields, rows


def batch_load(engine: Engine, loads: Iterable[tuple[str, Path]], batch_size: int = 1000) -> dict[str, int]:
    """Load normalized CSVs for several allow-listed tables in one transaction.

    CSV headers must match the table columns exactly (except generated surrogate
    keys may be omitted where the SQL schema supplies a DEFAULT identity).
    Foreign keys in the normalized files must already refer to loaded rows.
    """
    if batch_size < 1:
        raise ValueError("batch_size должен быть положительным числом")

    prepared: list[tuple[str, list[str], list[dict[str, str | None]], str]] = []
    for table, path in loads:
        if table not in TABLE_SPECS:
            raise ValueError(f"Таблица {table!r} не разрешена для пакетной загрузки.")
        fields, rows = _read_csv_rows(path)
        allowed = set(TABLE_SPECS[table]["columns"])
        if not fields or len(fields) != len(set(fields)) or not set(fields) <= allowed:
            raise ValueError(f"Недопустимые или повторные поля в CSV для {table}: {fields}")
        if not rows:
            prepared.append((table, fields, rows, ""))
            continue
        # Values remain bound parameters. Identifiers come only from TABLE_SPECS.
        columns_sql = ", ".join(f'"{field}"' for field in fields)
        placeholders = ", ".join(f":{field}" for field in fields)
        conflict_key = TABLE_SPECS[table]["key"]
        update_columns = [field for field in fields if field not in {conflict_key, *TABLE_SPECS[table]["unique"]}]
        conflict_sql = f' ON CONFLICT ("{conflict_key}") '
        if update_columns:
            update_sql = ", ".join(f'"{column}" = EXCLUDED."{column}"' for column in update_columns)
            conflict_sql += f"DO UPDATE SET {update_sql}"
        else:
            conflict_sql += "DO NOTHING"
        statement = (
            f'INSERT INTO "{_schema()}"."{table}" ({columns_sql}) '
            f"VALUES ({placeholders}){conflict_sql}"
        )
        prepared.append((table, fields, rows, statement))

    totals: dict[str, int] = {}
    # A failure in any table rolls the complete multi-table batch back.
    with engine.begin() as connection:
        for table, _fields, rows, statement in prepared:
            if not rows:
                totals[table] = 0
                continue
            affected = 0
            for start in range(0, len(rows), batch_size):
                result = connection.execute(text(statement), rows[start : start + batch_size])
                affected += max(result.rowcount, 0)
            totals[table] = affected
    return totals


def check_data_quality(engine: Engine) -> list[dict[str, Any]]:
    """Report row counts, required-field nulls, and duplicate unique keys."""
    schema = _schema()
    report: list[dict[str, Any]] = []
    with engine.connect() as connection:
        for table, spec in TABLE_SPECS.items():
            columns = spec["columns"]
            # PK/FK/NOT NULL enforcement is the database's responsibility; this
            # report helps inspect staging or existing tables before submission.
            required = [col for col in columns if col.endswith("_id") or col in {"product_name", "method_name", "range_code", "gender_name", "city", "price", "stock_level", "tax_rate", "discount", "shipping_cost", "seasonality", "popularity_index"}]
            count = connection.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar_one()
            nulls: dict[str, int] = {}
            for column in required:
                nulls[column] = connection.execute(
                    text(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE "{column}" IS NULL')
                ).scalar_one()
            duplicate_keys: dict[str, int] = {}
            for column in (spec["key"], *spec["unique"]):
                duplicate_keys[column] = connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM '
                        f'(SELECT "{column}" FROM "{schema}"."{table}" '
                        f'GROUP BY "{column}" HAVING COUNT(*) > 1) AS duplicate_groups'
                    )
                ).scalar_one()
            report.append({"table": table, "rows": count, "nulls": nulls, "duplicate_key_groups": duplicate_keys})
    return report


def export_mart(engine: Engine, output: Path) -> int:
    """Export the team's materialized sales mart to UTF-8 CSV via pandas."""
    view_name = os.getenv("ANALYTICS_VIEW", "mv_sales_analytics").strip()
    if not IDENTIFIER_RE.fullmatch(view_name):
        raise ValueError("ANALYTICS_VIEW должен быть простым именем представления.")
    schema = _schema()
    frame = pd.read_sql_query(f'SELECT * FROM "{schema}"."{view_name}"', engine)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False, encoding="utf-8-sig")
    return len(frame.index)


def _parse_load(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("Формат загрузки: table=path.csv")
    table, file_path = value.split("=", 1)
    return table.strip(), Path(file_path).expanduser()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Доступ к учебной БД электронной торговли Project-DB")
    commands = parser.add_subparsers(dest="command", required=True)
    get_command = commands.add_parser("get-product", help="Найти товар по исходному Product ID")
    get_command.add_argument("product_id")
    upsert = commands.add_parser("upsert-product", help="Вставить/обновить товар в транзакции")
    upsert.add_argument("--product-id", required=True)
    upsert.add_argument("--product-name", required=True)
    upsert.add_argument("--category-id", required=True, type=int)
    upsert.add_argument("--supplier-id", required=True)
    upsert.add_argument("--price", required=True, type=float)
    upsert.add_argument("--stock-level", required=True, type=int)
    upsert.add_argument("--tax-rate", required=True, type=int)
    upsert.add_argument("--return-rate", type=float)
    export = commands.add_parser("export-mart", help="Выгрузить материализованную витрину в CSV")
    export.add_argument("output", type=Path)
    batch = commands.add_parser("batch-load", help="Пакетно загрузить нормализованные CSV в несколько таблиц")
    batch.add_argument("loads", nargs="+", type=_parse_load, metavar="TABLE=CSV")
    batch.add_argument("--batch-size", type=int, default=1000)
    commands.add_parser("quality", help="Проверить количество строк, NULL и дубли ключей")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        write_engine, read_engine = make_engines()
        if args.command == "get-product":
            result = get_product(read_engine, args.product_id)
            print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
            return 0 if result else 1
        if args.command == "upsert-product":
            upsert_product(
                write_engine,
                product_id=args.product_id,
                product_name=args.product_name,
                category_id=args.category_id,
                supplier_id=args.supplier_id,
                price=args.price,
                stock_level=args.stock_level,
                tax_rate=args.tax_rate,
                return_rate=args.return_rate,
            )
            LOG.info("Товар %s сохранён.", args.product_id)
        elif args.command == "export-mart":
            rows = export_mart(read_engine, args.output)
            LOG.info("Выгружено строк: %s; файл: %s", rows, args.output)
        elif args.command == "batch-load":
            result = batch_load(write_engine, args.loads, args.batch_size)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.command == "quality":
            print(json.dumps(check_data_quality(read_engine), ensure_ascii=False, indent=2, default=str))
        write_engine.dispose()
        if read_engine is not write_engine:
            read_engine.dispose()
        return 0
    except (ValueError, OSError, SQLAlchemyError) as exc:
        LOG.error("Операция не выполнена: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
