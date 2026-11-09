# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "37d2ab88-f02c-4e21-84a3-019961a3b0c4",
# META       "default_lakehouse_name": "lh_team_06",
# META       "default_lakehouse_workspace_id": "0c5245c3-d2f1-4431-a687-63f45cb1b5fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "37d2ab88-f02c-4e21-84a3-019961a3b0c4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_silver – Bronze → Silver
# 
# Z každé tabulky `bronze_*` vytvoří `silver_*`. Celý Silver se při každém běhu přepočítá z Bronze (opakovaný běh = stejný výsledek).
# 
# Společné kroky pro všechny tabulky:
# 1. **Názvy sloupců** – malá písmena a podtržítka (`PERSON_SK` → `person_sk`).
# 2. **Texty** – oříznuté mezery, vícenásobné mezery → jedna, prázdný text → `NULL`.
# 3. **Duplicity** – úplně stejné řádky se odstraní; pokud má jeden klíč víc řádků, zůstane ten s nejnovějším `created_at`.
# 4. **Typy** – data jako `date`, časy jako `timestamp`, částky `decimal(10,2)`, příznaky `Y`/`N` → `boolean`, datum + čas → jeden sloupec `*_at`.
# 5. **Kvalita dat** – každé pravidlo má závažnost:
#    * **WARN** – řádek zůstává, kód pravidla je ve sloupci `dq_flags`,
#    * **ERROR** – nesmyslný řádek jde do `silver_dq_quarantine` a v Silver tabulce není.
# 6. **Osobní údaje** – jména, adresy, data narození, čísla dokladů, e-maily a volné texty se do Silver nepřenášejí.
# 
# Výstupy: `silver_<tabulka>`, `silver_dq_quarantine`, `ops_dq_result` (počty porušení pravidel).


# CELL ********************

import re
from pyspark.sql import functions as F
from pyspark.sql.window import Window

summary, dq_results, quarantines = [], [], []


def snake(name: str) -> str:
    """PERSON_SK -> person_sk, _loaded_at -> _loaded_at."""
    prefix = "_" if name.startswith("_") else ""
    return prefix + re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def read_bronze(table: str):
    """Bronze tabulka se snake_case sloupci a vyčištěnými texty."""
    df = spark.table(f"bronze_{table}")
    df = df.toDF(*[snake(c) for c in df.columns])
    for column, dtype in df.dtypes:
        if dtype == "string":
            cleaned = F.regexp_replace(F.trim(F.col(column)), r"\s+", " ")
            df = df.withColumn(column, F.when(cleaned == "", None).otherwise(cleaned))
    return df


def dedup(table: str, df, key: str):
    """Odstraní úplné duplicity a pak ponechá 1 řádek na klíč (nejnovější created_at)."""
    rows_bronze = df.count()
    data_columns = [c for c in df.columns if c != "_loaded_at"]
    df = df.dropDuplicates(data_columns)
    rows_exact = df.count()
    order_col = "created_at" if "created_at" in df.columns else "_loaded_at"  # REVIZOR nemá created_at
    window = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    df = df.withColumn("__rn", F.row_number().over(window)).filter("__rn = 1").drop("__rn")
    rows_unique = df.count()
    summary.append({"table": table, "bronze": rows_bronze, "exact_duplicates": rows_bronze - rows_exact,
                    "key_duplicates": rows_exact - rows_unique})
    return df


def yes_no(column: str):
    return F.when(F.upper(column) == "Y", True).when(F.upper(column) == "N", False)


def combine_date_time(date_col: str, time_col: str):
    """Datum + čas (zdroj má čas jako 1970-01-01 HH:MM:SS) → jeden timestamp."""
    return F.when(F.col(date_col).isNotNull() & F.col(time_col).isNotNull(),
                  F.to_timestamp(F.concat_ws(" ", F.col(date_col).cast("string"),
                                             F.date_format(time_col, "HH:mm:ss"))))


# Druh dopravy je ve zdroji ve dvou číselnících (Tramvaj / TRAM …) → jeden kód
TRANSPORT_MODES = {
    "METRO": ["metro"],
    "TRAM": ["tramvaj", "tram"],
    "BUS": ["autobus", "bus"],
    "TROLLEYBUS": ["trolejbus", "trolleybus"],
    "TRAIN": ["vlak", "train"],
    "FUNICULAR": ["lanovka", "funicular"],
    "FERRY": ["přívoz", "privoz", "ferry"],
}


def unify_transport_mode(column: str):
    expr = None
    for code_value, variants in TRANSPORT_MODES.items():
        condition = F.lower(F.col(column)).isin(variants)
        expr = F.when(condition, code_value) if expr is None else expr.when(condition, code_value)
    return expr  # neznámá hodnota → NULL (a pravidlo *_UNKNOWN_TRANSPORT_MODE)


def lookup(df, column: str, ref_df, ref_column: str, alias: str, extra: dict | None = None):
    """Left join na referenční tabulku: přidá __<alias>_exists (+ volitelné sloupce)."""
    selected = [F.col(ref_column).alias(f"__{alias}_key")]
    selected += [F.col(src).alias(dst) for dst, src in (extra or {}).items()]
    ref = ref_df.select(*selected).dropDuplicates([f"__{alias}_key"])
    df = df.join(ref, df[column] == ref[f"__{alias}_key"], "left")
    return df.withColumn(f"__{alias}_exists", F.col(f"__{alias}_key").isNotNull()).drop(f"__{alias}_key")


def finish(table: str, df, key: str, keep: list[str], warn: dict | None = None, error: dict | None = None):
    """Vyhodnotí DQ pravidla, zapíše silver_<table>, ERROR řádky odloží do karantény."""
    warn, error = warn or {}, error or {}
    rules = {**{r: (c, "WARN") for r, c in warn.items()}, **{r: (c, "ERROR") for r, c in error.items()}}
    for rule, (condition, _) in rules.items():
        df = df.withColumn(f"__{rule}", F.coalesce(condition, F.lit(False)))

    def flags(names):
        items = [F.when(F.col(f"__{r}"), F.lit(r)) for r in names] or [F.lit(None).cast("string")]
        return F.filter(F.array(*items), lambda x: x.isNotNull())

    df = df.withColumn("dq_flags", flags(list(warn))).withColumn("__errors", flags(list(error))).cache()
    counts = df.select(*[F.sum(F.col(f"__{r}").cast("int")).alias(r) for r in rules]).first() if rules else {}

    silver = df.filter(F.size("__errors") == 0).select(*keep, "dq_flags")
    silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"silver_{table}")

    quarantine = df.filter(F.size("__errors") > 0).select(
        F.lit(table).alias("table_name"), F.col(key).cast("string").alias("record_key"),
        F.col("__errors").alias("dq_errors"), F.to_json(F.struct(*keep)).alias("record"))
    quarantines.append(quarantine)

    rows_silver = spark.table(f"silver_{table}").count()
    entry = next(s for s in summary if s["table"] == table)
    entry.update({"silver": rows_silver, "quarantine": quarantine.count()})
    dq_results.extend((table, r, sev, int(counts[r] or 0)) for r, (_, sev) in rules.items())
    df.unpersist()
    print(f"silver_{table:<28} {rows_silver:>7} řádků")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Načtení a deduplikace všech tabulek

# CELL ********************

person = dedup("cestujici", read_bronze("cestujici"), "person_sk")
inspector = dedup("revizor", read_bronze("revizor"), "inspector_sk")
inspection = dedup("prepravni_kontrola", read_bronze("prepravni_kontrola"), "inspection_case_sk")
receivable = dedup("pohledavka", read_bronze("pohledavka"), "receivable_sk")
payment = dedup("platba", read_bronze("platba"), "payment_sk")
collection = dedup("collection_vymahani", read_bronze("collection_vymahani"), "collection_action_sk")
claim = dedup("claim_reklamace_pokuty", read_bronze("claim_reklamace_pokuty"), "claim_sk")
refund = dedup("refund", read_bronze("refund"), "refund_sk")
submission = dedup("podnet_cestujiciho", read_bronze("podnet_cestujiciho"), "submission_sk")

# Odvozené sloupce, které potřebují i jiné tabulky
inspection = (inspection
              .withColumnRenamed("person_id", "person_sk")
              .withColumn("inspection_at", combine_date_time("inspection_date", "inspection_time")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_cestujici
# Osobní údaje (jméno, datum narození, adresa, CRM ID, číslo dokladu) se nepřenášejí. Zůstává klíč `person_sk`,
# státní příslušnost, typ dokladu, obec bydliště (z adresy) a rok narození (NULL, pokud je datum nepravděpodobné).

# CELL ********************

birth_implausible = (F.col("person_birthdate") < F.lit("1920-01-01").cast("date")) | \
                    (F.col("person_birthdate") > F.to_date("_loaded_at"))
df = (person
      .withColumn("nationality", F.col("person_registration"))
      .withColumn("residence_city", F.nullif(F.regexp_extract("person_address", r"\d{3} ?\d{2} (.+)$", 1), F.lit("")))
      .withColumn("birth_year", F.when(~birth_implausible, F.year("person_birthdate"))))

finish("cestujici", df, "person_sk",
       keep=["person_sk", "nationality", "document_type", "residence_city", "birth_year", "created_at", "_loaded_at"],
       warn={
           "P01_NAME_MISSING": F.col("person_full_name").isNull() | F.col("person_firstname").isNull()
                               | F.col("person_surname").isNull(),
           "P02_BIRTHDATE_IMPLAUSIBLE": birth_implausible,
           "P03_BIRTHDATE_MISSING": F.col("person_birthdate").isNull(),
           "P04_NATIONALITY_MISSING": F.col("person_registration").isNull(),
           "P05_DOCUMENT_MISSING": F.col("document_type").isNull(),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_revizor
# Osobní číslo zaměstnance se nepřenáší. `valid_to = NULL` znamená, že revizor je stále platný.

# CELL ********************

df = inspector.withColumn("is_current", yes_no("is_current"))

finish("revizor", df, "inspector_sk",
       keep=["inspector_sk", "badge_no", "valid_from", "valid_to", "organisational_unit", "employment_status",
             "is_current", "_loaded_at"],
       warn={
           "R01_VALID_TO_BEFORE_FROM": F.col("valid_to") < F.col("valid_from"),
           "R02_CURRENT_INCONSISTENT": (F.col("is_current") & F.col("valid_to").isNotNull())
                                       | (~F.col("is_current") & F.col("valid_to").isNull()),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_prepravni_kontrola
# Datum + čas kontroly → `inspection_at`, `inspection_hour`. Druh dopravy sjednocený (`transport_mode`), původní
# hodnota v `transport_mode_src`. `person_id` přejmenováno na `person_sk` (stejný klíč jako v `silver_cestujici`).

# CELL ********************

df = (inspection
      .withColumn("inspection_hour", F.hour("inspection_at"))
      .withColumn("transport_mode_src", F.col("transport_mode"))
      .withColumn("transport_mode", unify_transport_mode("transport_mode_src"))
      .withColumn("identification_provided", yes_no("identification_provided"))
      .withColumn("police_assistance", yes_no("police_assistance"))
      .withColumn("paid_on_site", yes_no("paid_on_site")))
df = lookup(df, "inspector_sk", inspector, "inspector_sk", "insp",
            {"__insp_from": "valid_from", "__insp_to": "valid_to"})
df = lookup(df, "person_sk", person, "person_sk", "person")

finish("prepravni_kontrola", df, "inspection_case_sk",
       keep=["inspection_case_sk", "case_id", "zoppk_number", "inspection_date", "inspection_at", "inspection_hour",
             "inspector_sk", "person_sk", "transport_mode", "transport_mode_src", "line_id", "station_stop_name",
             "identification_provided", "police_assistance", "paid_on_site", "case_status", "created_at",
             "_loaded_at"],
       warn={
           "K01_UNKNOWN_INSPECTOR": F.col("inspector_sk").isNotNull() & ~F.col("__insp_exists"),
           "K02_INSPECTOR_NOT_VALID": F.col("__insp_exists") & (
               (F.col("inspection_date") < F.col("__insp_from"))
               | (F.col("__insp_to").isNotNull() & (F.col("inspection_date") > F.col("__insp_to")))),
           "K03_UNKNOWN_PERSON": F.col("person_sk").isNotNull() & ~F.col("__person_exists"),
           "K04_UNKNOWN_TRANSPORT_MODE": F.col("transport_mode_src").isNotNull() & F.col("transport_mode").isNull(),
           "K05_ZOPPK_INCONSISTENT": (F.col("paid_on_site") & F.col("zoppk_number").isNotNull())
                                     | (~F.col("paid_on_site") & F.col("zoppk_number").isNull()),
           "K06_CREATED_BEFORE_INSPECTION": F.col("created_at") < F.col("inspection_at"),
           "K07_INSPECTION_AT_MISSING": F.col("inspection_at").isNull(),
       },
       error={
           "K90_INSPECTION_IN_FUTURE": F.col("inspection_at") > F.col("_loaded_at"),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_pohledavka
# Doplněno `paid_success_amount` = součet úspěšných plateb.

# CELL ********************

paid = (payment.filter(F.col("payment_status") == "SUCCESS")
        .groupBy("receivable_sk").agg(F.sum("payment_amount").alias("__paid")))
df = (receivable.join(paid, "receivable_sk", "left")
      .withColumn("paid_success_amount", F.coalesce("__paid", F.lit(0)).cast("decimal(12,2)")))
df = lookup(df, "inspection_case_sk", inspection, "inspection_case_sk", "case")
amounts = ["surcharge_original", "surcharge_due", "outstanding_amount"]

finish("pohledavka", df, "receivable_sk",
       keep=["receivable_sk", "receivable_id", "inspection_case_sk", "surcharge_original", "surcharge_due",
             "outstanding_amount", "paid_success_amount", "issue_date", "discount_deadline", "due_date", "status",
             "created_at", "_loaded_at"],
       warn={
           "H01_UNKNOWN_INSPECTION": F.col("inspection_case_sk").isNotNull() & ~F.col("__case_exists"),
           "H02_DATES_ORDER": (F.col("discount_deadline") < F.col("issue_date"))
                              | (F.col("due_date") < F.col("issue_date")),
           "H03_DUE_GT_ORIGINAL": F.col("surcharge_due") > F.col("surcharge_original"),
           "H04_OUTSTANDING_GT_DUE": F.col("outstanding_amount") > F.col("surcharge_due"),
           "H05_PAID_BUT_OUTSTANDING": (F.col("status") == "PAID") & (F.col("outstanding_amount") != 0),
           "H06_PAID_BUT_UNDERPAID": (F.col("status") == "PAID")
                                     & (F.col("paid_success_amount") < F.col("surcharge_due")),
       },
       error={
           "H90_NEGATIVE_AMOUNT": F.greatest(*[(F.col(a) < 0) for a in amounts]),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_platba

# CELL ********************

df = lookup(payment, "receivable_sk", receivable, "receivable_sk", "rec")

finish("platba", df, "payment_sk",
       keep=["payment_sk", "payment_id", "receivable_sk", "payment_timestamp", "payment_amount", "payment_method",
             "payment_channel", "variable_symbol", "payment_status", "transaction_reference", "created_at",
             "_loaded_at"],
       warn={
           "M01_UNKNOWN_RECEIVABLE": F.col("receivable_sk").isNotNull() & ~F.col("__rec_exists"),
           "M02_CREATED_BEFORE_PAYMENT": F.col("created_at") < F.col("payment_timestamp"),
       },
       error={
           "M90_PAYMENT_IN_FUTURE": (F.col("payment_timestamp") > F.col("_loaded_at"))
                                    | (F.col("created_at") > F.col("_loaded_at")),
           "M91_NON_POSITIVE_AMOUNT": F.col("payment_amount") <= 0,
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_collection_vymahani
# Doplněno pořadí kroku `action_step` (1 upomínka → 2 předání → 3 soud → 4 exekuce).

# CELL ********************

df = (collection
      .withColumn("action_step", F.when(F.col("action_type") == "REMINDER", 1)
                                  .when(F.col("action_type") == "LEGAL_HANDOVER", 2)
                                  .when(F.col("action_type") == "COURT_PROCEEDING", 3)
                                  .when(F.col("action_type") == "ENFORCEMENT", 4)))
df = lookup(df, "receivable_sk", receivable, "receivable_sk", "rec")

finish("collection_vymahani", df, "collection_action_sk",
       keep=["collection_action_sk", "collection_action_id", "receivable_sk", "action_type", "action_step",
             "action_date", "amount_at_action", "additional_cost", "external_party_id", "reference_no", "status",
             "created_at", "_loaded_at"],
       warn={
           "C01_UNKNOWN_RECEIVABLE": F.col("receivable_sk").isNotNull() & ~F.col("__rec_exists"),
           "C02_UNKNOWN_ACTION_TYPE": F.col("action_type").isNotNull() & F.col("action_step").isNull(),
           "C03_NEGATIVE_COST": F.col("additional_cost") < 0,
       },
       error={
           "C90_ACTION_IN_FUTURE": F.col("action_date") > F.to_date("_loaded_at"),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_claim_reklamace_pokuty
# Časy s artefaktem `.999` ms se zaokrouhlí dolů na celé sekundy. Doplněna doba vyřízení `resolution_days`.

# CELL ********************

df = claim
for column in ["submitted_at", "resolved_at", "created_at"]:
    df = df.withColumn(column, F.date_trunc("second", column))
df = df.withColumn("resolution_days", F.datediff("resolved_at", "submitted_at"))
df = lookup(df, "inspection_case_sk", inspection, "inspection_case_sk", "case", {"__case_at": "inspection_at"})

finish("claim_reklamace_pokuty", df, "claim_sk",
       keep=["claim_sk", "claim_id", "inspection_case_sk", "submitted_at", "submission_channel", "claim_reason",
             "claim_status", "resolved_at", "resolution", "resolution_days", "surcharge_before", "surcharge_after",
             "refund_amount", "created_at", "_loaded_at"],
       warn={
           "L01_UNKNOWN_INSPECTION": F.col("inspection_case_sk").isNotNull() & ~F.col("__case_exists"),
           "L02_SUBMITTED_BEFORE_INSPECTION": F.col("submitted_at") < F.col("__case_at"),
           "L03_STATUS_INCONSISTENT": ((F.col("claim_status") == "RESOLVED") & F.col("resolved_at").isNull())
                                      | (F.col("claim_status").isin("OPEN", "IN_REVIEW")
                                         & F.col("resolved_at").isNotNull()),
           "L04_SURCHARGE_INCREASED": F.col("surcharge_after") > F.col("surcharge_before"),
       },
       error={
           "L90_RESOLVED_IN_FUTURE": F.col("resolved_at") > F.col("_loaded_at"),
           "L91_RESOLVED_BEFORE_SUBMITTED": F.col("resolved_at") < F.col("submitted_at"),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_refund

# CELL ********************

df = lookup(refund, "claim_sk", claim, "claim_sk", "claim")
df = lookup(df, "payment_sk", payment, "payment_sk", "pay", {"__pay_amount": "payment_amount"})

finish("refund", df, "refund_sk",
       keep=["refund_sk", "refund_id", "claim_sk", "payment_sk", "refund_amount", "refund_method", "refund_status",
             "refund_timestamp", "transaction_reference", "created_at", "_loaded_at"],
       warn={
           "F01_UNKNOWN_CLAIM": F.col("claim_sk").isNotNull() & ~F.col("__claim_exists"),
           "F02_UNKNOWN_PAYMENT": F.col("payment_sk").isNotNull() & ~F.col("__pay_exists"),
           "F03_REFUND_GT_PAYMENT": F.col("refund_amount") > F.col("__pay_amount"),
           "F04_REFUNDED_BEFORE_REQUEST": F.col("refund_timestamp") < F.col("created_at"),
       },
       error={
           "F90_REFUND_IN_FUTURE": F.col("refund_timestamp") > F.col("_loaded_at"),
           "F91_NON_POSITIVE_AMOUNT": F.col("refund_amount") <= 0,
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## silver_podnet_cestujiciho
# Jméno, příjmení, e-mail a text podnětu se nepřenášejí. Datum + čas incidentu → `incident_at`.
# Druh dopravy sjednocený stejně jako u kontrol. `line_id` je normalizované číslo linky, `line_reported` / `stop_reported`
# jsou původní texty od cestujícího.

# CELL ********************

df = (submission
      .withColumn("incident_at", combine_date_time("incident_date", "incident_time"))
      .withColumn("transport_mode_src", F.col("transport_mode"))
      .withColumn("transport_mode", unify_transport_mode("transport_mode_src")))

finish("podnet_cestujiciho", df, "submission_sk",
       keep=["submission_sk", "submission_id", "submission_type", "submitted_at", "submission_channel",
             "incident_date", "incident_at", "transport_mode", "transport_mode_src", "line_reported", "line_id",
             "stop_reported", "stop_id", "stop_match_status", "category", "subcategory", "status", "resolved_at",
             "resolution_type", "created_at", "_loaded_at"],
       warn={
           "S01_INCIDENT_AFTER_SUBMISSION": F.col("incident_at") > F.col("submitted_at"),
           "S02_UNKNOWN_TRANSPORT_MODE": F.col("transport_mode_src").isNotNull() & F.col("transport_mode").isNull(),
           "S03_LINE_NOT_NORMALIZED": F.col("line_reported") != F.col("line_id"),
           "S04_STOP_NOT_MATCHED": F.col("stop_id").isNull(),
       },
       error={
           "S90_RESOLVED_IN_FUTURE": F.col("resolved_at") > F.col("_loaded_at"),
           "S91_RESOLVED_BEFORE_SUBMITTED": F.col("resolved_at") < F.col("submitted_at"),
       })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Karanténa a výsledky pravidel

# CELL ********************

quarantine_all = quarantines[0]
for q in quarantines[1:]:
    quarantine_all = quarantine_all.unionByName(q)
(quarantine_all.withColumn("quarantined_at", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_dq_quarantine"))

run_at = spark.sql("SELECT current_timestamp() AS t").first()["t"]
(spark.createDataFrame(dq_results, "table_name string, rule_id string, severity string, violations long")
 .withColumn("checked_at", F.lit(run_at))
 .write.mode("append").saveAsTable("ops_dq_result"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Přehled pro README
print("| Tabulka | Bronze | Úplné duplicity | Duplicitní klíč | Karanténa | Silver |")
print("| --- | ---: | ---: | ---: | ---: | ---: |")
for s in summary:
    print(f"| `silver_{s['table']}` | {s['bronze']} | {s['exact_duplicates']} | {s['key_duplicates']} | "
          f"{s['quarantine']} | {s['silver']} |")

print("\n| Tabulka | Pravidlo | Závažnost | Porušení |")
print("| --- | --- | --- | ---: |")
for table, rule, severity, violations in dq_results:
    print(f"| {table} | {rule} | {severity} | {violations} |")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
