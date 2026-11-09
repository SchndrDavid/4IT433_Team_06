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

# # nb_bronze_check – kontrola počtů řádků zdroj vs. Bronze
# 
# Pro každou tabulku spočítá řádky ve zdroji `SQL_4IT433` a v `bronze_<tabulka>`.
# Výsledek zobrazí, uloží do tabulky `ops_bronze_count_check` a při neshodě notebook skončí chybou.
# Tabulku z výstupu přepiš do README.

# PARAMETERS CELL ********************

# Parametry – sdílená zdrojová DB SQL_4IT433 (ověř v SQL_4IT433 → Settings → Connection strings)
server = "wosfckz7iqdejdfedecwu6neiq-rn6ftjrgxlbufhaldgmeqjfm2q.database.fabric.microsoft.com"
database = "SQL_4IT433-efca03d8-ca80-4f51-83bd-7b3baed3c015"

tables = ["CESTUJICI", "REVIZOR", "PREPRAVNI_KONTROLA", "POHLEDAVKA", "PLATBA",
          "COLLECTION_VYMAHANI", "CLAIM_REKLAMACE_POKUTY", "REFUND", "PODNET_CESTUJICIHO"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Přihlášení tokenem identity, která notebook spouští – žádné heslo v kódu
token = notebookutils.credentials.getToken("https://database.windows.net/")
jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;trustServerCertificate=false"


def read_source(table: str):
    """Načte celou tabulku dbo.<table> ze zdrojové databáze SQL_4IT433."""
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"dbo.{table}")
            .option("accessToken", token)
            .load())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = []
for table in tables:
    source_count = read_source(table).count()
    bronze_count = spark.table(f"bronze_{table.lower()}").count()
    rows.append((table, f"bronze_{table.lower()}", source_count, bronze_count, source_count == bronze_count))

result = (spark.createDataFrame(rows, "zdrojova_tabulka string, bronze_tabulka string, pocet_zdroj long, "
                                      "pocet_bronze long, shoda boolean")
          .withColumn("checked_at", F.current_timestamp()))
result.write.mode("append").saveAsTable("ops_bronze_count_check")
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Výpis jako markdown tabulka – rovnou zkopírovat do README
print("| Tabulka | Počet ve zdroji | Počet v Bronze | Shoda |")
print("| --- | ---: | ---: | :---: |")
for t, b, s, c, ok in rows:
    print(f"| {t} → `{b}` | {s} | {c} | {'✅' if ok else '❌'} |")

mismatch = [t for t, _, s, c, ok in rows if not ok]
if mismatch:
    raise RuntimeError(f"Počty nesedí: {mismatch}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
