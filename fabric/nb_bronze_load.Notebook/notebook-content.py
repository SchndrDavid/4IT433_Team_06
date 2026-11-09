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

# # nb_bronze_load – SQL_4IT433 → Bronze
# 
# Načte všechny tabulky ze zdrojové databáze `SQL_4IT433` (schéma `dbo`) do lakehouse jako `bronze_<tabulka>`.
# 
# * **Bronze = přesná kopie zdroje**: názvy sloupců, typy i hodnoty beze změny, navíc jen sloupec `_loaded_at` (čas načtení, UTC).
# * **Bez duplicit při opakovaném běhu**: každá tabulka se zapisuje v režimu `overwrite`, tj. při dalším spuštění se celá nahradí aktuálním stavem zdroje.
# * Default lakehouse notebooku: `lh_team_06`.

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

load_ts = F.current_timestamp()  # stejný okamžik pro celý běh

for table in tables:
    df = read_source(table).withColumn("_loaded_at", load_ts)
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"bronze_{table.lower()}"))
    print(f"{table:<25} -> bronze_{table.lower()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
