# etl_load_movies.py
import os
import io
import sys
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2 import sql
import pandas.api.types as ptypes

# =======================
# Rutas de salida (auto-Escritorio o ENV)
# =======================
def default_output_paths():
    home = Path.home()
    candidates = [
        home / "OneDrive" / "Escritorio" / "main",
        home / "Desktop"  / "main",
        home / "Escritorio" / "main",
    ]
    for c in candidates:
        try:
            c.mkdir(parents=True, exist_ok=True)
            return (c / "FilmTV_USAMoviesClean.csv", c / "FilmTV_USAMoviesClean_DEMO.csv")
        except Exception:
            continue
    here = Path.cwd()
    return (here / "FilmTV_USAMoviesClean.csv", here / "FilmTV_USAMoviesClean_DEMO.csv")

# =======================
# Config
# =======================
INPUT_CSV  = os.getenv("INPUT_CSV", "movie_metadata.csv")

_out_main, _out_demo = default_output_paths()
OUTPUT_CSV = Path(os.getenv("OUTPUT_CSV", str(_out_main)))
OUTPUT_CSV_DEMO = Path(os.getenv("OUTPUT_CSV_DEMO", str(_out_demo)))

PGHOST     = os.getenv("PGHOST", "localhost")
PGPORT     = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "etl")
PGUSER     = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "ELENA1206")  # ideal: usa variable de entorno
SCHEMA     = os.getenv("PGSCHEMA", "public")
TABLE_NAME = os.getenv("PGTABLE", "film_tv_usa_movies_clean")

# =======================
# Utilidades
# =======================
def pg_type(dtype) -> str:
    if ptypes.is_integer_dtype(dtype): return "BIGINT"
    if ptypes.is_float_dtype(dtype):   return "DOUBLE PRECISION"
    if ptypes.is_bool_dtype(dtype):    return "BOOLEAN"
    return "TEXT"

def assert_or_fail(cond: bool, msg: str):
    if not cond:
        print("❌ VALIDACIÓN FALLÓ:", msg)
        sys.exit(1)

# =======================
# 1) Extract & Transform (puntos 1–6)
# =======================
print(f"📥 Leyendo CSV de entrada: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)

required = {"gross", "facenumber_in_poster", "movie_imdb_link", "title_year", "country"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Faltan columnas requeridas en el CSV: {sorted(missing)}")

orig_rows = len(df)

# (1) gross -> rellenar con promedio (imputación)
gross_orig = pd.to_numeric(df["gross"], errors="coerce")
gross_mean = gross_orig.mean(skipna=True)
df["gross"] = gross_orig.fillna(gross_mean)

# (2) facenumber_in_poster -> TODAS las filas en 0 (como pediste)
faces_orig = pd.to_numeric(df["facenumber_in_poster"], errors="coerce")
df["facenumber_in_poster"] = 0  # forzado a 0 en todo el dataset
df["facenumber_in_poster"] = df["facenumber_in_poster"].astype(int)

# (3) TittleCode desde movie_imdb_link (robusto: tt + 7–8 dígitos)
# Ej.: http://www.imdb.com/title/tt0499549/?ref_=fn_tt_tt_1 -> tt0499549
imdb = df["movie_imdb_link"].astype("string")
df["TittleCode"] = imdb.str.extract(r"/title/(tt\d{7,8})", expand=False)
df.loc[df["TittleCode"].isna(), "TittleCode"] = imdb.str.extract(r"(tt\d{7,8})", expand=False)

# (4) title_year -> TODAS las filas en 0 (como pediste)
df["title_year"] = 0
df["title_year"] = df["title_year"].astype(int)

# (5) solo USA (tolerante a espacios/mayúsculas)
df["country"] = df["country"].astype("string").str.strip()
mask_usa = df["country"].str.upper().eq("USA")
kept = int(mask_usa.sum())
df = df.loc[mask_usa].copy()

# --- Diagnósticos útiles ---
# Regla #2 (faces forzado)
fnp = df["facenumber_in_poster"]
n_nan   = faces_orig.loc[df.index].isna().sum()
n_neg   = (faces_orig.loc[df.index] < 0).sum()
n_zero0 = (faces_orig.loc[df.index] == 0).sum()
print(f"Regla#2 facenumber_in_poster → FORZADO_A_0 | [ANTES] NaN={n_nan} neg={n_neg} zeros_orig={n_zero0} | [DESPUÉS] NaN={fnp.isna().sum()} min={fnp.min()} max={fnp.max()} zeros={(fnp==0).sum()}")

# Regla #4 (title_year forzado)
ty_after = pd.to_numeric(df["title_year"], errors="coerce")
print(f"Regla#4 title_year → FORZADO_A_0 | [DESPUÉS] NaN={ty_after.isna().sum()} min={ty_after.min()} max={ty_after.max()} zeros={(ty_after==0).sum()}")

# (6) guardar CSV limpio (todas las columnas + TittleCode al final)
cols_no_tcode = [c for c in df.columns if c != "TittleCode"]
ordered_cols = cols_no_tcode + ["TittleCode"]
df = df[ordered_cols].copy()

OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"💾 CSV limpio guardado en: {OUTPUT_CSV.resolve()} (filas originales={orig_rows}, USA={kept})")

# — Comprobación rápida de encabezado (debe contener TittleCode)
with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
    header = f.readline().strip()
print("Encabezado CSV:", header)
assert_or_fail("TittleCode" in header.split(","), "El CSV exportado no contiene la columna TittleCode")

# =======================
# CSV DEMO (evidencia de reglas/llenados)
# =======================
demo = pd.DataFrame({
    "gross": df["gross"],
    "gross_imputed_mean": (gross_orig.loc[df.index].isna()).astype(int),  # 1 si se imputó con promedio
    "facenumber_in_poster": df["facenumber_in_poster"],
    "TittleCode": df["TittleCode"],
    "title_year": df["title_year"],
    "country": df["country"]
})
OUTPUT_CSV_DEMO.parent.mkdir(parents=True, exist_ok=True)
demo.to_csv(OUTPUT_CSV_DEMO, index=False, encoding="utf-8")
print(f"🧾 CSV DEMO (evidencia) guardado en: {OUTPUT_CSV_DEMO.resolve()}")


# =======================
# 2) Validaciones (ajustadas a tu requerimiento)
# =======================
print("🔎 Validando reglas del ETL...")
# 1: gross sin NaN
assert_or_fail(not pd.to_numeric(df["gross"], errors="coerce").isna().any(), "gross aún tiene NaN")
# 2: facenumber_in_poster = 0 en TODAS las filas
faces2 = pd.to_numeric(df["facenumber_in_poster"], errors="coerce")
assert_or_fail(not faces2.isna().any(), "facenumber_in_poster con NaN")
assert_or_fail((faces2 == 0).all(), "facenumber_in_poster no está en 0 para todas las filas")
# 3: TittleCode existe; cualquier valor NO nulo cumple regex
assert_or_fail("TittleCode" in df.columns, "no existe columna TittleCode")
mask_notnull_tcode = df["TittleCode"].notna()
assert_or_fail(df.loc[mask_notnull_tcode, "TittleCode"].astype(str).str.match(r"^tt\d{7,8}$").all(),
                "Hay valores de TittleCode con formato inválido (esperado tt########)")
# 4: title_year = 0 en TODAS las filas
years = pd.to_numeric(df["title_year"], errors="coerce")
assert_or_fail(not years.isna().any(), "title_year con NaN")
assert_or_fail((years == 0).all(), "title_year no está en 0 para todas las filas")
# 5: sólo USA
assert_or_fail(set(df["country"].str.upper().unique()) == {"USA"}, "country contiene países distintos de USA")
print("✅ Todas las reglas y tus requerimientos (facenumber_in_poster=0 y title_year=0 en toda la columna) se cumplieron.")

# =======================
# 3) (Opcional) Load a PostgreSQL
#     Activa/desactiva con:  set LOAD_TO_PG=1  (Windows) / export LOAD_TO_PG=1 (Linux/Mac)
# =======================
if os.getenv("LOAD_TO_PG", "1") == "1":
    print(f"🐘 Conectando a PostgreSQL: {PGUSER}@{PGHOST}:{PGPORT}/{PGDATABASE}")
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # DDL con CHECKs que refuerzan el requerimiento (ambas columnas = 0)
    cols_ddl = ",\n  ".join(f"\"{c}\" {pg_type(df[c].dtype)}" for c in df.columns)
    checks = [
        "CHECK (facenumber_in_poster = 0)",
        "CHECK (title_year = 0)",
        "CHECK (\"TittleCode\" IS NULL OR \"TittleCode\" ~ '^tt[0-9]{7,8}$')"
    ]
    ddl = f'''
    CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_NAME}" (
        {cols_ddl},
        {", ".join(checks)}
    );
    TRUNCATE TABLE "{SCHEMA}"."{TABLE_NAME}";
    '''
    cur.execute(ddl)

    # COPY FROM STDIN respetando el mismo orden de columnas
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    copy_sql = sql.SQL("""
        COPY {}.{} ({})
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
    """).format(
        sql.Identifier(SCHEMA),
        sql.Identifier(TABLE_NAME),
        sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    )
    cur.copy_expert(copy_sql.as_string(conn), buf)

    # Índices
    cur.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {} ON {}.{} ("TittleCode");')
        .format(sql.Identifier(f"ix_{TABLE_NAME}_tittlecode"),
                sql.Identifier(SCHEMA), sql.Identifier(TABLE_NAME)))
    cur.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {} ON {}.{} ("country");')
        .format(sql.Identifier(f"ix_{TABLE_NAME}_country"),
                sql.Identifier(SCHEMA), sql.Identifier(TABLE_NAME)))

    cur.close()
    conn.close()
    print(f"🎯 Carga completada OK: {len(df)} filas en {SCHEMA}.{TABLE_NAME}")

print(f"ℹ️ Promedio usado para gross: {gross_mean:,.2f}")  
