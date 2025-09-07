# app.py  —  Streamlit NTR Réconciliator
# pip install streamlit duckdb pyarrow pandas xlsxwriter openpyxl
# streamlit run app.py

import os, re, io, tempfile, unicodedata
import pandas as pd
import duckdb
import streamlit as st

# -------------------- CONFIG PAR DEFAUT --------------------
DEFAULT_BASE = r"C:\Users\HADJADJI\Desktop\Automatic_Reonciliation_System"

def cfg_from_base(base_dir: str):
    base = base_dir
    return {
        "BASE": base,
        "AXIS_OUT_DIR": os.path.join(base, "data", "axis", "output"),
        "RED_IN_DIR":   os.path.join(base, "data", "red",  "input"),
        "RED_OUT_DIR":  os.path.join(base, "data", "red",  "output"),
        "OUT_DIR":      os.path.join(base, "output"),
        "AN_DIR":       os.path.join(base, "analytics"),
    }

# -------------------- HELPERS --------------------
def _normalize_text_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    return s.apply(lambda x: unicodedata.normalize("NFKD", x).encode("ascii","ignore").decode("ascii"))

def normalize_join_key(s: pd.Series) -> pd.Series:
    return _normalize_text_series(s).str.upper().str.strip()

def normalize_join_key_nospace(s: pd.Series) -> pd.Series:
    return normalize_join_key(s).str.replace(r"[^A-Z0-9]", "", regex=True)

def parse_amount(series: pd.Series) -> pd.Series:
    def _c(x):
        if pd.isna(x): return None
        x = str(x).replace("\u00a0"," ").replace(" ","").strip()
        if "," in x and "." not in x: x = x.replace(",",".")
        x = re.sub(r"[^0-9\.\-]","", x)
        return x or None
    return pd.to_numeric(series.map(_c), errors="coerce")

def parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def qident(col: str) -> str:
    return '"' + str(col).replace('"','""') + '"'

def fmt_csv(df: pd.DataFrame) -> bytes:
    # 2 décimales partout pour float
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8", float_format="%.2f")
    return ("\ufeff" + buf.getvalue()).encode("utf-8")  # UTF-8 BOM

def write_xlsx_multi(sheets: list[tuple[str,pd.DataFrame]]) -> bytes:
    bio = io.BytesIO()
    engine = "xlsxwriter"
    try:
        with pd.ExcelWriter(bio, engine=engine) as xw:
            for name, df in sheets:
                # round 2 decimals for float
                df2 = df.copy()
                for c in df2.columns:
                    if pd.api.types.is_float_dtype(df2[c].dtype):
                        df2[c] = df2[c].round(2)
                df2.to_excel(xw, sheet_name=name[:31], index=False)
                wb = xw.book
                ws = xw.sheets[name[:31]]
                fmt2 = wb.add_format({'num_format': '#,##0.00'})
                # apply to float columns
                for j, col in enumerate(df2.columns):
                    if pd.api.types.is_float_dtype(df2[col].dtype):
                        ws.set_column(j, j, None, fmt2)
    except Exception:
        # Fallback openpyxl sans format
        with pd.ExcelWriter(bio, engine="openpyxl") as xw:
            for name, df in sheets:
                df.to_excel(xw, sheet_name=name[:31], index=False)
    bio.seek(0)
    return bio.read()

# -------------------- TRAITEMENT RED (CSV/Parquet -> clean Parquet+CSV) --------------------
def process_red_files_to_output(input_paths: list[str], red_out_dir: str) -> tuple[str,str]:
    os.makedirs(red_out_dir, exist_ok=True)
    dfs = []
    for path in input_paths:
        try:
            if path.lower().endswith(".parquet"):
                df = pd.read_parquet(path)
            else:
                # lecture robuste CSV
                for enc in ("utf-8-sig","utf-8","latin1","cp1252"):
                    try:
                        df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding=enc)
                        break
                    except Exception:
                        df = None
                if df is None:
                    st.warning(f"Lecture impossible: {path}")
                    continue
            df.dropna(how="all", inplace=True)
            df = df.loc[:, ~df.columns.isna()]
            df.columns = df.columns.astype(str).str.strip()
            # map colonnes clés
            ref_col = next((c for c in df.columns if c.lower() in
                            {"votre référence","votre reference","reference","référence","ref","orderid","id_commande2"}), None)
            if ref_col: df.rename(columns={ref_col:"Votre référence"}, inplace=True)
            if "Votre référence" not in df.columns:
                st.warning(f"'Votre référence' manquante: {os.path.basename(path)} — ignoré.")
                continue
            statut_col = next((c for c in df.columns if c.lower() in {"statut","status","etat","état","payment status","bank status"}), None)
            if statut_col: df.rename(columns={statut_col:"Statut_red"}, inplace=True)
            else: df["Statut_red"] = pd.NA

            date_col = next((c for c in df.columns if c.lower() in
                             {"date","date opération","date de traitement","transaction date","date paiement","processing date"}), None)
            if date_col: df.rename(columns={date_col:"Date_red_raw"}, inplace=True)
            else: df["Date_red_raw"] = pd.NA
            df["Date_red"] = parse_date(df["Date_red_raw"])

            amt_col = next((c for c in df.columns if c.lower() in {"montant","amount","montant (eur)","total","amount eur"}), None)
            if amt_col: df.rename(columns={amt_col:"Montant_red_raw"}, inplace=True)
            else: df["Montant_red_raw"] = pd.NA
            df["Montant_red"] = parse_amount(df["Montant_red_raw"])

            cur_col = next((c for c in df.columns if c.lower() in {"devise","currency","monnaie"}), None)
            if cur_col:
                df.rename(columns={cur_col:"Devise_red"}, inplace=True)
                df["Devise_red"] = _normalize_text_series(df["Devise_red"])
            else:
                df["Devise_red"] = pd.NA

            df["Votre référence"] = df["Votre référence"].astype(str).str.strip()
            df["join_key_red"] = normalize_join_key(df["Votre référence"])
            df["join_key_red_nospace"] = normalize_join_key_nospace(df["Votre référence"])

            dfs.append(df)
        except Exception as e:
            st.error(f"Erreur {os.path.basename(path)}: {e}")

    if not dfs:
        raise RuntimeError("Aucune donnée RED valide.")

    df_all = pd.concat(dfs, ignore_index=True)
    # dédoublonnage: garde la plus récente
    df_all.sort_values(by=["Votre référence","Date_red"], inplace=True)
    df_all = df_all.drop_duplicates(subset=["Votre référence"], keep="last").reset_index(drop=True)

    csv_path = os.path.join(red_out_dir, "red_transactions_clean.csv")
    parq_path = os.path.join(red_out_dir, "red_transactions_clean.parquet")
    df_all.to_csv(csv_path, index=False, encoding="utf-8-sig", float_format="%.2f")
    df_all.to_parquet(parq_path, index=False, engine="pyarrow")
    return parq_path, csv_path

# -------------------- RECONCILIATION AXIS↔RED --------------------
def reconcile_axis_red(axis_parquet_glob: str, red_parquet_glob: str, out_dir: str) -> tuple[str,str,pd.DataFrame]:
    os.makedirs(out_dir, exist_ok=True)
    out_parq = os.path.join(out_dir, "reconciliation_axis_red.parquet").replace("\\","/")
    out_csv  = os.path.join(out_dir, "reconciliation_axis_red.csv").replace("\\","/")
    axis_glob = axis_parquet_glob.replace("\\","/")
    red_glob  = red_parquet_glob.replace("\\","/")

    sql = f"""
    WITH axis_raw AS (
        SELECT
            *,
            UPPER(REGEXP_REPLACE(CAST("ID_Commande2" AS VARCHAR), '[^A-Za-z0-9]', '', 'g')) AS join_key_axis_nospace,
            UPPER(TRIM(CAST("ID_Commande2" AS VARCHAR))) AS join_key_axis
        FROM read_parquet('{axis_glob}')
    ),
    red_raw AS (
        SELECT
            *,
            UPPER(REGEXP_REPLACE(CAST("Votre référence" AS VARCHAR), '[^A-Za-z0-9]', '', 'g')) AS join_key_red_nospace,
            UPPER(TRIM(CAST("Votre référence" AS VARCHAR))) AS join_key_red
        FROM read_parquet('{red_glob}')
    ),
    j1 AS (
        SELECT
            a.*,
            r1."Votre référence" AS ref_match1,
            r1."Statut_red"      AS Statut_red1,
            r1."Date_red"        AS Date_red1,
            r1."Montant_red"     AS Montant_red1,
            r1."Devise_red"      AS Devise_red1
        FROM axis_raw a
        LEFT JOIN red_raw r1
          ON r1.join_key_red_nospace = a.join_key_axis_nospace
    ),
    j2 AS (
        SELECT
            j1.*,
            r2."Votre référence" AS ref_match2,
            r2."Statut_red"      AS Statut_red2,
            r2."Date_red"        AS Date_red2,
            r2."Montant_red"     AS Montant_red2,
            r2."Devise_red"      AS Devise_red2
        FROM j1
        LEFT JOIN red_raw r2
          ON r2.join_key_red = j1.join_key_axis
         AND j1.ref_match1 IS NULL
    ),
    merged AS (
        SELECT
            j2.*,
            COALESCE(ref_match1,  ref_match2)   AS ref_match,
            COALESCE(Statut_red1, Statut_red2)  AS Statut_red,
            COALESCE(Date_red1,   Date_red2)    AS Date_red,
            COALESCE(Montant_red1,Montant_red2) AS Montant_red,
            COALESCE(Devise_red1, Devise_red2)  AS Devise_red
        FROM j2
    ),
    labeled AS (
        SELECT
            *,
            CASE
              WHEN lower(replace(CAST("Statut transaction" AS VARCHAR),'é','e')) LIKE 'approuv%'
                   AND ref_match IS NOT NULL THEN 'Réconcilié'
              WHEN lower(replace(CAST("Statut transaction" AS VARCHAR),'é','e')) LIKE 'approuv%'
                   AND ref_match IS NULL THEN 'Approuvé non retrouvé dans RED'
              ELSE 'Non approuvé'
            END AS Statut_Reconciliation,
            (ref_match IS NOT NULL) AS Found_in_RED
        FROM merged
    )
    SELECT * EXCLUDE(ref_match1,ref_match2,Statut_red1,Statut_red2,Date_red1,Date_red2,Montant_red1,Montant_red2,Devise_red1,Devise_red2)
    FROM labeled
    """
    con = duckdb.connect()
    con.execute("CREATE OR REPLACE TABLE reconciliation_axis_red AS " + sql)
    con.execute(f"COPY reconciliation_axis_red TO '{out_parq}' (FORMAT PARQUET);")
    con.execute(f"COPY reconciliation_axis_red TO '{out_csv}'  (HEADER, DELIMITER ',');")
    df_preview = con.execute("SELECT * FROM reconciliation_axis_red LIMIT 1000").fetchdf()
    return out_parq, out_csv, df_preview

# -------------------- ANALYSE + SYNTHESE --------------------
def _pick_axis_cols(con) -> tuple[str,str,str]:
    cols_df = con.execute("PRAGMA table_info('recon')").fetchdf()
    cols = cols_df['name'].tolist()
    types = dict(zip(cols_df['name'], cols_df['type']))

    def pick_axis_date():
        cands = [c for c in cols if 'date' in c.lower() and not c.lower().startswith('date_red')]
        def score(c):
            t = types.get(c,''); s=0
            if 'TIMESTAMP' in t or 'DATE' in t: s+=3
            if 'axis' in c.lower(): s+=1
            if 'transaction' in c.lower(): s+=1
            return -s
        cands.sort(key=score)
        return cands[0] if cands else 'Date_red'

    def pick_axis_amount():
        patt=re.compile(r'(montant|amount|total|captured|authorized|sale)', re.I)
        cands=[c for c in cols if patt.search(c) and c not in ('Montant_red','Amount_red','Total_red')]
        def score(c):
            t=types.get(c,''); s=0
            if any(k in t for k in ('DOUBLE','DECIMAL','BIGINT','INTEGER')): s+=3
            if 'axis' in c.lower(): s+=1
            return -s
        cands.sort(key=score)
        return cands[0] if cands else None

    def pick_axis_status():
        if 'Statut transaction' in cols: return 'Statut transaction'
        cands=[c for c in cols if ('statut' in c.lower() or 'status' in c.lower()) and c!='Statut_red']
        return cands[0] if cands else None

    return pick_axis_date(), pick_axis_amount(), pick_axis_status()

def analyze_and_summarize(recon_parquet: str, red_parquet_glob: str) -> dict:
    out = {}
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW recon AS SELECT * FROM read_parquet('{recon_parquet.replace('\\','/')}')")
    AXIS_DATE_COL, AXIS_AMOUNT_COL, AXIS_STATUS_COL = _pick_axis_cols(con)
    if AXIS_AMOUNT_COL is None or AXIS_STATUS_COL is None:
        raise RuntimeError("Colonnes Axis manquantes pour l'analyse.")

    QD, QA, QS = map(qident, [AXIS_DATE_COL, AXIS_AMOUNT_COL, AXIS_STATUS_COL])

    axis_date_expr = f"""
    COALESCE(
      TRY_CAST({QD} AS TIMESTAMP),
      TRY_CAST(strptime(CAST({QD} AS VARCHAR), '%d/%m/%Y %H:%M:%S') AS TIMESTAMP),
      TRY_CAST(strptime(CAST({QD} AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP),
      TRY_CAST(strptime(CAST({QD} AS VARCHAR), '%Y-%m-%d') AS TIMESTAMP),
      TRY_CAST(strptime(CAST({QD} AS VARCHAR), '%d/%m/%Y') AS TIMESTAMP)
    )
    """
    axis_amount_expr = f"""
    COALESCE(
      TRY_CAST({QA} AS DOUBLE),
      TRY_CAST(REPLACE(REPLACE(CAST({QA} AS VARCHAR),' ','') ,',','.') AS DOUBLE)
    )
    """
    axis_status_norm = f"lower(replace(CAST({QS} AS VARCHAR),'é','e'))"

    con.execute(f"""
    CREATE OR REPLACE VIEW v_axis AS
    SELECT
      {axis_date_expr} AS axis_date,
      strftime(date_trunc('month', {axis_date_expr}), '%Y-%m') AS axis_month,
      {axis_amount_expr} AS axis_amount,
      CASE WHEN {axis_status_norm} LIKE 'approuv%' THEN 'Approuvé' ELSE 'Non approuvé' END AS axis_statut
    FROM recon;
    """)

    out["axis_status_monthly"] = con.execute("""
    SELECT axis_month, axis_statut,
           COUNT(*) AS nb_transactions,
           COALESCE(SUM(axis_amount),0) AS montant_total
    FROM v_axis
    GROUP BY 1,2
    ORDER BY 1,2
    """).fetchdf()

    out["axis_found_missing"] = con.execute(f"""
    SELECT
      strftime(date_trunc('month', {axis_date_expr}), '%Y-%m') AS axis_month,
      CASE
        WHEN "Statut_Reconciliation" = 'Réconcilié' THEN 'Trouvée dans Red'
        WHEN "Statut_Reconciliation" = 'Approuvé non retrouvé dans RED' THEN 'Non trouvée dans Red'
        ELSE 'Autre'
      END AS correspondance_red,
      COUNT(*) AS nb_transactions,
      COALESCE(SUM({axis_amount_expr}),0) AS montant_total
    FROM recon
    WHERE {axis_status_norm} LIKE 'approuv%'
    GROUP BY 1,2
    ORDER BY 1,2
    """).fetchdf()

    con.execute(f"CREATE OR REPLACE VIEW ref_matched AS SELECT DISTINCT \"ref_match\" AS votre_reference FROM recon WHERE \"ref_match\" IS NOT NULL")
    con.execute(f"CREATE OR REPLACE VIEW red_all AS SELECT * FROM read_parquet('{red_parquet_glob.replace('\\','/')}')")

    con.execute("""
    CREATE OR REPLACE VIEW red_clean AS
    SELECT
      "Votre référence" AS votre_reference,
      COALESCE(
        TRY_CAST("Date_red" AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red" AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red" AS VARCHAR), '%Y-%m-%d') AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red_raw" AS VARCHAR), '%d/%m/%Y %H:%M:%S') AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red_raw" AS VARCHAR), '%d/%m/%Y') AS TIMESTAMP)
      ) AS date_red,
      strftime(date_trunc('month', COALESCE(
        TRY_CAST("Date_red" AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red" AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red" AS VARCHAR), '%Y-%m-%d') AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red_raw" AS VARCHAR), '%d/%m/%Y %H:%M:%S') AS TIMESTAMP),
        TRY_CAST(strptime(CAST("Date_red_raw" AS VARCHAR), '%d/%m/%Y') AS TIMESTAMP)
      )), '%Y-%m') AS red_month,
      COALESCE(
        TRY_CAST("Montant_red" AS DOUBLE),
        TRY_CAST(REPLACE(REPLACE(CAST("Montant_red_raw" AS VARCHAR),' ','') ,',','.') AS DOUBLE)
      ) AS montant_red,
      COALESCE(CAST("Statut_red" AS VARCHAR), '') AS statut_red,
      *
    FROM red_all
    """)

    con.execute("""
    CREATE OR REPLACE VIEW red_only AS
    SELECT r.*
    FROM red_clean r
    LEFT JOIN ref_matched m
      ON m.votre_reference = r.votre_reference
    WHERE m.votre_reference IS NULL
    """)

    out["red_not_in_axis_monthly"] = con.execute("""
    SELECT red_month,
           CASE WHEN lower(replace(statut_red,'é','e')) LIKE 'approuv%' THEN 'Approuvé' ELSE 'Non approuvé' END AS statut_red_grp,
           COUNT(*) AS nb_transactions,
           COALESCE(SUM(montant_red),0) AS montant_total
    FROM red_only
    GROUP BY 1,2
    ORDER BY 1,2
    """).fetchdf()

    # Synthèse globale
    ax1 = out["axis_status_monthly"].pivot_table(index="axis_month", columns="axis_statut",
                                                 values=["nb_transactions","montant_total"], aggfunc="sum", fill_value=0)
    ax2 = out["axis_found_missing"].pivot_table(index="axis_month", columns="correspondance_red",
                                                values=["nb_transactions","montant_total"], aggfunc="sum", fill_value=0)
    ax3 = out["red_not_in_axis_monthly"].rename(columns={"red_month":"axis_month"}).pivot_table(
        index="axis_month", columns="statut_red_grp",
        values=["nb_transactions","montant_total"], aggfunc="sum", fill_value=0)

    syn = ax1.join(ax2, how="outer").join(ax3, how="outer").fillna(0)
    syn.columns = [" ".join([c for c in col if c]).strip() for col in syn.columns.values]
    out["synthese_mensuelle"] = syn.reset_index().sort_values("axis_month")

    # Totaux
    out["tot_axis_status"] = out["axis_status_monthly"].groupby("axis_statut", as_index=False)[["nb_transactions","montant_total"]].sum()
    out["tot_axis_found"]  = out["axis_found_missing"].groupby("correspondance_red", as_index=False)[["nb_transactions","montant_total"]].sum()
    out["tot_red_only"]    = out["red_not_in_axis_monthly"].groupby("statut_red_grp", as_index=False)[["nb_transactions","montant_total"]].sum()

    # Arrondis 2 décimales
    for k, df in out.items():
        if isinstance(df, pd.DataFrame):
            for c in df.columns:
                if pd.api.types.is_float_dtype(df[c].dtype):
                    df[c] = df[c].round(2)
    return out

# -------------------- UI --------------------
st.set_page_config(page_title="NTR Réconciliator", layout="wide")
st.title("Réconciliation Axis ↔ Red")

mode = st.sidebar.radio("Source des données", ["Dossier local (NTR)", "Glisser-déposer"])
base_dir = st.sidebar.text_input("Répertoire BASE", DEFAULT_BASE)

cfg = cfg_from_base(base_dir)
for d in (cfg["RED_OUT_DIR"], cfg["OUT_DIR"], cfg["AN_DIR"]):
    os.makedirs(d, exist_ok=True)

uploaded_axis = []
uploaded_red  = []

if mode == "Glisser-déposer":
    st.subheader("Chargement par glisser-déposer")
    uploaded_axis = st.file_uploader("Parquet Axis (un ou plusieurs)", type=["parquet"], accept_multiple_files=True)
    uploaded_red  = st.file_uploader("Fichiers Red (CSV/Parquet, un ou plusieurs)", type=["csv","parquet"], accept_multiple_files=True)

    tmp_axis_dir = tempfile.mkdtemp(prefix="axis_")
    tmp_red_dir  = tempfile.mkdtemp(prefix="red_")
    axis_paths, red_paths = [], []

    if uploaded_axis:
        for f in uploaded_axis:
            p = os.path.join(tmp_axis_dir, f.name)
            with open(p, "wb") as w: w.write(f.getbuffer())
            axis_paths.append(p)

    if uploaded_red:
        for f in uploaded_red:
            p = os.path.join(tmp_red_dir, f.name)
            with open(p, "wb") as w: w.write(f.getbuffer())
            red_paths.append(p)

    if st.button("1) Traiter Red (clean + parquet)"):
        try:
            red_parq_path, red_csv_path = process_red_files_to_output(red_paths, cfg["RED_OUT_DIR"])
            st.success(f"Red traité → {red_parq_path}")
        except Exception as e:
            st.error(e)

    if st.button("2) Réconcilier Axis ↔ Red"):
        try:
            # Axis glob = les fichiers déposés
            axis_glob = os.path.join(tmp_axis_dir, "*.parquet")
            red_glob  = os.path.join(cfg["RED_OUT_DIR"], "*.parquet")
            out_parq, out_csv, prev = reconcile_axis_red(axis_glob, red_glob, cfg["OUT_DIR"])
            st.success("Réconciliation OK")
            st.dataframe(prev, use_container_width=True, height=320)
            st.download_button("Télécharger reconciliation_axis_red.csv",
                               data=open(out_csv, "rb").read(), file_name="reconciliation_axis_red.csv", mime="text/csv")
        except Exception as e:
            st.error(e)

else:
    st.subheader("Mode local (arborescence NTR)")
    st.text_area("Chemins utilisés", value="\n".join([
        f"AXIS_OUT_DIR = {cfg['AXIS_OUT_DIR']}",
        f"RED_IN_DIR   = {cfg['RED_IN_DIR']}",
        f"RED_OUT_DIR  = {cfg['RED_OUT_DIR']}",
        f"OUT_DIR      = {cfg['OUT_DIR']}",
        f"AN_DIR       = {cfg['AN_DIR']}",
    ]), height=120)

    if st.button("1) Traiter Red (clean + parquet)"):
        try:
            # prend tout CSV/Parquet du dossier input Red
            files = [os.path.join(cfg["RED_IN_DIR"], f) for f in os.listdir(cfg["RED_IN_DIR"])
                     if f.lower().endswith((".csv",".parquet"))]
            red_parq_path, red_csv_path = process_red_files_to_output(files, cfg["RED_OUT_DIR"])
            st.success(f"Red traité → {red_parq_path}")
        except Exception as e:
            st.error(e)

    if st.button("2) Réconcilier Axis ↔ Red"):
        try:
            axis_glob = os.path.join(cfg["AXIS_OUT_DIR"], "*.parquet")
            red_glob  = os.path.join(cfg["RED_OUT_DIR"], "*.parquet")
            out_parq, out_csv, prev = reconcile_axis_red(axis_glob, red_glob, cfg["OUT_DIR"])
            st.success("Réconciliation OK")
            st.dataframe(prev, use_container_width=True, height=320)
            st.download_button("Télécharger reconciliation_axis_red.csv",
                               data=open(out_csv, "rb").read(), file_name="reconciliation_axis_red.csv", mime="text/csv")
        except Exception as e:
            st.error(e)

st.divider()
st.subheader("3) Analyses et synthèse")

if st.button("Générer analyses + synthèse"):
    try:
        recon_parq = os.path.join(cfg["OUT_DIR"], "reconciliation_axis_red.parquet")
        red_glob   = os.path.join(cfg["RED_OUT_DIR"], "*.parquet")
        results = analyze_and_summarize(recon_parq, red_glob)

        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Axis approuvées", int(results["axis_status_monthly"][results["axis_status_monthly"]["axis_statut"]=="Approuvé"]["nb_transactions"].sum()))
        with c2: st.metric("Réconciliées", int(results["axis_found_missing"][results["axis_found_missing"]["correspondance_red"]=="Trouvée dans Red"]["nb_transactions"].sum()))
        with c3: st.metric("Red non trouvées en Axis", int(results["red_not_in_axis_monthly"]["nb_transactions"].sum()))

        st.write("Axis par mois et statut")
        st.dataframe(results["axis_status_monthly"], use_container_width=True)

        st.write("Axis approuvées: trouvées vs non trouvées")
        st.dataframe(results["axis_found_missing"], use_container_width=True)

        st.write("Red non retrouvées côté Axis")
        st.dataframe(results["red_not_in_axis_monthly"], use_container_width=True)

        st.write("Synthèse mensuelle")
        st.dataframe(results["synthese_mensuelle"], use_container_width=True)

        # Téléchargements CSV
        st.download_button("axis_status_monthly.csv", fmt_csv(results["axis_status_monthly"]), "axis_status_monthly.csv", "text/csv")
        st.download_button("axis_approved_found_vs_missing_monthly.csv", fmt_csv(results["axis_found_missing"]), "axis_approved_found_vs_missing_monthly.csv", "text/csv")
        st.download_button("red_not_in_axis_monthly.csv", fmt_csv(results["red_not_in_axis_monthly"]), "red_not_in_axis_monthly.csv", "text/csv")
        st.download_button("synthese_mensuelle.csv", fmt_csv(results["synthese_mensuelle"]), "synthese_mensuelle.csv", "text/csv")

        # XLSX multi-feuilles
        xlsx_bytes = write_xlsx_multi([
            ("Axis_Status_Monthly", results["axis_status_monthly"]),
            ("Axis_Approved_Found", results["axis_found_missing"]),
            ("Red_Not_in_Axis_M",   results["red_not_in_axis_monthly"]),
            ("Synthese_Mensuelle",  results["synthese_mensuelle"]),
            ("Tot_Axis_Status",     results["tot_axis_status"]),
            ("Tot_Axis_Found",      results["tot_axis_found"]),
            ("Tot_Red_Only",        results["tot_red_only"]),
        ])
        st.download_button("analytics_summary.xlsx", xlsx_bytes, "analytics_summary.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        st.error(e)

st.caption("NTR — Réconciliation Financière des Transactions")

