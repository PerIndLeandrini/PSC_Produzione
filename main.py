import streamlit as st
import pandas as pd
from datetime import datetime, date
import io, re
from ftplib import FTP, error_perm
import math
import re

# ---------- CONFIG ----------
st.set_page_config(page_title="PRD • Raccolta Dati", page_icon="🛠️", layout="wide")
PRIMARY_DIR  = "/httpdocs/IA/luppichini/PRD"
REMOTE_FILE  = "Dati_PRD_Alessio.csv"
OPERATORI    = ["ALESSIO", "ALESSANDRO", "MICHELE", "LUCA"]

# ---------- STILE CLEAN ----------
st.markdown("""
<style>
.block-container{padding-top:3.2rem !important;}
/* Header clean */
.prd-header{display:flex;align-items:center;gap:.6rem;margin:0 0 1rem 0}
.prd-title{font-weight:800;font-size:1.6rem;letter-spacing:-.02em;margin:0;color:#111827}

/* Filtri e pulsanti */
.prd-toolbar{display:flex;align-items:end;gap:12px;margin:.2rem 0 1rem 0}

/* Card minimal */
.prd-card{
  border:1px solid #e5e7eb;
  border-left:6px solid #2563eb; /* accento blu */
  border-radius:12px;
  padding:14px 16px;
  margin:12px 0;
  background:#fff;
  box-shadow:0 1px 2px rgba(0,0,0,.04);
}
.prd-h4{font-size:1.02rem;font-weight:800;color:#111827;margin:0 0 .4rem 0}
.prd-meta{color:#374151;font-size:.92rem;margin-bottom:8px}
.prd-chip{
  display:inline-block;padding:3px 8px;margin:0 6px 6px 0;border-radius:999px;
  background:#eff6ff;color:#1d4ed8;border:1px solid #dbeafe;font-weight:700;font-size:.78rem
}
.prd-sep{height:1px;background:#e5e7eb;margin:8px 0}
.prd-kv{font-size:.9rem;color:#111827}

/* Dataframe full width */
[data-testid="stDataFrameResizable"]{width:100% !important;}
</style>
""", unsafe_allow_html=True)

# ---------- HEADER ----------
st.markdown(f"""
<div class="prd-header">
  <span style="font-size:1.6rem">🛠️</span>
  <div class="prd-title">Raccolta Dati Produzione</div>
</div>
""", unsafe_allow_html=True)

# ---------- FTP ----------
def ftp_connect() -> FTP:
    ftp = FTP(st.secrets["FTP_HOST"], timeout=25)
    ftp.set_pasv(True)
    ftp.login(user=st.secrets["FTP_USER"], passwd=st.secrets["FTP_PASS"])
    return ftp

def ftp_cwd_existing(ftp: FTP, target_dir: str):
    try: ftp.cwd(target_dir)
    except Exception as e: raise RuntimeError(f"Impossibile entrare in {target_dir}: {e}")

def ftp_download_file(ftp: FTP, filename: str) -> bytes | None:
    bio = io.BytesIO()
    try:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        return bio.getvalue()
    except error_perm as e:
        if "550" in str(e): return None
        raise

def ftp_upload_file(ftp: FTP, filename: str, payload: bytes):
    bio = io.BytesIO(payload)
    ftp.storbinary(f"STOR {filename}", bio)

# ---------- CSV helpers ----------
def _detect_sep(csv_bytes: bytes, default=";"):
    if not csv_bytes: return default
    try: head = csv_bytes.splitlines()[0].decode("utf-8","ignore")
    except: return default
    return ";" if head.count(";") >= head.count(",") else ","

def read_csv_bytes(csv_bytes: bytes | None):
    if not csv_bytes: return pd.DataFrame(), ";"
    sep = _detect_sep(csv_bytes)
    try: df = pd.read_csv(io.BytesIO(csv_bytes), sep=sep)
    except Exception:
        sep = "," if sep == ";" else ";"
        df = pd.read_csv(io.BytesIO(csv_bytes), sep=sep)
    return df, sep

def std(x: str) -> str:
    return " ".join(str(x or "").strip().split())

def to_int_safe(x, default=0):
    try:
        s = str(x).strip().replace(",", ".")
        v = int(float(s))
        return v if v >= 0 else default
    except Exception:
        return default

def hhmm_to_minutes(s: str) -> int | None:
    """Accetta 'H:MM' o 'HH:MM' (anche con spazi)."""
    try:
        s = str(s).strip()
        m = re.match(r"^\s*(\d{1,2})\s*:\s*([0-5]?\d)\s*$", s)
        if not m: return None
        return int(m.group(1))*60 + int(m.group(2))
    except: return None

def minutes_to_hhmm(m) -> str:
    try:
        if pd.isna(m):
            return ""
        m = int(round(float(m)))
        h = m // 60
        r = m % 60
        return f"{h:02d}:{r:02d}"
    except Exception:
        return ""

def parse_hhmmss_to_minutes(s: str) -> int | None:
    """
    Converte 'HH:MM:SS' o 'HH:MM' (anche con spazi) in minuti.
    Restituisce None se il formato non combacia.
    """
    if s is None:
        return None
    s = str(s).strip()
    # HH:MM:SS
    m = re.match(r"^\s*(\d{1,3})\s*:\s*([0-5]?\d)\s*:\s*([0-5]?\d)\s*$", s)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        return hh * 60 + mm
    # HH:MM
    m = re.match(r"^\s*(\d{1,3})\s*:\s*([0-5]?\d)\s*$", s)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        return hh * 60 + mm
    return None

def normalize_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Popola sempre:
      - TEMPO_FASE_MIN (int, minuti)
      - TEMPO_FASE (hh:mm) per display

    Regole:
      1) Se esiste TEMPO_FASE_MIN:
         - se contiene ":" → interpreta come hh:mm[:ss] e converte in minuti
         - altrimenti prova come numero di minuti
      2) Altrimenti cerca qualunque colonna che contenga 'TEMPO' e prova hh:mm[:ss]
      3) Altrimenti se ci sono ORE + MINUTI, combina.
    """
    # mappa uppercase -> nome originale
    u2orig = {c.upper().strip(): c for c in df.columns}

    tmin = None

    # 1) Caso TEMPO_FASE_MIN presente
    if "TEMPO_FASE_MIN" in u2orig:
        c = u2orig["TEMPO_FASE_MIN"]
        ser = df[c]

        # Se c'è almeno un ":" → trattiamo tutta la colonna come hh:mm[:ss]
        if ser.astype(str).str.contains(":").any():
            tmin = ser.apply(parse_hhmmss_to_minutes)
            tmin = pd.Series(tmin, dtype="Int64")
        else:
            # prova come numero di minuti
            tmin = pd.to_numeric(ser, errors="coerce").astype("Int64")

    # 2) Altre colonne con 'TEMPO' (es. TEMPO_FASE, TEMPO)
    if tmin is None:
        tempo_cols = [u2orig[u] for u in u2orig if "TEMPO" in u]
        for c in tempo_cols:
            ser = df[c].astype(str)
            if ser.str.contains(":").any():
                tmin = ser.apply(parse_hhmmss_to_minutes)
                tmin = pd.Series(tmin, dtype="Int64")
                break

    # 3) ORE + MINUTI
    if tmin is None and "ORE" in u2orig and "MINUTI" in u2orig:
        h, m = u2orig["ORE"], u2orig["MINUTI"]
        tmin = df[h].apply(to_int_safe) * 60 + df[m].apply(to_int_safe)
        tmin = tmin.astype("Int64")

    # Assegna colonne finali
    if tmin is None:
        df["TEMPO_FASE_MIN"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    else:
        df["TEMPO_FASE_MIN"] = tmin

    df["TEMPO_FASE (hh:mm)"] = df["TEMPO_FASE_MIN"].apply(minutes_to_hhmm)
    return df

def append_row(existing_bytes: bytes | None, row: dict) -> bytes:
    new_df = pd.DataFrame([row])
    if existing_bytes:
        old_df, sep = read_csv_bytes(existing_bytes)
        if not old_df.empty:
            # allinea colonne
            for c in new_df.columns:
                if c not in old_df.columns: old_df[c] = pd.NA
            for c in old_df.columns:
                if c not in new_df.columns: new_df[c] = pd.NA
            df = pd.concat([old_df[new_df.columns], new_df], ignore_index=True)
        else:
            df = new_df; sep=";"
    else:
        df = new_df; sep=";"
    out = io.BytesIO(); df.to_csv(out, index=False, sep=sep); return out.getvalue()

# ---------- SIDEBAR ----------
with st.sidebar:
    mode = st.radio("Modalità", ["✍️ Scrittura", "📖 Lettura"], index=1)
    if st.button("🔎 Verifica accesso"):
        try:
            ftp = ftp_connect(); root_pwd = ftp.pwd()
            ftp_cwd_existing(ftp, PRIMARY_DIR); here = ftp.pwd()
            data = ftp_download_file(ftp, REMOTE_FILE); ftp.quit()
            st.success(f"OK. Root: {root_pwd} → Dir: {here} → File: {REMOTE_FILE} "
                       f"{'(trovato)' if data is not None else '(vuoto)'}")
        except Exception as e:
            st.error(f"Verifica fallita: {e}")

# ---------- SCRITTURA ----------
if mode == "✍️ Scrittura":
    st.subheader("✍️ Inserisci dati")
    operatore = st.selectbox("Operatore", OPERATORI, 0)
    data_lavoro = st.date_input("Data", value=date.today(), format="DD/MM/YYYY")

    st.markdown("#### Dati tecnici")
    c1, c2 = st.columns([2,3])
    with c1: codice_materiale = st.text_input("CODICE Materiale").upper()
    with c2: descrizione = st.text_input("DESCRIZIONE")
    c3, c4, c5 = st.columns(3)
    with c3: ciclo_nr = st.text_input("CICLO NR")
    with c4: macchina = st.selectbox("MACCHINA",
            ["DMG MORI","TAKISAWA","QUASER","MAZAK VCN","MAZAK VRX","MAZAK HCN","HYUNDAI","HURCO"], 0)
    with c5: fase = st.selectbox("FASE",
            ["Fase 1","Fase 2","Fase 3","Fase 4","Fase 5","Fase 6","Attrezzaggio","Programmazione"], 0)
    c6, c7, c8 = st.columns(3)
    with c6: numero_prg = st.text_input("NUMERO PRG")
    with c7: cartella_mac = st.text_input("CARTELLA MACCHINA")
    with c8:
        st.markdown("Tempo fase (hh:mm)")
        ore = st.number_input("Ore", 0, 24, 0, 1, label_visibility="collapsed")
        minuti = st.number_input("Minuti", 0, 59, 0, 1, label_visibility="collapsed")

    if st.button("📩 Invia"):
        missing = []
        if not codice_materiale: missing.append("CODICE Materiale")
        if not descrizione: missing.append("DESCRIZIONE")
        if not fase: missing.append("FASE")
        if missing:
            st.error("Compila: " + ", ".join(missing))
        else:
            tot_min = to_int_safe(ore)*60 + to_int_safe(minuti)
            record = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "OPERATORE": operatore,
                "DATA": data_lavoro.strftime("%Y-%m-%d"),
                "CODICE_MATERIALE": std(codice_materiale),
                "DESCRIZIONE": std(descrizione),
                "CICLO_NR": std(ciclo_nr),
                "MACCHINA": std(macchina),
                "NUMERO_PRG": std(numero_prg),
                "CARTELLA_MACCHINA": std(cartella_mac),
                "FASE": std(fase),
                "TEMPO_FASE_MIN": tot_min,
            }
            try:
                ftp = ftp_connect(); ftp_cwd_existing(ftp, PRIMARY_DIR)
                existing = ftp_download_file(ftp, REMOTE_FILE)
                updated  = append_row(existing, record)
                ftp_upload_file(ftp, REMOTE_FILE, updated)
                where = ftp.pwd(); ftp.quit()
                st.success(f"✅ Salvato in: {where}/{REMOTE_FILE}")
                st.balloons()
            except Exception as e:
                st.error(f"❌ Errore salvataggio su FTP: {e}")

# ---------- LETTURA ----------
else:
    st.subheader("📘 Consultazione dati")

    # leggi CSV
    try:
        ftp = ftp_connect(); ftp_cwd_existing(ftp, PRIMARY_DIR)
        csv_bytes = ftp_download_file(ftp, REMOTE_FILE); here = ftp.pwd(); ftp.quit()
    except Exception as e:
        csv_bytes = None; here="(n/a)"; st.error(f"Lettura FTP: {e}")

    if not csv_bytes:
        st.info(f"Nessun dato presente in {here}/{REMOTE_FILE}.")
    else:
        df, sep = read_csv_bytes(csv_bytes)
        df.columns = [c.strip() for c in df.columns]
        df = normalize_time_columns(df)

        # ordinamento colonne
        preferred = ["Timestamp","OPERATORE","DATA","CODICE_MATERIALE","DESCRIZIONE","CICLO_NR",
                     "MACCHINA","NUMERO_PRG","CARTELLA_MACCHINA","FASE","TEMPO_FASE_MIN","TEMPO_FASE (hh:mm)"]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]

        # stato filtri + reset
        ss = st.session_state
        ss.setdefault("flt_operatore","(tutti)")
        ss.setdefault("flt_codice","")
        ss.setdefault("flt_descr","")
        ss.setdefault("flt_data",None)

        st.markdown("### 🔎 Filtra")
        with st.container():
            col1,col2,col3,col4,col5 = st.columns([1,1.2,2,1.1,.9])
            with col1: ss.flt_operatore = st.selectbox("Operatore", ["(tutti)"]+OPERATORI,
                                                       index=(["(tutti)"]+OPERATORI).index(ss.flt_operatore))
            with col2: ss.flt_codice = st.text_input("CODICE Materiale contiene", ss.flt_codice)
            with col3: ss.flt_descr  = st.text_input("DESCRIZIONE contiene", ss.flt_descr)
            with col4: ss.flt_data   = st.date_input("Solo data", value=ss.flt_data)
            with col5:
                if st.button("↺ Reset filtri"):
                    ss.flt_operatore="(tutti)"; ss.flt_codice=""; ss.flt_descr=""; ss.flt_data=None; st.rerun()

        # applica filtri
        fdf = df.copy()
        if ss.flt_operatore!="(tutti)" and "OPERATORE" in fdf: fdf = fdf[fdf["OPERATORE"]==ss.flt_operatore]
        if ss.flt_codice and "CODICE_MATERIALE" in fdf:
            fdf = fdf[fdf["CODICE_MATERIALE"].astype(str).str.contains(re.escape(ss.flt_codice), case=False, regex=True)]
        if ss.flt_descr and "DESCRIZIONE" in fdf:
            fdf = fdf[fdf["DESCRIZIONE"].astype(str).str.contains(re.escape(ss.flt_descr), case=False, regex=True)]
        if ss.flt_data and "DATA" in fdf:
            fdf = fdf[fdf["DATA"]==ss.flt_data.strftime("%Y-%m-%d")]

        if "Timestamp" in fdf:
            fdf["Timestamp"] = pd.to_datetime(fdf["Timestamp"], errors="coerce")
            fdf = fdf.sort_values("Timestamp", ascending=False)

        st.markdown("### 👀 Visualizzazione")
        left, _ = st.columns([1,4])
        show_cards = left.toggle("Modalità mobile-friendly (schede)", value=True)

        if show_cards:
            if fdf.empty:
                st.info("Nessun record corrisponde ai filtri.")
            else:
                for _, r in fdf.iterrows():
                    st.markdown(f"""
                    <div class="prd-card">
                      <div class="prd-h4">🔩 {r.get('CODICE_MATERIALE','')} — {r.get('DESCRIZIONE','')}</div>
                      <div class="prd-meta">📅 <b>{r.get('DATA','')}</b> &nbsp;•&nbsp; 👤 <b>{r.get('OPERATORE','')}</b> &nbsp;•&nbsp; 🏭 <b>{r.get('MACCHINA','')}</b> &nbsp;•&nbsp; 🚦 <b>{r.get('FASE','')}</b></div>
                      <div>
                        <span class="prd-chip">CICLO: {r.get('CICLO_NR','')}</span>
                        <span class="prd-chip">PRG: {r.get('NUMERO_PRG','')}</span>
                        <span class="prd-chip">CARTELLA: {r.get('CARTELLA_MACCHINA','')}</span>
                        <span class="prd-chip">Tempo: {r.get('TEMPO_FASE (hh:mm)','')}</span>
                      </div>
                      <div class="prd-sep"></div>
                      <div class="prd-kv"><b>Timestamp:</b> {r.get('Timestamp','')}</div>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.dataframe(fdf, use_container_width=True, height=620)

        st.download_button(
            "⬇️ Scarica CSV filtrato",
            data=fdf.to_csv(index=False, sep=sep).encode("utf-8"),
            file_name="estratto_prd.csv",
            mime="text/csv",
        )
