import streamlit as st
import pandas as pd
from datetime import datetime, date
import io, re
from ftplib import FTP, error_perm

# ---------------- CONFIG ----------------
st.set_page_config(page_title="PRD • Raccolta Dati", page_icon="🛠️", layout="centered")
st.title("🛠️ Raccolta Dati Produzione")

PRIMARY_DIR  = "/httpdocs/IA/luppichini/PRD"   # <— cartella GIÀ ESISTENTE
REMOTE_FILE  = "Dati_PRD_Alessio.csv"         # <— file GIÀ ESISTENTE
OPERATORI    = ["ALESSIO", "ALESSANDRO", "MICHELE", "LUCA"]

# ---------------- FTP ----------------
def ftp_connect() -> FTP:
    ftp = FTP(st.secrets["FTP_HOST"], timeout=25)
    ftp.set_pasv(True)
    ftp.login(user=st.secrets["FTP_USER"], passwd=st.secrets["FTP_PASS"])
    return ftp

def ftp_cwd_existing(ftp: FTP, target_dir: str):
    try:
        ftp.cwd(target_dir)
    except Exception as e:
        raise RuntimeError(f"Impossibile entrare in {target_dir}: {e}")

def ftp_download_file(ftp: FTP, filename: str) -> bytes | None:
    bio = io.BytesIO()
    try:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        return bio.getvalue()
    except error_perm as e:
        if "550" in str(e):   # file non trovato/vuoto
            return None
        raise

def ftp_upload_file(ftp: FTP, filename: str, payload: bytes):
    bio = io.BytesIO(payload)
    ftp.storbinary(f"STOR {filename}", bio)

# ---------------- CSV UTILS ----------------
def append_row(existing_bytes: bytes | None, row: dict) -> bytes:
    new_df = pd.DataFrame([row])
    if existing_bytes:
        try:
            old_df = pd.read_csv(io.BytesIO(existing_bytes))
            df = pd.concat([old_df, new_df], ignore_index=True)
        except Exception:
            df = new_df
    else:
        df = new_df
    out = io.BytesIO()
    df.to_csv(out, index=False)
    return out.getvalue()

def std(x: str) -> str:
    return " ".join(str(x or "").strip().split())

def to_int_safe(x, default=0):
    try:
        v = int(x)
        return v if v >= 0 else default
    except:
        return default

# ---------------- SIDEBAR ----------------
with st.sidebar:
    mode = st.radio("Modalità", ["✍️ Scrittura", "📖 Lettura"], index=0)
    if st.button("🔎 Verifica accesso"):
        try:
            ftp = ftp_connect()
            root_pwd = ftp.pwd()
            ftp_cwd_existing(ftp, PRIMARY_DIR)
            here = ftp.pwd()
            # Prova sola lettura header (se il file esiste)
            data = ftp_download_file(ftp, REMOTE_FILE)
            ftp.quit()
            st.success(f"Accesso OK. Root: {root_pwd} → Dir: {here} → File: {REMOTE_FILE} {'(trovato)' if data is not None else '(non trovato o vuoto)'}")
        except Exception as e:
            st.error(f"Verifica fallita: {e}")

# ---------------- SCRITTURA ----------------
if mode == "✍️ Scrittura":
    st.subheader("✍️ Inserisci dati lavorazione")

    operatore    = st.selectbox("Operatore", OPERATORI, index=0)
    data_lavoro  = st.date_input("Data", value=date.today(), format="DD/MM/YYYY")

    st.markdown("#### 🔧 Dati tecnici")
    codice_materiale = st.text_input("CODICE Materiale").upper()
    descrizione      = st.text_input("DESCRIZIONE")
    ciclo_nr         = st.text_input("CICLO NR")
    macchina         = st.selectbox("MACCHINA",
                        ["DMG MORI","TAKISAWA","QUASER","MAZAK VCN","MAZAK VRX","MAZAK HCN","HYUNDAI","HURCO"], 0)
    numero_prg       = st.text_input("NUMERO PRG")
    cartella_mac     = st.text_input("CARTELLA MACCHINA")
    fase             = st.selectbox("FASE",
                        ["Fase 1","Fase 2","Fase 3","Fase 4","Fase 5","Fase 6","Attrezzaggio","Programmazione"], 0)

    st.markdown("#### ⏱️ Tempo fase (ore/min)")
    c1, c2 = st.columns(2)
    with c1: ore    = st.number_input("Ore",    0, 24, 0, 1)
    with c2: minuti = st.number_input("Minuti", 0, 59, 0, 1)

    if st.button("📩 Invia"):
        missing = []
        if not codice_materiale: missing.append("CODICE Materiale")
        if not descrizione:      missing.append("DESCRIZIONE")
        if not fase:             missing.append("FASE")

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
                ftp = ftp_connect()
                ftp_cwd_existing(ftp, PRIMARY_DIR)        # <— entra solo in dir ESISTENTE
                existing = ftp_download_file(ftp, REMOTE_FILE)  # può tornare None se file vuoto/non creato
                updated  = append_row(existing, record)
                ftp_upload_file(ftp, REMOTE_FILE, updated)
                where = ftp.pwd()
                ftp.quit()
                st.success(f"✅ Salvato in: {where}/{REMOTE_FILE}")
                st.balloons()
            except Exception as e:
                st.error(f"❌ Errore salvataggio su FTP: {e}")
                st.caption("Controlla: nome file esatto (case sensitive), permessi, quota, modalità PASV.")

# ---------------- LETTURA ----------------
else:
    st.subheader("📖 Consultazione dati")

    try:
        ftp = ftp_connect()
        ftp_cwd_existing(ftp, PRIMARY_DIR)
        csv_bytes = ftp_download_file(ftp, REMOTE_FILE)
        here = ftp.pwd()
        ftp.quit()
    except Exception as e:
        csv_bytes = None
        here = "(n/a)"
        st.error(f"Errore lettura FTP: {e}")

    if not csv_bytes:
        st.info(f"Nessun dato presente in {here}/{REMOTE_FILE} (file vuoto o non trovato).")
    else:
        try:
            df = pd.read_csv(io.BytesIO(csv_bytes))
        except Exception:
            st.error("CSV non leggibile (intestazioni errate o file corrotto).")
            df = None

        if df is not None and not df.empty:
            c1, c2, c3 = st.columns(3)
            with c1: f_operatore = st.selectbox("Operatore", ["(tutti)"] + OPERATORI, 0)
            with c2: f_codice = st.text_input("Cerca CODICE Materiale", "")
            with c3: f_data = st.date_input("Solo data", value=None)

            fdf = df.copy()
            if f_operatore != "(tutti)":
                fdf = fdf[fdf["OPERATORE"] == f_operatore]
            if f_codice:
                patt = re.compile(re.escape(f_codice.strip()), re.IGNORECASE)
                fdf = fdf[fdf["CODICE_MATERIALE"].astype(str).str.contains(patt)]
            if f_data:
                fdf = fdf[fdf["DATA"] == f_data.strftime("%Y-%m-%d")]

            if "Timestamp" in fdf.columns:
                fdf["Timestamp"] = pd.to_datetime(fdf["Timestamp"], errors="coerce")
                fdf = fdf.sort_values(by="Timestamp", ascending=False)

            st.dataframe(fdf, use_container_width=True)
            st.download_button("⬇️ Scarica CSV filtrato",
                               data=fdf.to_csv(index=False).encode("utf-8"),
                               file_name="estratto_prd.csv",
                               mime="text/csv")
        else:
            st.info("Nessun dato disponibile.")
