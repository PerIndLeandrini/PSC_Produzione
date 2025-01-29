import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px

# Sidebar per i dati di input
# Aggiungere il logo in alto a sinistra
st.sidebar.image("logo.png", width=150) 
st.sidebar.header("Inserisci i dati dell'ordine")

# Sezione A: Dati Ordine
data_inizio = st.sidebar.date_input("Data di inizio lavori", datetime.today())
data_inizio_str = data_inizio.strftime("%d/%m/%Y")
num_pezzi = st.sidebar.number_input("Numero totale di pezzi", min_value=1, step=1)
num_lotti = st.sidebar.number_input("Numero di lotti", min_value=1, max_value=10, step=1)

# Gestione dinamica dei lotti
pezzi_per_lotto = {}
for i in range(1, num_lotti + 1):
    pezzi_per_lotto[f'Lotto {i}'] = st.sidebar.number_input(f"Pezzi nel Lotto {i}", min_value=1, step=1)

# Ore lavorative giornaliere e lavoro al sabato
ore_giornaliere = st.sidebar.number_input("Ore lavorative giornaliere", min_value=4.0, max_value=12.0, step=0.5, value=8.0)
lavora_sabato = st.sidebar.checkbox("Lavoriamo il sabato? (4 ore fisse)")

giorni_lavorativi = 5 + (1 if lavora_sabato else 0)  # Settimana lavorativa

# Sezione B: Attività Preliminari
st.sidebar.header("Attività Preliminari")
progettazione = st.sidebar.number_input("Durata Progettazione (giorni)", min_value=0.0, step=0.5)
supporto = st.sidebar.number_input("Durata Realizzazione Supporto (giorni)", min_value=0.0, step=0.5)
provino = st.sidebar.number_input("Durata Primo Provino (giorni)", min_value=0.0, step=0.5)

# Sezione C: Produzione
st.sidebar.header("Produzione")
num_fasi_produzione = st.sidebar.number_input("Numero di fasi di produzione", min_value=1, max_value=8, step=1)

fasi_produzione = {}
for i in range(1, num_fasi_produzione + 1):
    fasi_produzione[f'Fase {i}'] = st.sidebar.number_input(f"Ore di lavoro Fase {i}", min_value=0.5, step=0.5)

# Sezione D: Attività Outsourcing
st.sidebar.header("Attività di Outsourcing")
outsourcing = {}
for attivita in ["Trattamento", "Verniciatura", "Protezione", "Lavorazione Meccanica"]:
    if st.sidebar.checkbox(f"{attivita} necessario?"):
        outsourcing[attivita] = st.sidebar.number_input(f"Ore per {attivita}", min_value=0.5, step=0.5)

# Sezione E: Controllo Qualità
st.sidebar.header("Qualità")
controllo_qualita = st.sidebar.number_input("Ore di Controllo Qualità", min_value=0.0, step=0.5)

# Sezione F: Invio a Collaudo
st.sidebar.header("Collaudo")
collaudo = st.sidebar.number_input("Ore di Invio a Collaudo", min_value=0.0, step=0.5)

# Strutturazione riepilogo
st.title("Analisi Produzione")

st.write("## Riepilogo Dati Ordine")
st.write(f"**Data di inizio lavori:** {data_inizio}")
st.write(f"**Numero totale di pezzi:** {num_pezzi}")
st.write(f"**Numero di lotti:** {num_lotti}")
for lotto, pezzi in pezzi_per_lotto.items():
    st.write(f"{lotto}: {pezzi} pezzi")
st.write(f"**Ore lavorative giornaliere:** {ore_giornaliere}")
st.write(f"**Lavoro al sabato:** {'Sì (4 ore)' if lavora_sabato else 'No'}")

st.write("## Riepilogo Attività Preliminari")
st.write(f"Progettazione: {progettazione} giorni")
st.write(f"Realizzazione Supporto: {supporto} giorni")
st.write(f"Primo Provino: {provino} giorni")

st.write("## Riepilogo Produzione")
for fase, ore in fasi_produzione.items():
    st.write(f"{fase}: {ore} ore")

st.write("## Riepilogo Attività Outsourcing")
for attivita, ore in outsourcing.items():
    st.write(f"{attivita}: {ore} ore")

st.write("## Riepilogo Controllo Qualità e Collaudo")
st.write(f"Controllo Qualità: {controllo_qualita} ore")
st.write(f"Collaudo: {collaudo} ore")

st.write("## ANALISI")

# Calcolo date di consegna dei lotti e ordine finale
st.write("### CALCOLO DATA CONSEGNA")

start_date = data_inizio
lotti_consegna = {}
for lotto, pezzi in pezzi_per_lotto.items():
    giorni_per_lotto = (sum(fasi_produzione.values()) * pezzi) / ore_giornaliere
    end_date = start_date + timedelta(days=int(giorni_per_lotto * (7 / giorni_lavorativi)))
    lotti_consegna[lotto] = end_date.strftime('%d/%m/%Y')
    start_date = end_date

for lotto, data in lotti_consegna.items():
    st.write(f"{lotto}: {data}")

# Data di consegna dell'ordine
finalizzazione_tempo = (sum(outsourcing.values()) + controllo_qualita + collaudo) / ore_giornaliere
data_consegna = start_date + timedelta(days=int(finalizzazione_tempo * (7 / giorni_lavorativi)))
st.write(f"**Data conclusiva dell'ordine:** {data_consegna.strftime('%d/%m/%Y')}")

# Creazione del Gantt delle attività con colori
attivita_df = []
start_date = data_inizio

# Aggiunta attività con categoria
for attivita, durata in {"Progettazione": progettazione, "Supporto": supporto, "Provino": provino}.items():
    if durata > 0:
        end_date = start_date + timedelta(days=int(durata))
        attivita_df.append({"Attività": attivita, "Inizio": start_date, "Fine": end_date, "Categoria": "Preliminari"})
        start_date = end_date

for fase, durata in fasi_produzione.items():
    end_date = start_date + timedelta(days=int((durata * num_pezzi) / ore_giornaliere))
    attivita_df.append({"Attività": fase, "Inizio": start_date, "Fine": end_date, "Categoria": "Produzione"})
    start_date = end_date

for attivita, durata in {**outsourcing, "Controllo Qualità": controllo_qualita, "Collaudo": collaudo}.items():
    end_date = start_date + timedelta(days=int(durata / ore_giornaliere))
    attivita_df.append({"Attività": attivita, "Inizio": start_date, "Fine": end_date, "Categoria": "Finalizzazione"})
    start_date = end_date

# Creazione del grafico Gantt con colori
if attivita_df:
    df_gantt = pd.DataFrame(attivita_df)
    fig = px.timeline(df_gantt, x_start="Inizio", x_end="Fine", y="Attività",
                      title="Pianificazione della Produzione", color="Categoria")
    st.plotly_chart(fig)