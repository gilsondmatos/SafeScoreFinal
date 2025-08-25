"""
Dashboard do SafeScore (funcional, estável para Streamlit Cloud).
- NÃO aciona coleta on-chain aqui (evita problemas de set_page_config/calls).
- Consome CSV gerado por `python main.py`.
- KPIs, filtros, explicabilidade por regra, PDF e exportação JSONL.
"""

import os
import sys
import json
from pathlib import Path
from typing import List, Dict

import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(page_title="SafeScore Dashboard", layout="wide")

# --- paths ---
THIS_FILE = Path(__file__).resolve()
ROOT = THIS_FILE.parents[2]            # <repo_root>
DATA_DIR = ROOT / "app" / "data"

# GARANTE QUE O ROOT ESTEJA NO sys.path (necessário para importar gerar_relatorio.py)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def load_threshold() -> int:
    try:
        return int(os.getenv("SCORE_ALERT_THRESHOLD","50"))
    except Exception:
        return 50

def list_csvs() -> List[Path]:
    return sorted(DATA_DIR.glob("transactions*.csv"))

def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    # normaliza tipos
    for c in ("amount","score","penalty_total","velocity_last_window"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df

def parse_contrib(explain: str) -> Dict[str, float]:
    try:
        return {k: float(v) for k, v in (json.loads(explain or "{}").get("contrib_pct") or {}).items()}
    except Exception:
        return {}

st.title("🔎 SafeScore — Dashboard")
st.caption(f"Diretório de dados: {DATA_DIR}")

files = list_csvs()
if not files:
    st.info("Nenhum CSV encontrado em app/data. Rode `python main.py` localmente para gerar os dados.")
    st.stop()

sel = st.selectbox("Arquivo de transações", options=files, index=len(files)-1, format_func=lambda p: p.name)
df_all = load_df(sel)

# --------- Filtros ---------
st.sidebar.header("Filtros")
tokens = ["(todos)"] + sorted([t for t in df_all.get("token", pd.Series(dtype=str)).dropna().unique().tolist()])
tok = st.sidebar.selectbox("Token", tokens, index=0)
addr = st.sidebar.text_input("Filtro por endereço (contém)")
score_min, score_max = st.sidebar.slider("Faixa de score", 0, 100, (0, 100))
show_explain = st.sidebar.checkbox("Mostrar contribuição por regra (%)", value=True)

# --------- Parâmetros ---------
thr = load_threshold()
st.sidebar.markdown("### Parâmetros")
thr = st.sidebar.number_input("Limiar de alerta (score < x)", min_value=0, max_value=100, value=thr, step=1)

# aplica filtros
df = df_all.copy()
if tok != "(todos)":
    df = df[df["token"] == tok]
if addr.strip():
    q = addr.strip().lower()
    df = df[
        df["from_address"].astype(str).str.lower().str.contains(q) |
        df["to_address"].astype(str).str.lower().str.contains(q)
    ]
df = df[(df["score"] >= score_min) & (df["score"] <= score_max)]

# --------- KPIs ---------
c1,c2,c3 = st.columns(3)
c1.metric("Transações (filtro)", f"{len(df)}")
c2.metric("Média de score", f"{(df['score'].mean() if not df.empty else 0):.1f}")
c3.metric("Críticas (< limiar)", f"{int((df['score'] < thr).sum()) if not df.empty else 0}")

# --------- Gráfico ---------
st.subheader("Distribuição por token")
try:
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(x=alt.X("token:N", sort="-y", title="Token"), y=alt.Y("count():Q", title="Quantidade"))
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write("(Sem dados para gráfico)")

# --------- Tabela ---------
st.subheader("Transações")
df_show = df.copy()
if show_explain and "explain" in df_show.columns:
    contrib = df_show["explain"].apply(parse_contrib)
    rules = sorted({k for d in contrib for k in d.keys()})
    for rk in rules:
        df_show[f"{rk}_%"] = contrib.apply(lambda d: d.get(rk, 0.0))
    base = ["tx_id","timestamp","from_address","to_address","amount","token","method","chain","score","penalty_total","reasons"]
    cols = [c for c in base if c in df_show.columns] + [f"{rk}_%" for rk in rules]
    st.dataframe(df_show[cols].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)
else:
    st.dataframe(df_show.sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)

# --------- Explicabilidade por transação ---------
st.markdown("### Explicabilidade (por transação)")
if not df.empty:
    tx_ids = df["tx_id"].astype(str).tolist()
    sel_tx = st.selectbox("Transação", options=tx_ids, index=0)
    row = df[df["tx_id"].astype(str) == sel_tx].iloc[0]
    st.write(f"Score: **{int(row['score'])}** — Penalidade total: **{int(row.get('penalty_total',0))}**")
    st.json(json.loads(row.get("explain","{}")))
    cdict = parse_contrib(row.get("explain","{}"))
    if cdict:
        dbar = pd.DataFrame({"regra": list(cdict.keys()), "pct": list(cdict.values())})
        st.altair_chart(
            alt.Chart(dbar).mark_bar().encode(
                x=alt.X("pct:Q", title="Contribuição (%)"),
                y=alt.Y("regra:N", sort="-x", title="Regra")
            ).properties(height=200),
            use_container_width=True
        )
    else:
        st.info("Sem contribuições (nenhuma penalidade).")

# --------- Relatório (PDF) ---------
st.markdown("### Relatório (PDF)")
st.caption("Gera PDF das transações críticas (score < limiar atual) usando o arquivo selecionado.")
try:
    from importlib import import_module
    rel = import_module("gerar_relatorio")  # lazy import a partir do repo root
except Exception as e:
    st.error(f"Não foi possível importar o módulo de relatório. Verifique se 'gerar_relatorio.py' está na raiz do projeto. Detalhes: {e}")
    rel = None

if rel and st.button("Gerar PDF agora"):
    rows = df[df["score"] < thr].to_dict(orient="records")
    os.environ["SCORE_ALERT_THRESHOLD"] = str(thr)  # usado dentro do script de relatório
    path = rel.build_pdf(rows, thr)
    st.success(f"Relatório gerado: {path}")
    try:
        with open(path, "rb") as f:
            st.download_button("Baixar PDF", data=f.read(), file_name=Path(path).name, mime="application/pdf")
    except Exception as e:
        st.warning(f"PDF gerado, mas não foi possível anexar para download ({e}). Baixe em: {path}")

# --------- Export JSONL ---------
st.markdown("### Exportação (JSONL)")
if st.button("Gerar JSONL (filtro atual)"):
    out = []
    for _, r in df.iterrows():
        d = r.to_dict()
        try:
            d["explain"] = json.loads(d.get("explain","{}"))
        except Exception:
            d["explain"] = {}
        out.append(d)
    payload = "\n".join(json.dumps(x, ensure_ascii=False) for x in out).encode("utf-8")
    st.download_button("Baixar JSONL", data=payload, file_name=f"{sel.stem}_filtered.jsonl", mime="application/json")

st.caption("© SafeScore — Challenge FIAP × TecBan")
