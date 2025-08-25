import os
import sys
import json
from pathlib import Path
from typing import List, Dict

import pandas as pd
import streamlit as st
import altair as alt

# ---------------------- Config de página ----------------------
st.set_page_config(page_title="SafeScore Dashboard", layout="wide")

# ---------------------- Resolução robusta do app/data ----------------------
def _possiveis_data_dirs() -> List[Path]:
    """Candidatos mais comuns de onde fica a pasta de dados no deploy/local."""
    here = Path(__file__).resolve()
    return [
        # /app/dashboard/app.py  -> /app/data
        here.parents[1] / "data",
        # repo_root/app/data (quando app/dashboard está dois níveis abaixo)
        here.parents[2] / "app" / "data",
        # caminhos relativos (fallbacks)
        Path("app/data").resolve(),
        Path("app").resolve() / "data",
    ]

def _resolver_data_dir() -> Path:
    for p in _possiveis_data_dirs():
        if p.exists() and p.is_dir():
            return p
    # Se nada existir, criamos o melhor candidato (primeiro da lista)
    p0 = _possiveis_data_dirs()[0]
    p0.mkdir(parents=True, exist_ok=True)
    return p0

DATA_DIR = _resolver_data_dir()

# Garantir que o diretório raiz do repo esteja no sys.path para imports como gerar_relatorio / main
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ---------------------- Utilidades ----------------------
def lazy_import_relatorio():
    """Importa gerar_relatorio.py sob demanda (evita custo no load do app)."""
    import importlib
    return importlib.import_module("gerar_relatorio")

def load_threshold() -> int:
    try:
        return int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        return 50

def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def list_transaction_files() -> List[Path]:
    # Busca somente os CSVs de transações padronizados
    return sorted([p for p in DATA_DIR.glob("transactions*.csv") if p.is_file()])

def parse_contrib_dict(explain_str: str) -> Dict[str, float]:
    try:
        data = json.loads(explain_str or "{}")
        return {k: float(v) for k, v in (data.get("contrib_pct") or {}).items()}
    except Exception:
        return {}

# ---------------------- Cabeçalho ----------------------
st.title("🔎 SafeScore — Dashboard")
st.caption(f"Diretório de dados: {DATA_DIR}")

# ---------------------- Ações (coleta on-chain) ----------------------
col_run, _ = st.columns([1, 4])
with col_run:
    if st.button("⚡ Coletar agora (ETH)", help="Executa o main.py com coletor 'eth' e atualiza os CSVs"):
        try:
            os.environ["COLLECTOR"] = "eth"  # garante coletor
            # Import preguiçoso do main para não pesar o load do app
            import importlib
            main_mod = importlib.import_module("main")
            # Executa pipeline completa (coletar → pontuar → salvar CSV/pendências/alertas)
            main_mod.main()
            st.success("Coleta executada com sucesso. Recarregando arquivos...")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"Falha ao coletar dados on-chain: {e}")

# ---------------------- Seleção de arquivo ----------------------
files = list_transaction_files()

# Diagnóstico quando a pasta está vazia no deploy
if not files:
    with st.container():
        st.info("Nenhum CSV encontrado em app/data. Rode `python main.py` localmente ou use o botão acima para gerar os dados.")
        # Informação extra de debug para saber o que o app está enxergando
        st.caption("Diagnóstico rápido (arquivos visíveis neste diretório):")
        try:
            st.code("\n".join([str(p) for p in DATA_DIR.glob('*.csv')]) or "(vazio)")
        except Exception:
            st.code("(não foi possível listar)")
    st.stop()

default_idx = len(files) - 1
sel = st.selectbox(
    "Arquivo de transações",
    options=files,
    index=default_idx,
    format_func=lambda p: p.name
)
df = load_df(sel)

# ---------------------- Parâmetros e filtros ----------------------
threshold = load_threshold()
st.sidebar.header("Filtros")

# Parâmetros (limiar)
st.sidebar.markdown("### Parâmetros")
threshold = st.sidebar.number_input(
    "Limiar de alerta (score < x)", min_value=0, max_value=100, value=threshold, step=1
)

# Filtros
tokens = ["(todos)"] + sorted(df.get("token", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
token_sel = st.sidebar.selectbox("Token", tokens, index=0)

addr_query = st.sidebar.text_input("Filtro por endereço (contém)", "")
score_min, score_max = st.sidebar.slider("Faixa de score", 0, 100, (0, 100))
show_explain = st.sidebar.checkbox("Mostrar contribuição por regra (%)", value=True)

filtered = df.copy()
if token_sel != "(todos)":
    filtered = filtered[filtered["token"].astype(str) == token_sel]

if addr_query.strip():
    q = addr_query.strip().lower()
    filtered = filtered[
        filtered["from_address"].astype(str).str.lower().str.contains(q) |
        filtered["to_address"].astype(str).str.lower().str.contains(q)
    ]

filtered = filtered[(filtered["score"] >= score_min) & (filtered["score"] <= score_max)]

# ---------------------- KPIs ----------------------
total = int(filtered.shape[0]) if not filtered.empty else 0
avg_score = float(filtered["score"].mean()) if total else 0.0
criticos = int((filtered["score"] < threshold).sum()) if total else 0

k1, k2, k3 = st.columns(3)
k1.metric("Transações (filtro)", f"{total}")
k2.metric("Média de score", f"{avg_score:.1f}")
k3.metric("Críticas (< limiar)", f"{criticos}")

# ---------------------- Gráfico ----------------------
st.subheader("Distribuição por token")
try:
    # Conta por token (somente linhas do filtro)
    g = filtered.groupby("token", dropna=False)["tx_id"].count().reset_index()
    g.columns = ["token", "count"]
    chart = (
        alt.Chart(g)
        .mark_bar()
        .encode(x=alt.X("token:N", sort="-y", title="Token"),
                y=alt.Y("count:Q", title="Transações"))
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write("(Sem dados suficientes para gráfico)")

# ---------------------- Tabela principal ----------------------
st.subheader("Transações")

df_show = filtered.copy()
if show_explain and "explain" in df_show.columns:
    contrib_series = df_show["explain"].apply(parse_contrib_dict)
    all_rules = sorted({rk for d in contrib_series for rk in d.keys()})
    for rk in all_rules:
        df_show[f"{rk}_%"] = contrib_series.apply(lambda d: d.get(rk, 0.0))
    base_cols = [
        "tx_id","timestamp","from_address","to_address","amount","token","method",
        "chain","score","penalty_total","reasons"
    ]
    cols = [c for c in base_cols if c in df_show.columns] + [f"{rk}_%" for rk in all_rules]
    st.dataframe(df_show[cols].sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)
else:
    if show_explain and "explain" not in df_show.columns:
        st.info("Arquivo sem coluna 'explain'. Rode `python main.py` novamente para gerar explicações por regra.")
    st.dataframe(df_show.sort_values(by="timestamp", ascending=False), use_container_width=True, height=420)

# ---------------------- Inspector por transação ----------------------
st.markdown("### Explicabilidade (por transação)")
if not filtered.empty:
    tx_ids = filtered["tx_id"].astype(str).tolist()
    sel_tx = st.selectbox("Selecione a transação", options=tx_ids, index=0)
    row = filtered[filtered["tx_id"].astype(str) == sel_tx].iloc[0]
    st.write(f"Score: **{int(row['score'])}** — Penalidade total: **{int(row.get('penalty_total',0))}**")
    # JSON completo (weights + contrib)
    try:
        st.json(json.loads(row.get("explain", "{}")))
    except Exception:
        st.write("(explain inválido)")

    # Barras horizontais de contribuição
    contrib = parse_contrib_dict(row.get("explain", "{}"))
    if contrib:
        df_bar = pd.DataFrame({"regra": list(contrib.keys()), "pct": list(contrib.values())})
        chart = (
            alt.Chart(df_bar)
            .mark_bar()
            .encode(x=alt.X("pct:Q", title="Contribuição (%)"),
                    y=alt.Y("regra:N", sort="-x", title="Regra"))
            .properties(height=200)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Sem contribuições (nenhuma penalidade).")

# ---------------------- Fila de retenção ----------------------
pending_path = DATA_DIR / "pending_review.csv"
pending = load_df(pending_path)
with st.expander("Fila de retenção (pending_review)", expanded=False):
    if pending.empty:
        st.write("Sem itens na fila.")
    else:
        st.dataframe(pending, use_container_width=True)

# ---------------------- Relatório em PDF ----------------------
st.markdown("### Relatório (PDF)")
st.caption("Gera PDF das transações críticas (score < limiar atual) usando o arquivo selecionado.")

def _rows_criticos(df_in: pd.DataFrame, thr: int):
    if df_in.empty:
        return []
    return df_in[df_in["score"] < thr].to_dict(orient="records")

col_pdf1, col_pdf2 = st.columns([1, 3])
with col_pdf1:
    if st.button("Gerar PDF agora", type="primary"):
        try:
            gr = lazy_import_relatorio()
            os.environ["SCORE_ALERT_THRESHOLD"] = str(threshold)
            rows = _rows_criticos(filtered, threshold)
            path = gr.build_pdf(rows=rows, threshold=threshold)
            st.session_state["last_pdf_path"] = str(path)
            st.success(f"Relatório gerado: {path}")
        except Exception as e:
            st.error(f"Falha ao gerar PDF: {e}")

last_pdf = st.session_state.get("last_pdf_path")
with col_pdf2:
    if last_pdf and Path(last_pdf).exists():
        with open(last_pdf, "rb") as f:
            st.download_button(
                label=f"Baixar PDF ({Path(last_pdf).name})",
                data=f.read(),
                file_name=Path(last_pdf).name,
                mime="application/pdf",
            )
    else:
        st.info("Nenhum PDF gerado nesta sessão.")

# ---------------------- Exportação JSONL ----------------------
st.markdown("### Exportação (JSONL)")
export_name = f"{sel.stem}_filtered.jsonl"
if st.button("Gerar JSONL (filtro atual)"):
    out_recs = []
    for _, r in filtered.iterrows():
        rec = r.to_dict()
        try:
            exp = json.loads(rec.get("explain","{}"))
        except Exception:
            exp = {}
        rec["explain"] = exp
        out_recs.append(rec)
    jsonl = "\n".join(json.dumps(x, ensure_ascii=False) for x in out_recs).encode("utf-8")
    st.download_button("Baixar JSONL", data=jsonl, file_name=export_name, mime="application/json")

st.caption("© SafeScore — Challenge FIAP × TecBan")