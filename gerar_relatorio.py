"""
Geração de relatório em PDF (tabela) das transações críticas (score < limiar).
Usado pelo dashboard e pode ser executado direto via `python gerar_relatorio.py`.
"""

import os
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from fpdf import FPDF  # type: ignore

DIR_DADOS = Path("app/data")
ARQ_TX = DIR_DADOS / "transactions.csv"
ARQ_SAIDA = DIR_DADOS / "relatorio.pdf"


def safe_text(t: str) -> str:
    if not t:
        return ""
    t = (
        str(t)
        .replace("—", "-").replace("–", "-").replace("…", "...")
        .replace("≥", ">=").replace("≤", "<=").replace("•", "-")
    )
    return t.encode("latin-1", "replace").decode("latin-1")


def abreviar(addr: str, n1: int = 10, n2: int = 6) -> str:
    addr = str(addr or "")
    return addr if len(addr) <= (n1 + n2 + 1) else f"{addr[:n1]}…{addr[-n2:]}"


def carregar_limiar() -> int:
    try:
        return int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        return 50


def ler_tx_csv() -> List[Dict[str, Any]]:
    if not ARQ_TX.exists():
        return []
    with ARQ_TX.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class ReportPDF(FPDF):
    def header(self):
        self.set_font("Arial", "B", 14)
        self.cell(0, 8, safe_text("Relatório SafeScore - Transações Críticas"), 0, ln=True, align="C")
        self.ln(2)

    def footer(self):
        self.set_y(-12)
        self.set_font("Arial", "I", 8)
        self.cell(0, 8, safe_text(f"Página {self.page_no()}"), 0, 0, "C")


def _cabecalho(pdf: ReportPDF, ws):
    pdf.set_fill_color(235, 235, 235)
    pdf.set_draw_color(200, 200, 200)
    pdf.set_font("Arial", "B", 10)
    for rot, w in zip(["TX", "Data/Hora", "De", "Para", "Valor", "Score", "Penal.", "Motivos"], ws):
        pdf.cell(w, 8, safe_text(rot), border=1, ln=0, align="C", fill=True)
    pdf.ln(8)


def _linha(pdf: ReportPDF, ws, r: Dict[str, Any]):
    pdf.set_font("Arial", "", 9)
    pdf.set_draw_color(220, 220, 220)
    valor = f"{r.get('amount','')} {r.get('token','')}".strip()
    motivos = safe_text(r.get("reasons", "") or "n/d")
    if len(motivos) > 120:
        motivos = motivos[:119] + "…"
    celulas = [
        safe_text(str(r.get("tx_id",""))[:20]),
        safe_text(str(r.get("timestamp",""))[:19]),
        safe_text(abreviar(r.get("from_address",""))),
        safe_text(abreviar(r.get("to_address",""))),
        safe_text(valor),
        safe_text(str(r.get("score",""))),
        safe_text(str(r.get("penalty_total",""))),
        motivos,
    ]
    for txt, w in zip(celulas, ws):
        pdf.cell(w, 8, txt, border=1)
    pdf.ln(8)


def build_pdf(rows: List[Dict[str, Any]], threshold: int) -> Path:
    DIR_DADOS.mkdir(parents=True, exist_ok=True)
    pdf = ReportPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 6, safe_text(f"Data de geração: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"), ln=True)
    pdf.cell(0, 6, safe_text(f"Limiar de alerta: score < {threshold}"), ln=True)
    pdf.cell(0, 6, safe_text(f"Total de críticos: {len(rows)}"), ln=True)
    pdf.ln(2)

    ws = [38, 30, 46, 46, 26, 16, 18, 57]  # larguras
    _cabecalho(pdf, ws)
    if not rows:
        pdf.set_font("Arial", "I", 11)
        pdf.cell(0, 10, safe_text("Nenhuma transação crítica encontrada no período."), ln=True)
    else:
        for r in rows:
            _linha(pdf, ws, r)

    try:
        pdf.output(str(ARQ_SAIDA))
        return ARQ_SAIDA
    except PermissionError:
        alt = DIR_DADOS / f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(str(alt))
        return alt


def _filtrar_criticos(txs: List[Dict[str, Any]], thr: int) -> List[Dict[str, Any]]:
    out = []
    for r in txs:
        try:
            sc = int(str(r.get("score", "0")))
        except Exception:
            sc = 0
        if sc < thr:
            r["penalty_total"] = r.get("penalty_total", 0)
            r["reasons"] = r.get("reasons", "")
            out.append(r)
    return out


def main():
    thr = carregar_limiar()
    linhas = ler_tx_csv()
    criticos = _filtrar_criticos(linhas, thr)
    caminho = build_pdf(criticos, thr)
    print(f"[OK] Relatório gerado: {caminho}")


if __name__ == "__main__":
    main()
