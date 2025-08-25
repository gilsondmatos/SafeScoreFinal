# -*- coding: utf-8 -*-
"""
SafeScore - Pipeline principal
coletar → pontuar → salvar CSV → alertar → (opcional) loop automático

NOVIDADES:
- Modo contínuo (--daemon): coleta em intervalos fixos, aplicando scoring e salvando.
- Monitoramento por carteiras:
    * --monitor-watchlist         → usa endereços do app/data/watchlist.csv
    * --monitor-addresses=...     → lista adicional via CLI
    * ETH_MONITOR_ADDRESSES=...   → lista adicional via variável de ambiente
    * --require-match             → só processa transações que envolvam os endereços monitorados
- Deduplicação por tx_id (não regrava o que já foi processado).
- Mantém histórico em memória para regra de VELOCIDADE.

Obs.: O dashboard (Streamlit) continua apenas LENDO os CSVs produzidos.
"""

import os
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple

# Carrega .env se existir
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from app.engine.scoring import ScoreEngine
from app.alerts.telegram import TelegramAlerter
from app.collectors.mock_collector import load_input_or_mock

def try_load_eth_collector():
    """Import lazy do coletor ETH (evita falha quando usuário usa mock)."""
    try:
        from app.collectors.eth_collector import load_from_eth  # type: ignore
        return load_from_eth
    except Exception:
        return None

# ---------------------- Caminhos ---------------------- #
DATA_DIR = Path("app/data")
TX_CSV = DATA_DIR / "transactions.csv"
DAY_FMT = "transactions_%Y%m%d.csv"
PENDING_CSV = DATA_DIR / "pending_review.csv"
KNOWN_CSV = DATA_DIR / "known_addresses.csv"
WATCHLIST_CSV = DATA_DIR / "watchlist.csv"

# ---------------------- Utilidades ---------------------- #
def safe_text(text: str) -> str:
    """Sanitiza strings para evitar problemas de PDF/CSV com latin-1."""
    if text is None:
        return ""
    t = str(text)
    t = (
        t.replace("—", "-").replace("–", "-").replace("…", "...")
         .replace("≥", ">=").replace("≤", "<=").replace("•", "-")
    )
    return t.encode("latin-1", "replace").decode("latin-1")

def ensure_data_files():
    """Garante a estrutura mínima de dados/listas (idempotente)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # known_addresses
    if not KNOWN_CSV.exists():
        with KNOWN_CSV.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=["address","first_seen"]).writeheader()
    # listas base (se foram apagadas)
    def _ensure(path: Path, header: list[str], rows: list[Dict[str, Any]] | None = None):
        if path.exists(): return
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for r in (rows or []): w.writerow(r)
    _ensure(DATA_DIR / "blacklist.csv", ["address","reason"], [])
    _ensure(WATCHLIST_CSV, ["address","note"], [])
    _ensure(DATA_DIR / "sensitive_tokens.csv", ["token"], [])
    _ensure(DATA_DIR / "sensitive_methods.csv", ["method"], [])

def read_known_addresses() -> Set[str]:
    if not KNOWN_CSV.exists(): return set()
    with KNOWN_CSV.open("r", encoding="utf-8") as f:
        return {row["address"].strip().lower() for row in csv.DictReader(f) if row.get("address")}

def append_known_address(addr: str):
    addr = (addr or "").strip().lower()
    if not addr: return
    known = read_known_addresses()
    if addr in known: return
    with KNOWN_CSV.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=["address","first_seen"]).writerow(
            {"address": addr, "first_seen": datetime.now(timezone.utc).isoformat()}
        )

def read_prev_transactions() -> List[Dict[str, Any]]:
    if not TX_CSV.exists(): return []
    with TX_CSV.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def load_seen_tx_ids() -> Set[str]:
    """Carrega conjunto de tx_ids já processados (dedupe)."""
    if not TX_CSV.exists(): return set()
    with TX_CSV.open("r", encoding="utf-8") as f:
        return {row.get("tx_id","") for row in csv.DictReader(f)}

def _write_rows_to(path: Path, rows: List[Dict[str, Any]], header: list[str]):
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if new_file: w.writeheader()
        w.writerows(rows)

def write_transactions(rows: List[Dict[str, Any]]):
    header = [
        "tx_id","timestamp","from_address","to_address","amount","token","method","chain",
        "is_new_address","velocity_last_window","score","penalty_total","reasons","explain"
    ]
    if not rows: return
    _write_rows_to(TX_CSV, rows, header)
    day_file = DATA_DIR / datetime.now().strftime(DAY_FMT)
    _write_rows_to(day_file, rows, header)

def append_pending(rows: List[Dict[str, Any]]):
    if not rows: return
    header = [
        "tx_id","timestamp","from_address","to_address","amount","token","method","chain",
        "score","penalty_total","reasons","explain"
    ]
    pend = []
    for r in rows:
        pend.append({
            "tx_id": r["tx_id"],
            "timestamp": r["timestamp"],
            "from_address": r["from_address"],
            "to_address": r["to_address"],
            "amount": r["amount"],
            "token": r["token"],
            "method": r["method"],
            "chain": r["chain"],
            "score": r["score"],
            "penalty_total": r.get("penalty_total", 0),
            "reasons": r["reasons"],
            "explain": r.get("explain","{}"),
        })
    _write_rows_to(PENDING_CSV, pend, header)

def abbreviate(addr: str) -> str:
    if not addr: return ""
    return addr if len(addr) <= 10 else f"{addr[:6]}…{addr[-4:]}"

# ---------------------- Monitoramento de endereços ---------------------- #
def load_watch_addresses(from_watchlist: bool, extra_cli: str | None, extra_env: str | None) -> Set[str]:
    """
    Retorna o conjunto de endereços a monitorar (lowercase).
    - from_watchlist: lê app/data/watchlist.csv (coluna 'address').
    - extra_cli/extra_env: listas comma-separated adicionais.
    """
    addrs: Set[str] = set()

    def _split_list(s: str) -> List[str]:
        return [x.strip().lower() for x in s.split(",") if x.strip()]

    if from_watchlist and WATCHLIST_CSV.exists():
        with WATCHLIST_CSV.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                a = (row.get("address") or "").strip().lower()
                if a: addrs.add(a)

    if extra_env:
        for a in _split_list(extra_env):
            addrs.add(a)

    if extra_cli:
        for a in _split_list(extra_cli):
            addrs.add(a)

    return addrs

def filter_by_addresses(txs: List[Dict[str, Any]], watch: Set[str], require_match: bool) -> List[Dict[str, Any]]:
    """
    Se require_match=True, retorna apenas transações que envolvam 'watch'.
    Caso contrário:
      - Se watch estiver vazio → retorna todas.
      - Se watch tiver itens → retorna todas (para manter visão geral), mas a watchlist pesará no score.
    """
    if not require_match or not watch:
        return txs
    out = []
    for tx in txs:
        fr = (tx.get("from_address") or "").lower()
        to = (tx.get("to_address") or "").lower()
        if fr in watch or to in watch:
            out.append(tx)
    return out

def escolher_coletor() -> str:
    """Lê coletor de CLI/env. Padrão: mock."""
    for i, a in enumerate(sys.argv):
        if a in ("--collector", "-c") and i + 1 < len(sys.argv):
            return sys.argv[i + 1].strip().lower()
        if a.startswith("--collector="):
            return a.split("=", 1)[1].strip().lower()
    return os.getenv("COLLECTOR", "mock").strip().lower()

def parse_flag(name: str) -> bool:
    """Retorna True se a flag (ex.: --daemon) estiver presente na CLI."""
    return any(arg == f"--{name}" for arg in sys.argv)

def parse_kv(name: str, default: str | None = None) -> str | None:
    """Lê parâmetro estilo --interval=15; se ausente, retorna default."""
    for a in sys.argv:
        if a.startswith(f"--{name}="):
            return a.split("=", 1)[1]
    return default

# ---------------------- Execução de 1 passada ---------------------- #
def run_once(collector: str,
             engine: ScoreEngine,
             alerter: TelegramAlerter,
             limiar: int,
             chain_label: str,
             seen: Set[str],
             watch: Set[str],
             require_match: bool) -> Tuple[int, int]:
    """
    Executa UMA coleta + scoring + persistência.
    Retorna (novas_transacoes_persistidas, qtd_pendentes_geradas)
    """
    # 1) Coleta
    if collector == "eth":
        load_from_eth = try_load_eth_collector()
        if load_from_eth is None:
            print("[WARN] Coletor ETH indisponível. Usando mock.")
            txs = load_input_or_mock(DATA_DIR)
        else:
            txs = load_from_eth(DATA_DIR) or []
            if not txs:
                print("[WARN] Coletor ETH não retornou dados. Usando mock.")
                txs = load_input_or_mock(DATA_DIR)
    else:
        txs = load_input_or_mock(DATA_DIR)

    # 2) Filtro por carteiras monitoradas (se requerido)
    txs = filter_by_addresses(txs, watch, require_match)

    # 3) Dedup por tx_id
    txs = [t for t in txs if t.get("tx_id") and t["tx_id"] not in seen]
    if not txs:
        return (0, 0)

    # 4) Scoring e explicabilidade
    out_rows: List[Dict[str, Any]] = []
    pendings: List[Dict[str, Any]] = []

    for tx in txs:
        scored = engine.score_transaction(tx)
        hits: Dict[str, int] = scored["hits"] or {}
        penalty_total = int(sum(hits.values()))
        contrib_pct = {k: round((v / penalty_total) * 100, 1) for k, v in hits.items()} if penalty_total > 0 else {}
        explain_payload = {"weights": hits, "contrib_pct": contrib_pct}

        reasons_txt = safe_text("; ".join(scored["reasons"]) if scored["reasons"] else "")
        row = {
            "tx_id": tx.get("tx_id",""),
            "timestamp": tx.get("timestamp",""),
            "from_address": (tx.get("from_address","") or "").lower(),
            "to_address": (tx.get("to_address","") or "").lower(),
            "amount": tx.get("amount",0),
            "token": tx.get("token",""),
            "method": tx.get("method",""),
            "chain": chain_label,
            "is_new_address": "yes" if hits.get("new_address") else "no",
            "velocity_last_window": scored.get("velocity_last_window", 0),
            "score": scored["score"],
            "penalty_total": penalty_total,
            "reasons": reasons_txt,
            "explain": json.dumps(explain_payload, ensure_ascii=False),
        }
        out_rows.append(row)

        # Atualiza base de conhecidos
        if hits.get("new_address"):
            append_known_address(tx.get("from_address",""))

        # Alerta se crítico
        if row["score"] < limiar:
            pendings.append(row)
            msg = (
                f"🚨 SafeScore ALERTA\n"
                f"TX: {row['tx_id']}\n"
                f"Score: {row['score']} (< {limiar})\n"
                f"De: {abbreviate(row['from_address'])}\n"
                f"Para: {abbreviate(row['to_address'])}\n"
                f"Valor: {row['amount']} {row['token']}\n"
                f"Motivos: {row['reasons'] or 'n/d'}"
            )
            alerter.send(msg)

    # 5) Persistência
    write_transactions(out_rows)
    if pendings:
        append_pending(pendings)

    # 6) Atualiza 'seen' (dedupe) e histórico para VELOCIDADE
    seen.update(r["tx_id"] for r in out_rows if r.get("tx_id"))
    # Acrescenta ao histórico na memória (para próxima janela de velocidade)
    engine.prev.extend(out_rows)

    return (len(out_rows), len(pendings))

# ---------------------- CLI ---------------------- #
def main():
    ensure_data_files()

    try:
        limiar = int(os.getenv("SCORE_ALERT_THRESHOLD", "50"))
    except Exception:
        limiar = 50

    collector = escolher_coletor()
    chain_label = os.getenv("CHAIN_NAME", "ETH") if collector == "eth" else "MOCK"

    # Engine/alertas
    prev = read_prev_transactions()
    known = read_known_addresses()
    engine = ScoreEngine(data_dir=str(DATA_DIR), prev_transactions=prev, known_addresses=known)
    alerter = TelegramAlerter.from_env()

    # Monitoramento/endereços
    from_watchlist = parse_flag("monitor-watchlist")
    extra_cli = parse_kv("monitor-addresses")
    extra_env = os.getenv("ETH_MONITOR_ADDRESSES")
    require_match = parse_flag("require-match")
    watch = load_watch_addresses(from_watchlist, extra_cli, extra_env)
    if watch:
        print(f"[INFO] Monitorando {len(watch)} endereço(s). "
              f"{'Processando apenas matches (--require-match ativado).' if require_match else 'Processando tudo e usando watchlist para score.'}")

    # Modo Daemon?
    daemon = parse_flag("daemon")
    interval_s = int(parse_kv("interval", os.getenv("POLL_SECONDS", "20")) or "20")

    # Dedup inicial
    seen = load_seen_tx_ids()

    if not daemon:
        new_rows, new_pend = run_once(collector, engine, alerter, limiar, chain_label, seen, watch, require_match)
        print(f"[OK] Coletor '{collector}': {new_rows} nova(s) transação(ões) persistida(s).")
        if new_pend:
            print(f"[HOLD] {new_pend} transação(ões) crítica(s) adicionada(s) a {PENDING_CSV.name} (score < {limiar}).")
        else:
            print("[HOLD] Nenhuma transação crítica para retenção.")
        return

    # Loop contínuo
    print(f"[DAEMON] Iniciando loop: coletor='{collector}', intervalo={interval_s}s. "
          f"{'(somente matches)' if require_match else '(todas as tx, com score em watchlist se aplicável)'}")
    try:
        while True:
            start = time.time()
            new_rows, new_pend = run_once(collector, engine, alerter, limiar, chain_label, seen, watch, require_match)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] Tick: +{new_rows} novas, +{new_pend} críticas. Total seen={len(seen)}.")
            # Dorme o restante do período
            elapsed = time.time() - start
            sleep_for = max(1.0, interval_s - elapsed)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\n[DAEMON] Encerrado pelo usuário.")

if __name__ == "__main__":
    main()
