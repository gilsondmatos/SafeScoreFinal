# Regras de risco (comentadas em PT-BR)
from __future__ import annotations
import os, csv
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Set, Any

def get_env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

DEFAULT_WEIGHTS: Dict[str,int] = {
    "blacklist":60, "watchlist":30, "high_amount":25, "unusual_hour":15,
    "new_address":40, "velocity":20, "sensitive_token":15, "sensitive_method":15,
}

@dataclass
class RuleContext:
    data_dir: Path
    blacklist: Set[str]
    watchlist: Set[str]
    sensitive_tokens: Set[str]
    sensitive_methods: Set[str]
    known_addresses: Set[str]
    prev_transactions: List[Dict[str, Any]]

def _load_set(path: Path, col: str) -> Set[str]:
    if not path.exists(): return set()
    with path.open("r", encoding="utf-8") as f:
        return {(r.get(col) or "").strip().lower() for r in csv.DictReader(f) if r.get(col)}

def carregar_contexto(data_dir: Path, prev: List[Dict[str, Any]], known: Set[str]) -> RuleContext:
    stx = []
    for r in prev or []:
        stx.append({**r, "from_address": (r.get("from_address") or "").lower(), "timestamp": r.get("timestamp")})
    return RuleContext(
        data_dir=data_dir,
        blacklist=_load_set(data_dir / "blacklist.csv", "address"),
        watchlist=_load_set(data_dir / "watchlist.csv", "address"),
        sensitive_tokens=_load_set(data_dir / "sensitive_tokens.csv", "token"),
        sensitive_methods=_load_set(data_dir / "sensitive_methods.csv", "method"),
        known_addresses={a.lower() for a in (known or set())},
        prev_transactions=stx,
    )

def rule_blacklist(tx, ctx): 
    if (tx.get("from_address","").lower() in ctx.blacklist) or (tx.get("to_address","").lower() in ctx.blacklist):
        return True,"Endereço em blacklist"
    return False,""

def rule_watchlist(tx, ctx): 
    if (tx.get("from_address","").lower() in ctx.watchlist) or (tx.get("to_address","").lower() in ctx.watchlist):
        return True,"Endereço em watchlist"
    return False,""

def rule_high_amount(tx, ctx):
    thr = float(os.getenv("AMOUNT_THRESHOLD","10000"))
    try: val = float(tx.get("amount",0))
    except Exception: val = 0.0
    return (True,f"Valor alto (≥ {thr})") if val >= thr else (False,"")

def rule_unusual_hour(tx, ctx):
    try: ts = datetime.fromisoformat(str(tx.get("timestamp"))); 
    except Exception: return False,""
    if ts.utcoffset() is None: ts = ts.replace(tzinfo=timezone.utc)
    return (True,"Horário incomum (madrugada UTC)") if ts.astimezone(timezone.utc).hour in {0,1,2,3,4,5} else (False,"")

def rule_new_address(tx, ctx):
    de = (tx.get("from_address") or "").lower()
    return (True,"Endereço remetente não conhecido") if de and de not in ctx.known_addresses else (False,"")

def rule_velocity(tx, ctx):
    try: ts = datetime.fromisoformat(str(tx.get("timestamp")))
    except Exception: return False,""
    if ts.utcoffset() is None: ts = ts.replace(tzinfo=timezone.utc)
    jan = get_env_int("VELOCITY_WINDOW_MIN",10)
    mx  = get_env_int("VELOCITY_MAX_TX",5)
    de = (tx.get("from_address") or "").lower()
    ini = ts - timedelta(minutes=jan)
    cnt = 0
    for r in ctx.prev_transactions:
        try:
            rts = datetime.fromisoformat(str(r.get("timestamp"))); 
            if rts.utcoffset() is None: rts = rts.replace(tzinfo=timezone.utc)
        except Exception: 
            continue
        if r.get("from_address")==de and ini <= rts <= ts:
            cnt += 1
    return (True,f"Alta velocidade ({cnt} tx em {jan}min)") if cnt >= mx else (False,"")

def rule_sensitive_token(tx, ctx):
    return (True,"Token sensível") if (tx.get("token","").upper() in {t.upper() for t in ctx.sensitive_tokens}) else (False,"")

def rule_sensitive_method(tx, ctx):
    return (True,"Método sensível") if (tx.get("method","").upper() in {m.upper() for m in ctx.sensitive_methods}) else (False,"")

ORDER = [
    ("blacklist", rule_blacklist),
    ("watchlist", rule_watchlist),
    ("high_amount", rule_high_amount),
    ("unusual_hour", rule_unusual_hour),
    ("new_address", rule_new_address),
    ("velocity", rule_velocity),
    ("sensitive_token", rule_sensitive_token),
    ("sensitive_method", rule_sensitive_method),
]
