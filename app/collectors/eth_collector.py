# (igual ao enviado anteriormente, com comentários PT-BR)
# Coletor ETH via JSON-RPC simples
from __future__ import annotations
import os, time, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import requests

def _to_int(h: str) -> int:
    try: return int(h, 16)
    except Exception: return 0

def _eth_to_float_wei(wei_hex: str) -> float:
    try: return int(wei_hex, 16) / 10**18
    except Exception: return 0.0

def _rpc(url: str, method: str, params: list) -> Any:
    try:
        r = requests.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=15)
        if r.status_code == 429: time.sleep(0.5)
        r.raise_for_status()
        js = r.json()
        if "error" in js: raise RuntimeError(js["error"])
        return js.get("result")
    except Exception as e:
        raise RuntimeError(f"RPC error {method}: {e}")

def _urls() -> List[str]:
    urls = os.getenv("ETH_RPC_URL","")
    urls = ",".join([u.strip() for u in urls.split(",") if u.strip()])
    if not urls:
        urls = "https://ethereum.publicnode.com,https://eth.llamarpc.com,https://cloudflare-eth.com"
    return [u.strip() for u in urls.split(",") if u.strip()]

def _choose_url() -> Optional[str]:
    for u in _urls():
        try:
            _rpc(u,"eth_blockNumber",[])
            return u
        except Exception:
            continue
    return None

def load_from_eth(data_dir: Path) -> List[Dict[str, Any]]:
    url = _choose_url()
    if not url:
        print("[WARN] RPC ETH indisponível.")
        return []

    blocks_back = int(os.getenv("ETH_BLOCKS_BACK","20"))
    max_tx = int(os.getenv("ETH_MAX_TX","50"))
    val_min = float(os.getenv("ETH_INCLUDE_ETH_VALUE_MIN","0"))

    latest = _to_int(_rpc(url,"eth_blockNumber",[]))
    out: List[Dict[str,Any]] = []

    for n in range(latest, max(latest - blocks_back, -1), -1):
        if len(out) >= max_tx: break
        blk = _rpc(url,"eth_getBlockByNumber",[hex(n), True]) or {}
        ts_iso = datetime.fromtimestamp(_to_int(blk.get("timestamp","0x0")), tz=timezone.utc).isoformat()
        for tx in blk.get("transactions",[]) or []:
            if len(out) >= max_tx: break
            amount = _eth_to_float_wei(tx.get("value","0x0"))
            if amount < val_min: continue
            out.append({
                "tx_id": tx.get("hash",""),
                "timestamp": ts_iso,
                "from_address": (tx.get("from") or "").lower(),
                "to_address": (tx.get("to") or "").lower(),
                "amount": round(amount,8),
                "token": "ETH",
                "method": "TRANSFER" if amount>0 else "CALL",
                "chain": "ETH",
            })
    return out
