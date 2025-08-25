# Engine de pontuação (explicável)
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Set
from app.engine.rules import RuleContext, DEFAULT_WEIGHTS, carregar_contexto, ORDER

class ScoreEngine:
    def __init__(self, data_dir: str, prev_transactions: List[Dict[str, Any]], known_addresses: Set[str]):
        self.dir = Path(data_dir)
        self.prev = prev_transactions or []
        self.known = known_addresses or set()
        self.ctx: RuleContext = carregar_contexto(self.dir, self.prev, self.known)
        self.weights: Dict[str,int] = dict(DEFAULT_WEIGHTS)
        pesos_json = self.dir / "weights.json"
        if pesos_json.exists():
            try: self.weights.update(json.loads(pesos_json.read_text(encoding="utf-8")))
            except Exception: pass

    def score_transaction(self, tx: Dict[str, Any]) -> Dict[str, Any]:
        hits: Dict[str,int] = {}
        reasons: List[str] = []
        vel_info = 0
        for name, fn in ORDER:
            try:
                fired, reason = fn(tx, self.ctx)
            except Exception:
                fired, reason = False, ""
            if fired:
                peso = int(self.weights.get(name, DEFAULT_WEIGHTS.get(name, 0)))
                if peso > 0: hits[name] = peso
                if reason: reasons.append(reason)
                if name == "velocity": vel_info += 1
        penalty = int(sum(hits.values()))
        score = max(0, 100 - penalty)
        return {"score": int(score), "reasons": reasons, "hits": hits, "velocity_last_window": vel_info}
