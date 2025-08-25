"""
Alerta via Telegram (best-effort).
Não derruba o pipeline se falhar (sem token/chat, rede indisponível, etc).
"""

from __future__ import annotations
import os
import requests  # type: ignore

class TelegramAlerter:
    def __init__(self, token: str | None, chat_id: str | None):
        self.token = (token or "").strip()
        self.chat_id = (chat_id or "").strip()

    @classmethod
    def from_env(cls) -> "TelegramAlerter":
        return cls(os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"))

    def send(self, text: str) -> None:
        if not self.token or not self.chat_id:
            return  # silencioso
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": text},
                timeout=10,
            )
        except Exception:
            pass
