"""
rate_limit.py — Rate limiting anti-spam.

Regras:
- 1 mensagem a cada 5 segundos por usuário (ignorar silenciosamente)
- >10 interações em <1 segundo: spam agressivo → revoga licença
"""
import time
import logging
from collections import defaultdict
from config import RATE_MSG_INTERVALO, RATE_SPAM_AGRESSIVO

logger = logging.getLogger(__name__)

_ultimo_msg:   dict[int, float]       = defaultdict(float)
_historico_1s: dict[int, list[float]] = defaultdict(list)


def checar_rate_limit(chat_id: int) -> str:
    """
    Verifica o rate limit para um chat_id.

    Retorna:
        "ok"      — mensagem dentro do limite
        "ignorar" — ultrapassou 1 msg/5s, ignorar silenciosamente
        "spam"    — spam agressivo: >10 interações em <1s
    """
    agora = time.time()

    # Spam agressivo (>RATE_SPAM_AGRESSIVO em <1 segundo)
    hist = _historico_1s[chat_id]
    hist.append(agora)
    _historico_1s[chat_id] = [t for t in hist if agora - t < 1.0]

    if len(_historico_1s[chat_id]) > RATE_SPAM_AGRESSIVO:
        logger.warning("Spam agressivo detectado. chat_id=%d interações=%d",
                       chat_id, len(_historico_1s[chat_id]))
        return "spam"

    # Intervalo mínimo (1 msg / RATE_MSG_INTERVALO segundos)
    ultimo = _ultimo_msg[chat_id]
    if agora - ultimo < RATE_MSG_INTERVALO:
        return "ignorar"

    _ultimo_msg[chat_id] = agora
    return "ok"


def resetar_rate(chat_id: int):
    """Reseta o rate limit de um usuário (ex: após revogar licença)."""
    _ultimo_msg.pop(chat_id, None)
    _historico_1s.pop(chat_id, None)
