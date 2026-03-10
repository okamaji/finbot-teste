"""
middleware.py — Verificação de licença, cache, termos e rate limit.
"""
import time
import threading
import logging
from cachetools import TTLCache
from telegram import Update
from telegram.constants import ParseMode
from database import (db_verificar_licenca, db_verificar_termos,
                      db_log_spam, db_revogar_licenca)
from config import CACHE_TTL, CACHE_MAXSIZE, ESTADOS_TTL_MINUTOS
from rate_limit import checar_rate_limit, resetar_rate

logger = logging.getLogger(__name__)

_cache: TTLCache = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL)
_cache_lock = threading.Lock()


def _cache_get(chat_id: int) -> str | None:
    with _cache_lock:
        return _cache.get(chat_id)


def _cache_set(chat_id: int, status: str):
    with _cache_lock:
        _cache[chat_id] = status


def cache_invalidar(chat_id: int):
    with _cache_lock:
        _cache.pop(chat_id, None)


def estado_novo(dados: dict) -> dict:
    return {**dados, "_ts": time.time()}


def limpar_estados_expirados(estados: dict) -> int:
    agora     = time.time()
    ttl       = ESTADOS_TTL_MINUTOS * 60
    expirados = [
        cid for cid, est in list(estados.items())
        if agora - est.get("_ts", agora) > ttl
    ]
    for cid in expirados:
        estados.pop(cid, None)
    if expirados:
        logger.info("Estados expirados removidos: %d", len(expirados))
    return len(expirados)


def get_chat_id_efetivo(chat_id: int, conta_ativa: dict | None = None) -> int:
    if conta_ativa and conta_ativa.get(chat_id) == 2:
        return -(chat_id)
    return chat_id


def verificar_licenca_cache(chat_id: int) -> str:
    status = _cache_get(chat_id)
    if status is None:
        status = db_verificar_licenca(chat_id)
        _cache_set(chat_id, status)
    return status


async def checar_spam(update: Update) -> bool:
    """
    Verifica rate limit. Retorna True se deve bloquear a mensagem.
    Em caso de spam agressivo, revoga a licença automaticamente.
    """
    chat_id   = update.effective_chat.id
    resultado = checar_rate_limit(chat_id)

    if resultado == "ignorar":
        return True

    if resultado == "spam":
        db_log_spam(chat_id, "agressivo")
        db_revogar_licenca(chat_id)
        cache_invalidar(chat_id)
        resetar_rate(chat_id)
        try:
            await update.message.reply_text(
                "🚫 *Acesso bloqueado.*\n\n"
                "Foram detectadas muitas interações em pouco tempo.\n"
                "Sua licença foi revogada. Entre em contato com @okamaji.",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass
        return True

    return False


async def verificar_acesso(update: Update, conta_ativa: dict | None = None) -> bool:
    """Verifica licença com cache."""
    chat_id = update.effective_chat.id
    status  = _cache_get(chat_id)
    if status is None:
        status = db_verificar_licenca(chat_id)
        _cache_set(chat_id, status)

    if status == "ok":
        return True

    if status == "expirada":
        await update.message.reply_text(
            "⚠️ *Sua licença expirou!*\n\nUse /key para registrar uma nova chave.\nSuporte: @okamaji",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            "🔒 *Sua licença não está ativa.*\n\nUse /key para registrar uma nova licença.",
            parse_mode=ParseMode.MARKDOWN
        )
    return False
