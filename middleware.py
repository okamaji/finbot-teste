"""
middleware.py — Verificação de licença, cache e helpers de contexto.
"""
import time
import threading
import logging
from cachetools import TTLCache
from telegram import Update
from telegram.constants import ParseMode
from database import db_verificar_licenca
from config import CACHE_TTL, CACHE_MAXSIZE, ESTADOS_TTL_MINUTOS

logger = logging.getLogger(__name__)

# A4 — TTLCache protegido por Lock — thread-safe contra race condition de eviction
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


# ── Estado com timestamp ──────────────────────────────────────────────────────
def estado_novo(dados: dict) -> dict:
    """Cria estado com _ts para controle de expiração."""
    return {**dados, "_ts": time.time()}


def limpar_estados_expirados(estados: dict) -> int:
    """
    Remove estados de conversa expirados.
    Chamada pelo JobQueue a cada 5 minutos — NÃO a cada mensagem.
    Retorna número de estados removidos.
    """
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


# ── Helpers de contexto ───────────────────────────────────────────────────────
def get_chat_id_efetivo(chat_id: int, conta_ativa: dict | None = None) -> int:
    if conta_ativa and conta_ativa.get(chat_id) == 2:
        return -(chat_id)
    return chat_id


async def verificar_acesso(update: Update, conta_ativa: dict | None = None) -> bool:
    """Verifica licença com cache. Nunca bate no banco se cache válido."""
    chat_id = update.effective_chat.id
    status  = _cache_get(chat_id)
    if status is None:
        status = db_verificar_licenca(chat_id)
        _cache_set(chat_id, status)

    if status == "ok":
        return True
    if status == "expirada":
        await update.message.reply_text(
            "⚠️ *Seu plano expirou!*\n\nRenove agora com @okamaji para continuar.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            "🔒 Acesso negado. Use /start para inserir sua chave de acesso."
        )
    return False


def verificar_licenca_cache(chat_id: int) -> str:
    """Versão síncrona do verificar_acesso para uso em handle_texto."""
    status = _cache_get(chat_id)
    if status is None:
        status = db_verificar_licenca(chat_id)
        _cache_set(chat_id, status)
    return status
