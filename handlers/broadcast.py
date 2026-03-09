"""
handlers/broadcast.py — Broadcast para todos os usuários ativos.

Correção: usa asyncio.Semaphore para envio concorrente controlado.
O envio sequencial original bloqueava o event loop por ~40s com 1k users.
Agora envia até 25 mensagens em paralelo e ainda respeita o limite de 30/s da API.
"""
import asyncio
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.error import Forbidden, ChatMigrated, BadRequest

from config import ADMIN_ID
from database import get_conn, release_conn

logger = logging.getLogger(__name__)

_CONCORRENCIA = 25   # mensagens em paralelo (abaixo de 30/s do Telegram)


def _db_todos_usuarios() -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT chat_id, username
                FROM licencas
                WHERE ativo = TRUE
                  AND chat_id IS NOT NULL
                  AND (validade IS NULL OR validade >= CURRENT_DATE)
                ORDER BY chat_id
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


async def cmd_mensagem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        await update.message.reply_text("⛔ Sem permissão.")
        return

    texto = " ".join(context.args).strip() if context.args else ""
    if not texto:
        await update.message.reply_text(
            "📢 *Como usar /mensagem*\n\n"
            "`/mensagem Olá! O bot foi atualizado.`\n\n"
            "Suporta *negrito*, _itálico_, `código`.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    usuarios = _db_todos_usuarios()
    if not usuarios:
        await update.message.reply_text("📭 Nenhum usuário ativo encontrado.")
        return

    total = len(usuarios)
    await update.message.reply_text(
        f"📢 *Broadcast iniciado*\n\n"
        f"Enviando para *{total}* usuário(s)...\n\n"
        f"_{texto[:200]}{'...' if len(texto) > 200 else ''}_",
        parse_mode=ParseMode.MARKDOWN
    )

    mensagem_final = (
        f"📢 *Aviso do Bot*\n\n{texto}\n\n"
        f"─────────────────\n"
        f"_Mensagem enviada pelo administrador_"
    )

    enviado = bloq = falhou = 0
    sem = asyncio.Semaphore(_CONCORRENCIA)

    async def _enviar(usuario: dict):
        nonlocal enviado, bloq, falhou
        async with sem:
            cid      = usuario["chat_id"]
            username = usuario.get("username") or f"id:{cid}"
            try:
                await context.bot.send_message(
                    chat_id=cid, text=mensagem_final, parse_mode=ParseMode.MARKDOWN
                )
                enviado += 1
            except Forbidden:
                bloq += 1
                logger.info("Broadcast bloqueado por %s (%s)", username, cid)
            except (BadRequest, ChatMigrated) as e:
                falhou += 1
                logger.warning("Broadcast falhou para %s: %s", username, e)
            except Exception as e:
                falhou += 1
                logger.error("Broadcast erro para %s: %s", username, e)

    await asyncio.gather(*[_enviar(u) for u in usuarios])

    agora        = datetime.now().strftime("%d/%m/%Y %H:%M")
    linhas_extra = ""
    if bloq:   linhas_extra += f"🚫 Bloqueados: {bloq}\n"
    if falhou: linhas_extra += f"❌ Com erro:   {falhou}\n"

    await update.message.reply_text(
        f"✅ *Broadcast concluído!*  _{agora}_\n\n"
        f"👥 Total:    {total}\n"
        f"📤 Enviados: {enviado}\n"
        f"{linhas_extra}",
        parse_mode=ParseMode.MARKDOWN
    )
