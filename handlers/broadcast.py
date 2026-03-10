"""
handlers/broadcast.py — Broadcast e notificações para usuários.

Novidades:
- /mensagem — broadcast para todos os usuários ativos (semaphore de 25)
- /mensagemuser <chat_id> <msg> — mensagem para usuário específico
"""
import asyncio
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.error import Forbidden, ChatMigrated, BadRequest

from config import ADMIN_ID
from database import db_usuarios_ativos

logger     = logging.getLogger(__name__)
SEP        = "─" * 19
_CONCORRENCIA = 25


def _is_admin(update: Update) -> bool:
    return update.effective_chat.id == ADMIN_ID


async def cmd_mensagem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast para todos os usuários ativos."""
    if not _is_admin(update):
        await update.message.reply_text("⛔ Sem permissão.")
        return

    texto = " ".join(context.args).strip() if context.args else ""
    if not texto:
        await update.message.reply_text(
            "📢 *Como usar /mensagem*\n\n"
            "`/mensagem Olá! O bot foi atualizado.`\n\n"
            "Suporta *negrito*, _itálico_, `código`.\n\n"
            "Para usuário específico: /mensagemuser <chat_id> <mensagem>",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    usuarios = db_usuarios_ativos()
    if not usuarios:
        await update.message.reply_text("📭 Nenhum usuário ativo encontrado.")
        return

    total = len(usuarios)
    await update.message.reply_text(
        f"📢 *Broadcast iniciado*\n\nEnviando para *{total}* usuário(s)...\n\n"
        f"_{texto[:200]}{'...' if len(texto) > 200 else ''}_",
        parse_mode=ParseMode.MARKDOWN
    )

    mensagem_final = (
        f"📢 *Aviso do Bot*\n\n{texto}\n\n"
        f"{SEP}\n"
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


async def cmd_mensagemuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Envia mensagem para um usuário específico."""
    if not _is_admin(update):
        return

    args = context.args
    if not args or not args[0].lstrip("-").isdigit() or len(args) < 2:
        await update.message.reply_text(
            "Uso: /mensagemuser <chat_id> <mensagem>\n"
            "Ex: /mensagemuser 123456789 Sua licença foi renovada!"
        )
        return

    cid   = int(args[0])
    texto = " ".join(args[1:])

    try:
        await context.bot.send_message(
            chat_id=cid,
            text=f"📢 *Mensagem do suporte*\n\n{texto}",
            parse_mode=ParseMode.MARKDOWN
        )
        await update.message.reply_text(f"✅ Mensagem enviada para {cid}.")
    except Exception as e:
        await update.message.reply_text(f"❌ Erro ao enviar para {cid}: {e}")
