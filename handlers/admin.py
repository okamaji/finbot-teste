"""
handlers/admin.py — Comandos administrativos (somente ADMIN_ID).
"""
import secrets
from datetime import date, timedelta
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from config import ADMIN_ID
from database import get_conn, release_conn
from middleware import cache_invalidar


async def cmd_gerar_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        await update.message.reply_text("❌ Sem permissão.")
        return
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Uso: /gerar_key <dias>\nEx: /gerar_key 30")
        return
    dias     = int(args[0])
    validade = date.today() + timedelta(days=dias)
    key      = secrets.token_hex(8).upper()
    conn     = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO licencas (key, validade) VALUES (%s, %s)", (key, validade))
        conn.commit()
    finally:
        release_conn(conn)
    await update.message.reply_text(
        f"✅ Chave gerada!\n\n`{key}`\n\nValidade: {validade.strftime('%d/%m/%Y')} ({dias} dias)",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_revogar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_ID:
        await update.message.reply_text("❌ Sem permissão.")
        return
    args = context.args
    if not args:
        await update.message.reply_text(
            "Uso:\n/revogar @username\n/revogar 123456789\n/revogar key A1B2C3D4"
        )
        return

    conn = get_conn()
    msg  = ""
    try:
        with conn.cursor() as cur:
            if args[0].lower() == "all":
                cur.execute("UPDATE licencas SET ativo=FALSE WHERE ativo=TRUE RETURNING id")
                n = len(cur.fetchall())
                msg = f"✅ {n} licença(s) revogada(s)."
            elif args[0].lower() == "key":
                if len(args) < 2:
                    await update.message.reply_text("Uso: /revogar key A1B2C3D4")
                    return
                key = args[1].upper()
                cur.execute(
                    "UPDATE licencas SET ativo=FALSE WHERE key=%s AND ativo=TRUE RETURNING chat_id",
                    (key,)
                )
                row = cur.fetchone()
                if row and row["chat_id"]:
                    cache_invalidar(row["chat_id"])
                msg = f"✅ Key {key} revogada." if row else f"⚠️ Nenhuma licença ativa para {key}."
            elif args[0].lstrip("-").isdigit():
                alvo = int(args[0])
                cur.execute(
                    "UPDATE licencas SET ativo=FALSE WHERE chat_id=%s AND ativo=TRUE RETURNING chat_id",
                    (alvo,)
                )
                row = cur.fetchone()
                if row:
                    cache_invalidar(alvo)
                msg = f"✅ chat_id {alvo} revogado." if row else f"⚠️ Nenhuma licença ativa para {alvo}."
            else:
                username = args[0].lstrip("@")
                cur.execute(
                    "UPDATE licencas SET ativo=FALSE WHERE username=%s AND ativo=TRUE RETURNING chat_id",
                    (username,)
                )
                row = cur.fetchone()
                if row and row["chat_id"]:
                    cache_invalidar(row["chat_id"])
                msg = f"✅ @{username} revogado." if row else f"⚠️ Nenhuma licença para @{username}."
        conn.commit()   # commit sempre no mesmo bloco — garante consistência
    finally:
        release_conn(conn)
    await update.message.reply_text(msg)
