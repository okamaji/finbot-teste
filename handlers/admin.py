"""
handlers/admin.py — Comandos administrativos (somente ADMIN_ID).

Novidades vs versão anterior:
- /gerarkey (era /gerar_key) — compatível com novo nome
- /revogar all — revogar TODAS as licenças ativas
- /veruser <chat_id> — ver detalhes de um usuário
- /stats — painel de estatísticas
- /users — lista de usuários
- /admin — lista todos os comandos administrativos
- Usa db_gerar_key / db_revogar_por_* do novo database.py
"""
import secrets
import logging
from datetime import date
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from config import ADMIN_ID
from database import (db_gerar_key, db_revogar_por_chat, db_revogar_por_key,
                      db_revogar_por_username, db_licenca_por_chat,
                      db_stats, db_todos_usuarios, get_conn, release_conn)
from middleware import cache_invalidar

logger = logging.getLogger(__name__)

SEP = "─" * 19


def _is_admin(update: Update) -> bool:
    return update.effective_chat.id == ADMIN_ID


# ── /admin ────────────────────────────────────────────────────────────────────
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista todos os comandos administrativos disponíveis."""
    if not _is_admin(update):
        await update.message.reply_text("❌ Sem permissão.")
        return

    await update.message.reply_text(
        f"👑 *Painel Admin — FinBot v2*\n{SEP}\n\n"
        "🔑 *Licenças*\n"
        "/gerarkey `<dias>` — Gera nova chave de acesso\n"
        "/revogar `<chat_id>` — Revoga licença por chat\\_id\n"
        "/revogar `@username` — Revoga licença por username\n"
        "/revogar `key <KEY>` — Revoga uma chave específica\n"
        "/revogar `all` — Revoga todas as licenças ativas\n\n"
        f"{SEP}\n"
        "👤 *Usuários*\n"
        "/veruser `<chat_id>` — Ver detalhes de um usuário\n"
        "/users — Listar todos os usuários cadastrados\n\n"
        f"{SEP}\n"
        "📊 *Estatísticas*\n"
        "/stats — Painel geral de estatísticas do bot\n\n"
        f"{SEP}\n"
        "📣 *Mensagens*\n"
        "/mensagem `<texto>` — Broadcast para todos os usuários\n"
        "/mensagemuser `<chat_id>` `<texto>` — Mensagem para usuário específico\n\n"
        f"{SEP}\n"
        "_Todos os comandos acima são exclusivos para administradores._",
        parse_mode=ParseMode.MARKDOWN
    )


# ── /gerarkey ─────────────────────────────────────────────────────────────────
async def cmd_gerar_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Alias /gerar_key para compatibilidade."""
    await _cmd_gerarkey(update, context)


async def cmd_gerarkey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _cmd_gerarkey(update, context)


async def _cmd_gerarkey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("❌ Sem permissão.")
        return

    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text("Uso: /gerarkey <dias>\nEx: /gerarkey 30")
        return

    dias = int(args[0])
    key  = secrets.token_hex(8).upper()
    r    = db_gerar_key(dias, key)

    await update.message.reply_text(
        f"✅ *Chave gerada!*\n\n`{key}`\n\n"
        f"📅 Validade: {r['validade'].strftime('%d/%m/%Y')} ({dias} dias)",
        parse_mode=ParseMode.MARKDOWN
    )


# ── /revogar ──────────────────────────────────────────────────────────────────
async def cmd_revogar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update):
        await update.message.reply_text("❌ Sem permissão.")
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "Uso:\n"
            "/revogar all\n"
            "/revogar @username\n"
            "/revogar 123456789\n"
            "/revogar key A1B2C3D4"
        )
        return

    conn = get_conn()
    msg  = ""
    try:
        with conn.cursor() as cur:
            if args[0].lower() == "all":
                cur.execute("UPDATE licencas SET ativo=FALSE WHERE ativo=TRUE RETURNING chat_id")
                rows = cur.fetchall()
                n    = len(rows)
                for row in rows:
                    if row["chat_id"]:
                        cache_invalidar(row["chat_id"])
                msg = f"✅ {n} licença(s) revogada(s)."
            elif args[0].lower() == "key":
                if len(args) < 2:
                    await update.message.reply_text("Uso: /revogar key A1B2C3D4")
                    return
                key = args[1].upper()
                r   = db_revogar_por_key(key)
                if r and r.get("chat_id"):
                    cache_invalidar(r["chat_id"])
                msg = f"✅ Key `{key}` revogada." if r else f"⚠️ Nenhuma licença ativa para `{key}`."
            elif args[0].lstrip("-").isdigit():
                alvo = int(args[0])
                ok   = db_revogar_por_chat(alvo)
                if ok:
                    cache_invalidar(alvo)
                msg = f"✅ chat_id {alvo} revogado." if ok else f"⚠️ Nenhuma licença ativa para {alvo}."
            else:
                username = args[0].lstrip("@")
                r        = db_revogar_por_username(username)
                if r and r.get("chat_id"):
                    cache_invalidar(r["chat_id"])
                msg = f"✅ @{username} revogado." if r else f"⚠️ Nenhuma licença para @{username}."
        conn.commit()
    finally:
        release_conn(conn)

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ── /veruser ──────────────────────────────────────────────────────────────────
async def cmd_veruser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ver detalhes de um usuário específico."""
    if not _is_admin(update):
        return
    args = context.args
    if not args or not args[0].lstrip("-").isdigit():
        await update.message.reply_text("Uso: /veruser <chat_id>")
        return

    chat_id = int(args[0])
    lic     = db_licenca_por_chat(chat_id)
    if not lic:
        await update.message.reply_text(f"⚠️ Nenhuma licença para chat_id {chat_id}.")
        return

    status = "✅ Ativo" if lic["ativo"] and lic["validade"] >= date.today() else "❌ Inativo/Expirado"
    await update.message.reply_text(
        f"👤 *Usuário {chat_id}*\n\n"
        f"👤 Username: @{lic.get('username') or 'N/A'}\n"
        f"🔑 Key: `{lic['key']}`\n"
        f"📅 Validade: {lic['validade'].strftime('%d/%m/%Y')}\n"
        f"📊 Status: {status}\n"
        f"📋 Termos aceitos: {'✅' if lic.get('termos_aceitos') else '❌'}\n"
        f"⚠️ Tentativas inválidas: {lic.get('tentativas_key', 0)}\n"
        f"📆 Criado em: {lic['criado_em'].strftime('%d/%m/%Y')}",
        parse_mode=ParseMode.MARKDOWN
    )


# ── /stats ────────────────────────────────────────────────────────────────────
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Painel com estatísticas gerais."""
    if not _is_admin(update):
        return

    s = db_stats()
    await update.message.reply_text(
        f"📊 *Painel Administrativo*\n{SEP}\n\n"
        f"👥 Usuários cadastrados:  *{s['total_usuarios']}*\n"
        f"✅ Usuários ativos:       *{s['usuarios_ativos']}*\n"
        f"🔑 Licenças ativas:       *{s['licencas_ativas']}*\n"
        f"❌ Licenças expiradas:    *{s['licencas_expiradas']}*\n"
        f"⏳ Aguardando renovação:  *{s['aguardando_renovacao']}*\n\n"
        f"{SEP}\n"
        f"📋 Total de registros:    *{s['total_registros']}*\n"
        f"🚫 Spams hoje:           *{s['spam_hoje']}*\n\n"
        f"_Use /users para ver lista de usuários_",
        parse_mode=ParseMode.MARKDOWN
    )


# ── /users ────────────────────────────────────────────────────────────────────
async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista todos os usuários."""
    if not _is_admin(update):
        return

    usuarios = db_todos_usuarios()
    if not usuarios:
        await update.message.reply_text("📭 Nenhum usuário cadastrado.")
        return

    hoje   = date.today()
    linhas = [f"👥 *Usuários* ({len(usuarios)} total)\n{SEP}\n"]
    for u in usuarios[:50]:
        ativo  = u["ativo"] and u["validade"] >= hoje
        emoji  = "✅" if ativo else "❌"
        uname  = f"@{u['username']}" if u.get("username") else "N/A"
        linhas.append(
            f"{emoji} `{u['chat_id']}`  {uname}  —  até {u['validade'].strftime('%d/%m/%Y')}"
        )

    if len(usuarios) > 50:
        linhas.append(f"\n_...e mais {len(usuarios)-50} usuários_")

    await update.message.reply_text(
        "\n".join(linhas),
        parse_mode=ParseMode.MARKDOWN
    )
