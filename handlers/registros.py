"""
handlers/registros.py — Gerenciamento de registros: desfazer, editar, retirar, extrato paginado.

Correções:
- desfazer/retirar/excluir: usa db_saldo_agregado() — não busca todos os rows
- extrato_mais: usa db_recentes_com_total() — sem import inline de psycopg2
- Todos os imports no topo do módulo
"""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from helpers import fmt, fmt_registro
from database import (db_ultimo_registro, db_deletar_registro, db_registro_por_id,
                      db_atualizar_registro, db_recentes, db_saldo_agregado,
                      db_recentes_com_total)
from middleware import verificar_acesso, get_chat_id_efetivo
from keyboards import (teclado_editar_recentes, teclado_campos_editar,
                       teclado_tipos_editar, teclado_retirar_recentes,
                       teclado_extrato_paginado)

PAGE_SIZE = 20


async def cmd_desfazer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    r = db_ultimo_registro(chat_id)
    if not r:
        await update.message.reply_text("📭 Nenhum registro para desfazer.")
        return
    teclado = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirmar", callback_data=f"desfazer_confirm:{r['id']}"),
        InlineKeyboardButton("❌ Cancelar",  callback_data="desfazer_cancel"),
    ]])
    await update.message.reply_text(
        f"↩️ *Remover o último registro?*\n\n{fmt_registro(r)}",
        reply_markup=teclado,
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_retirar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id  = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    recentes = db_recentes(chat_id, 8)
    teclado  = teclado_retirar_recentes(recentes)
    if not teclado:
        await update.message.reply_text("📭 Nenhum registro para retirar.")
        return
    await update.message.reply_text(
        "🗑️ *Apagar registro*\n\n_Selecione o registro que deseja remover:_\n_(Exibindo os 8 mais recentes)_",
        reply_markup=teclado,
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_editar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id  = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    recentes = db_recentes(chat_id, 8)
    teclado  = teclado_editar_recentes(recentes)
    if not teclado:
        await update.message.reply_text("📭 Nenhum registro para editar.")
        return
    await update.message.reply_text(
        "✏️ *Editar registro*\n\n_Selecione o registro que deseja modificar:_\n_(Exibindo os 8 mais recentes)_",
        reply_markup=teclado,
        parse_mode=ParseMode.MARKDOWN
    )


# ── Callbacks ─────────────────────────────────────────────────────────────────
async def handle_registros_callback(query, chat_id: int, data: str,
                                     estados: dict, conta_ativa: dict) -> bool:
    SEP = "─" * 28

    if data.startswith("desfazer_confirm:"):
        reg_id = int(data.replace("desfazer_confirm:", ""))
        r = db_deletar_registro(reg_id)
        if not r:
            await query.edit_message_text("⚠️ Registro não encontrado ou já removido.")
            return True
        s = db_saldo_agregado(chat_id)
        await query.edit_message_text(
            f"↩️ *Registro removido!*\n\n{fmt_registro(r)}\n\n{SEP}\n💲 Saldo atual: {fmt(s['saldo'])}",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if data == "desfazer_cancel":
        await query.edit_message_text("Operação cancelada.")
        return True

    if data.startswith("retirar:"):
        val = data.replace("retirar:", "")
        if val == "cancelar":
            await query.edit_message_text("❌ Operação cancelada.")
            return True
        r = db_deletar_registro(int(val))
        if not r:
            await query.edit_message_text("⚠️ Registro não encontrado.")
            return True
        s = db_saldo_agregado(chat_id)
        await query.edit_message_text(
            f"🗑️ *Registro apagado!*\n\n{fmt_registro(r)}\n\n{SEP}\n💰 *Saldo atual: {fmt(s['saldo'])}*",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if data.startswith("editar_sel:"):
        val = data.replace("editar_sel:", "")
        if val == "cancelar":
            estados.pop(chat_id, None)
            await query.edit_message_text("❌ Edição cancelada.")
            return True
        reg_id = int(val)
        r = db_registro_por_id(reg_id)
        if not r:
            await query.edit_message_text("⚠️ Registro não encontrado.")
            return True
        estados[chat_id] = {"etapa": "editar_aguardando_campo", "reg_id": reg_id}
        await query.edit_message_text(
            f"✏️ *Editando registro #{reg_id}*\n\n{fmt_registro(r)}\n\n_Qual campo alterar?_",
            reply_markup=teclado_campos_editar(reg_id),
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if data.startswith("editar_campo:"):
        partes        = data.split(":")
        reg_id, campo = int(partes[1]), partes[2]
        if campo == "excluir":
            r = db_deletar_registro(reg_id)
            if not r:
                await query.edit_message_text("⚠️ Registro não encontrado.")
                return True
            estados.pop(chat_id, None)
            s = db_saldo_agregado(chat_id)
            await query.edit_message_text(
                f"🗑️ *Registro excluído!*\n\n{fmt_registro(r)}\n\n{SEP}\n💰 *Saldo atual: {fmt(s['saldo'])}*",
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        if campo == "tipo":
            estados[chat_id] = {"etapa": "editar_valor", "reg_id": reg_id, "campo": campo}
            r = db_registro_por_id(reg_id)
            await query.edit_message_text(
                f"🔄 *Alterar tipo do registro #{reg_id}*\nAtual: *{r['tipo']}*\n\nEscolha o novo tipo:",
                reply_markup=teclado_tipos_editar(reg_id),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            estados[chat_id] = {"etapa": "editar_valor", "reg_id": reg_id, "campo": campo}
            labels = {
                "valor":    "💵 Digite o novo valor:",
                "descricao": "📝 Digite a nova descrição:",
                "destino":  "📍 Digite o novo destino/origem:",
            }
            await query.edit_message_text(labels[campo], parse_mode=ParseMode.MARKDOWN)
        return True

    if data.startswith("editar_tipo:"):
        partes        = data.split(":")
        reg_id, ntipo = int(partes[1]), partes[2]
        r = db_atualizar_registro(reg_id, "tipo", ntipo)
        if not r:
            await query.edit_message_text("⚠️ Registro não encontrado.")
            return True
        estados.pop(chat_id, None)
        await query.edit_message_text(
            f"✅ *Tipo atualizado!*\n\n{fmt_registro(r)}",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if data.startswith("extrato_mais:"):
        offset = int(data.replace("extrato_mais:", ""))
        rows, total = db_recentes_com_total(chat_id, PAGE_SIZE, offset)
        if not rows:
            await query.edit_message_text("📭 Sem mais registros.")
            return True
        from helpers import enviar_em_partes
        SEP_EX  = "─" * 28
        linhas  = f"📋 *Extrato — registros {offset+1} a {offset+len(rows)}*\n{SEP_EX}\n\n"
        linhas += f"\n\n{SEP_EX}\n\n".join(fmt_registro(r) for r in rows)
        teclado = teclado_extrato_paginado(offset, total, PAGE_SIZE)
        from helpers import enviar_em_partes as _ep
        for parte in _ep(linhas):
            await query.message.reply_text(
                parte, parse_mode=ParseMode.MARKDOWN, reply_markup=teclado
            )
        return True

    return False
