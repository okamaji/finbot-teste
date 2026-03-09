"""
handlers/contas.py — Contas a pagar.

Correção: callback_data usa código curto (mf:42:credito) e converte com SHORT_METODO.
"""
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from helpers import fmt, fmt_conta, parsear_valor
from database import db_contas, db_conta_por_id, db_marcar_pago, db_inserir_conta
from middleware import verificar_acesso, get_chat_id_efetivo
from keyboards import teclado_contas_pendentes, teclado_metodo_pagamento, teclado_confirmar_pago
from config import EMOJI_METODO, SHORT_METODO


async def cmd_apagar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await verificar_acesso(update):
        return
    estados = context.bot_data.setdefault("estados", {})
    estados[update.effective_chat.id] = {"etapa": "pagar_nome"}
    await update.message.reply_text(
        "📋 *Nova conta a pagar*\n\n📝 Qual o nome da conta?\n_ex: luz, internet, aluguel_",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_pendentes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    contas  = db_contas(chat_id)

    if not contas:
        await update.message.reply_text("📭 Nenhuma conta cadastrada.\nUse /apagar para adicionar.")
        return

    pendentes = [r for r in contas if r["status"] == "PENDENTE"]
    pagos     = [r for r in contas if r["status"] == "PAGO"]
    SEP       = "─" * 28

    t = "📋 *Contas a Pagar*\n"
    if pendentes:
        total = sum(float(r["valor"]) for r in pendentes)
        t += f"\n⏳ *PENDENTES ({len(pendentes)})* — {fmt(total)}\n{SEP}\n"
        for r in sorted(pendentes, key=lambda x: x["vencimento"]):
            t += f"\n{fmt_conta(r)}\n"
    if pagos:
        total = sum(float(r["valor"]) for r in pagos)
        t += f"\n{SEP}\n✅ *PAGOS ({len(pagos)})* — {fmt(total)}\n{SEP}\n"
        for r in list(reversed(pagos))[:10]:
            t += f"\n{fmt_conta(r)}\n"

    teclado = teclado_contas_pendentes(contas)
    await update.message.reply_text(t, parse_mode=ParseMode.MARKDOWN, reply_markup=teclado)


async def cmd_pago(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    contas  = db_contas(chat_id)
    teclado = teclado_contas_pendentes(contas)
    if not teclado:
        await update.message.reply_text("✅ Nenhuma conta pendente no momento!")
        return
    await update.message.reply_text(
        "💳 *Qual conta foi paga?*\n\n_Selecione abaixo:_",
        reply_markup=teclado,
        parse_mode=ParseMode.MARKDOWN
    )


async def handle_contas_callback(query, chat_id: int, data: str) -> bool:

    # Passo 1: clicou numa conta → pedir método
    if data.startswith("pagar:"):
        conta_id = int(data.replace("pagar:", ""))
        conta    = db_conta_por_id(conta_id)
        if not conta or conta["status"] != "PENDENTE":
            await query.edit_message_text("⚠️ Conta não encontrada ou já paga.")
            return True
        await query.edit_message_text(
            f"💳 *{conta['nome']}* — {fmt(conta['valor'])}\n📅 Vence: {conta['vencimento']}\n\nComo foi pago?",
            reply_markup=teclado_metodo_pagamento(conta_id),
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    # Passo 2: escolheu método → confirmar  (mf:42:credito)
    if data.startswith("mf:"):
        _, conta_id_str, metodo_short = data.split(":")
        conta_id = int(conta_id_str)
        metodo   = SHORT_METODO.get(metodo_short, metodo_short)
        conta    = db_conta_por_id(conta_id)
        if not conta:
            await query.edit_message_text("⚠️ Conta não encontrada.")
            return True
        em = EMOJI_METODO.get(metodo, "💳")
        await query.edit_message_text(
            f"✅ *Confirmar pagamento?*\n\n"
            f"🧾 {conta['nome']} — {fmt(conta['valor'])}\n"
            f"{em} Método: *{metodo}*\n\n"
            f"_Isso criará um registro de despesa no seu extrato._",
            reply_markup=teclado_confirmar_pago(conta_id, metodo_short),
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    # Passo 3: confirmado → marcar pago  (cp:42:credito)
    if data.startswith("cp:"):
        _, conta_id_str, metodo_short = data.split(":")
        conta_id = int(conta_id_str)
        metodo   = SHORT_METODO.get(metodo_short, metodo_short)
        resultado = db_marcar_pago(conta_id, metodo)
        if not resultado:
            await query.edit_message_text("⚠️ Conta não encontrada ou já paga.")
            return True
        em = EMOJI_METODO.get(metodo, "💳")
        await query.edit_message_text(
            f"✅ *Pagamento registrado!*\n\n{fmt_conta(resultado)}\n\n"
            f"🧾 _Lançamento adicionado ao extrato automaticamente._",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if data == "cancelar_metodo":
        await query.edit_message_text("❌ Operação cancelada.")
        return True

    return False


async def handle_contas_texto(update: Update, estado: dict, chat_id: int,
                               chat_id_ef: int, estados: dict) -> bool:
    etapa = estado.get("etapa", "")
    texto = update.message.text.strip()

    if etapa == "pagar_nome":
        estados[chat_id]["nome"]  = texto
        estados[chat_id]["etapa"] = "pagar_valor"
        await update.message.reply_text(
            "💵 Qual o valor?\n_ex: 150 · 89,90_", parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "pagar_valor":
        valor = parsear_valor(texto)
        if valor is None:
            await update.message.reply_text(
                "❌ Valor inválido. Tente: `150` · `89,90`", parse_mode=ParseMode.MARKDOWN
            )
            return True
        estados[chat_id]["valor"] = valor
        estados[chat_id]["etapa"] = "pagar_vencimento"
        await update.message.reply_text(
            "📅 Data de vencimento?\n_ex: 10/03 · 15/04_", parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "pagar_vencimento":
        estados[chat_id]["vencimento"] = texto
        estados[chat_id]["etapa"]      = "pagar_banco"
        await update.message.reply_text(
            "🏦 Qual banco vai pagar?\n_ex: Nubank, Inter, Itaú_", parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "pagar_banco":
        d = estados.pop(chat_id)
        r = db_inserir_conta(chat_id_ef, d["nome"], d["valor"], d["vencimento"], texto)
        await update.message.reply_text(
            f"✅ *Conta cadastrada!*\n\n{fmt_conta(r)}\n\nUse /pago quando pagar.",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    return False
