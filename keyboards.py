"""
keyboards.py — Teclados inline do Telegram.

Correção: callback_data de método usa código curto (max 64 bytes no Telegram).
  "metodo_fatura:42:Cartão de Crédito" → "mf:42:credito"
"""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from config import EMOJI, METODOS_PAGAMENTO, EMOJI_METODO, METODO_SHORT
from helpers import fmt


def teclado_tipo() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 Despesa",  callback_data="tipo:despesa"),
            InlineKeyboardButton("🟢 Depósito", callback_data="tipo:deposito"),
            InlineKeyboardButton("🔵 Pix",      callback_data="tipo:pix"),
        ],
        [InlineKeyboardButton("❌ Cancelar", callback_data="tipo:cancelar")],
    ])


def teclado_metodo_pagamento(conta_id: int) -> InlineKeyboardMarkup:
    """
    Teclado para escolher método de pagamento de uma fatura.
    Usa código curto no callback para respeitar o limite de 64 bytes.
    """
    botoes = [
        [InlineKeyboardButton(
            f"{EMOJI_METODO[m]} {m}",
            callback_data=f"mf:{conta_id}:{METODO_SHORT[m]}"   # ex: mf:42:credito
        )]
        for m in METODOS_PAGAMENTO
    ]
    botoes.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancelar_metodo")])
    return InlineKeyboardMarkup(botoes)


def teclado_confirmar_pago(conta_id: int, metodo_short: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirmar", callback_data=f"cp:{conta_id}:{metodo_short}"),
        InlineKeyboardButton("❌ Cancelar",  callback_data="cancelar_metodo"),
    ]])


def teclado_contas_pendentes(contas: list) -> InlineKeyboardMarkup | None:
    pendentes = [r for r in contas if r["status"] == "PENDENTE"]
    if not pendentes:
        return None
    botoes = [
        [InlineKeyboardButton(
            f"⏳ {r['nome']} — {fmt(r['valor'])}  📅 {r['vencimento']}",
            callback_data=f"pagar:{r['id']}"
        )]
        for r in sorted(pendentes, key=lambda x: x["vencimento"])
    ]
    return InlineKeyboardMarkup(botoes)


def teclado_editar_recentes(recentes: list) -> InlineKeyboardMarkup | None:
    if not recentes:
        return None
    botoes = [
        [InlineKeyboardButton(
            f"{EMOJI.get(r['tipo'],'⚪')} {r['descricao'][:28]} — {fmt(r['valor'])} ({r['data']})",
            callback_data=f"editar_sel:{r['id']}"
        )]
        for r in recentes
    ]
    botoes.append([InlineKeyboardButton("❌ Cancelar", callback_data="editar_sel:cancelar")])
    return InlineKeyboardMarkup(botoes)


def teclado_campos_editar(reg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💵 Valor",     callback_data=f"editar_campo:{reg_id}:valor"),
         InlineKeyboardButton("📝 Descrição", callback_data=f"editar_campo:{reg_id}:descricao")],
        [InlineKeyboardButton("📍 Destino",   callback_data=f"editar_campo:{reg_id}:destino"),
         InlineKeyboardButton("🔄 Tipo",      callback_data=f"editar_campo:{reg_id}:tipo")],
        [InlineKeyboardButton("🗑️ Excluir",   callback_data=f"editar_campo:{reg_id}:excluir")],
        [InlineKeyboardButton("❌ Cancelar",  callback_data="editar_sel:cancelar")],
    ])


def teclado_tipos_editar(reg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 Despesa",  callback_data=f"editar_tipo:{reg_id}:despesa"),
            InlineKeyboardButton("🟢 Depósito", callback_data=f"editar_tipo:{reg_id}:deposito"),
            InlineKeyboardButton("🔵 Pix",      callback_data=f"editar_tipo:{reg_id}:pix"),
        ],
        [InlineKeyboardButton("❌ Cancelar", callback_data="editar_sel:cancelar")],
    ])


def teclado_retirar_recentes(recentes: list) -> InlineKeyboardMarkup | None:
    if not recentes:
        return None
    botoes = [
        [InlineKeyboardButton(
            f"{EMOJI.get(r['tipo'],'⚪')} {r['descricao'][:28]} — {fmt(r['valor'])} ({r['data']})",
            callback_data=f"retirar:{r['id']}"
        )]
        for r in recentes
    ]
    botoes.append([InlineKeyboardButton("❌ Cancelar", callback_data="retirar:cancelar")])
    return InlineKeyboardMarkup(botoes)


def teclado_extrato_paginado(offset: int, total: int, page_size: int = 20) -> InlineKeyboardMarkup | None:
    if offset + page_size >= total:
        return None
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬇️ Ver mais", callback_data=f"extrato_mais:{offset+page_size}")
    ]])


def teclado_inv_remover(inv_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🗑️ Remover", callback_data=f"inv_remover:{inv_id}"),
        InlineKeyboardButton("❌ Cancelar", callback_data="inv_cancelar"),
    ]])
