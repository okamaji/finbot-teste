"""
keyboards.py — Teclados inline do Telegram.
Todos os fluxos incluem botão ❌ Cancelar via callback "cancelar_fluxo".
"""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from config import EMOJI, METODOS_PAGAMENTO, EMOJI_METODO, METODO_SHORT
from helpers import fmt

BTN_CANCELAR = InlineKeyboardButton("❌ Cancelar", callback_data="cancelar_fluxo")


def teclado_tipo() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 Despesa",  callback_data="tipo:despesa"),
            InlineKeyboardButton("🟢 Depósito", callback_data="tipo:deposito"),
            InlineKeyboardButton("🔵 Pix",      callback_data="tipo:pix"),
        ],
        [BTN_CANCELAR],
    ])


def teclado_termos() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Concordo",    callback_data="termos_aceitar"),
        InlineKeyboardButton("❌ Não aceito",  callback_data="termos_recusar"),
    ]])


def teclado_nlp() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirmar", callback_data="nlp_confirmar"),
        InlineKeyboardButton("✏️ Corrigir",  callback_data="nlp_corrigir"),
        BTN_CANCELAR,
    ]])


def teclado_metodo_pagamento(conta_id: int) -> InlineKeyboardMarkup:
    botoes = [
        [InlineKeyboardButton(
            f"{EMOJI_METODO[m]} {m}",
            callback_data=f"mf:{conta_id}:{METODO_SHORT[m]}"
        )]
        for m in METODOS_PAGAMENTO
    ]
    botoes.append([BTN_CANCELAR])
    return InlineKeyboardMarkup(botoes)


def teclado_confirmar_pago(conta_id: int, metodo_short: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirmar", callback_data=f"cp:{conta_id}:{metodo_short}"),
        BTN_CANCELAR,
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
    botoes.append([BTN_CANCELAR])
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
    botoes.append([BTN_CANCELAR])
    return InlineKeyboardMarkup(botoes)


def teclado_campos_editar(reg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💵 Valor",     callback_data=f"editar_campo:{reg_id}:valor"),
         InlineKeyboardButton("📝 Descrição", callback_data=f"editar_campo:{reg_id}:descricao")],
        [InlineKeyboardButton("📍 Destino",   callback_data=f"editar_campo:{reg_id}:destino"),
         InlineKeyboardButton("🔄 Tipo",      callback_data=f"editar_campo:{reg_id}:tipo")],
        [InlineKeyboardButton("🗑️ Excluir",   callback_data=f"editar_campo:{reg_id}:excluir")],
        [BTN_CANCELAR],
    ])


def teclado_tipos_editar(reg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔴 Despesa",  callback_data=f"editar_tipo:{reg_id}:despesa"),
            InlineKeyboardButton("🟢 Depósito", callback_data=f"editar_tipo:{reg_id}:deposito"),
            InlineKeyboardButton("🔵 Pix",      callback_data=f"editar_tipo:{reg_id}:pix"),
        ],
        [BTN_CANCELAR],
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
    botoes.append([BTN_CANCELAR])
    return InlineKeyboardMarkup(botoes)


def teclado_extrato_paginado(offset: int, total: int, page_size: int = 20) -> InlineKeyboardMarkup | None:
    if offset + page_size >= total:
        return None
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬇️ Ver mais", callback_data=f"extrato_mais:{offset+page_size}")
    ]])
