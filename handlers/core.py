"""
handlers/core.py — Comandos principais: start, home, saldo, hoje, mes, extrato.

Correções vs versão anterior:
- cmd_saldo: usa db_saldo_agregado() — não busca todos os rows
- cmd_hoje: query SQL filtrada por data — não varre tudo em Python
- _listar_tipo: idem, usa query com WHERE tipo=
- cmd_start: usa verificar_licenca_cache (com cache) em vez de db_verificar_licenca raw
- cmd_extrato: usa db_recentes_com_total() — 1 conexão, não 2
"""
import re
import logging
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import MESES, EMOJI, LABEL, EMOJI_METODO, FUSO
from helpers import fmt, agora_br, calcular_saldo, fmt_registro, fmt_conta, enviar_em_partes
from database import (db_recentes, db_registros_mes, db_home_data,
                      db_saldo_agregado, db_recentes_com_total, get_conn, release_conn)
from middleware import verificar_acesso, get_chat_id_efetivo, verificar_licenca_cache
from keyboards import teclado_extrato_paginado

logger = logging.getLogger(__name__)

SEP  = "─" * 19
SEP2 = "━" * 19


def menu_principal() -> str:
    return (
        "💰 *Bot de Registro Financeiro*\n\n"
        "Envie um valor: `50` · `1460,90`\n"
        "Ou escreva naturalmente:\n\n"
        "_paguei 45 mercado_\n"
        "_recebi 1200 salario_\n"
        "_mandei 50 no pix pro joão_\n"
        "_gastei 20 uber no débito_\n\n"
        f"{SEP}\n\n"
        "📋 *Comandos disponíveis*\n\n"
        "📊 *Consultas*\n\n"
        "/home — Painel geral\n"
        "/saldo — Saldo atual\n"
        "/hoje — Registros de hoje\n"
        "/mes — Resumo do mês  _(ex: /mes 02)_\n"
        "/extrato — Extrato completo\n"
        "/entradas — Todas as entradas\n"
        "/despesas — Todas as despesas\n"
        "/pixs — Todos os Pix\n\n"
        "✏️ *Gerenciar registros*\n\n"
        "/desfazer — Remove o último registro\n"
        "/editar — Edita um registro existente\n"
        "/retirar — Apaga registro por seleção\n\n"
        "🧾 *Contas a pagar*\n\n"
        "/apagar — Cadastra conta a pagar\n"
        "/pendentes — Contas pendentes e pagas\n"
        "/pago — Marca conta como paga\n\n"
        "📈 *Investimentos*\n\n"
        "/investimentos — Carteira de investimentos\n"
        "/inv\\_add — Adicionar investimento\n"
        "/inv\\_del — Remover investimento\n\n"
        f"{SEP}\n"
        "_/ajuda para ver esta mensagem novamente_"
    )


# ── /start ────────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context):
    chat_id = update.effective_chat.id
    estados = context.bot_data.setdefault("estados", {})
    estados.pop(chat_id, None)

    # Usa cache — nunca bate no banco se já verificou recentemente
    status = verificar_licenca_cache(chat_id)
    if status == "ok":
        await update.message.reply_text(menu_principal(), parse_mode=ParseMode.MARKDOWN)
    elif status == "expirada":
        await update.message.reply_text(
            "⚠️ *Seu plano expirou!*\n\nRenove agora com @okamaji para continuar usando o bot.",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        estados[chat_id] = {"etapa": "aguardando_key", "_ts": __import__("time").time()}
        nome = update.effective_user.first_name or "usuário"
        await update.message.reply_text(
            f"👋 Bem-vindo, *{nome}*!\n\nPara começar, insira sua *chave de acesso*:",
            parse_mode=ParseMode.MARKDOWN
        )


# ── /ajuda ────────────────────────────────────────────────────────────────────
async def cmd_ajuda(update: Update, context):
    if not await verificar_acesso(update):
        return
    await update.message.reply_text(menu_principal(), parse_mode=ParseMode.MARKDOWN)


# ── /cancelar ─────────────────────────────────────────────────────────────────
async def cmd_cancelar(update: Update, context):
    chat_id = update.effective_chat.id
    estados = context.bot_data.setdefault("estados", {})
    if chat_id in estados:
        estados.pop(chat_id)
        await update.message.reply_text("❌ Ação cancelada.")
    else:
        await update.message.reply_text("ℹ️ Nenhuma ação em andamento.")


# ── /home ─────────────────────────────────────────────────────────────────────
async def cmd_home(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id  = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    now      = agora_br()
    hoje_str = now.strftime("%d/%m/%Y")
    mes_str  = now.strftime("%m/%Y")
    mes_nome = MESES[now.month - 1]

    d = db_home_data(chat_id, mes_str)

    ent_mes    = float(d["ent_mes"])
    desp_mes   = float(d["desp_mes"])
    pix_mes    = float(d["pix_mes"])
    ent_total  = float(d["ent_total"])
    desp_total = float(d["desp_total"])
    pix_total  = float(d["pix_total"])
    saldo_liq  = ent_total - desp_total - pix_total

    m_pix     = float(d["metodo_pix"])
    m_transf  = float(d["metodo_transf"])
    m_credito = float(d["metodo_credito"])
    m_debito  = float(d["metodo_debito"])

    contas  = d["contas"]
    pend    = [r for r in contas if r["status"] == "PENDENTE"]
    pagos_m = [
        r for r in contas
        if r["status"] == "PAGO"
        and r.get("pago_em")
        and r["pago_em"].astimezone(FUSO).strftime("%m/%Y") == mes_str
    ]

    desp_sem_metodo = desp_mes - m_credito - m_debito
    saldo_emoji = "📈" if saldo_liq >= 0 else "📉"

    t  = f"💰 *Painel Financeiro · {hoje_str}*\n{SEP}\n"
    t += f"\n🟢 Entradas:         {fmt(ent_mes)}\n"

    if m_credito or m_debito:
        if desp_sem_metodo > 0:
            t += f"🔴 Despesas:         {fmt(desp_sem_metodo)}\n"
        if m_credito:
            t += f"💳 Cartão Crédito:   {fmt(m_credito)}\n"
        if m_debito:
            t += f"🏧 Cartão Débito:    {fmt(m_debito)}\n"
    else:
        t += f"🔴 Despesas:         {fmt(desp_mes)}\n"

    t += f"🔵 Pix saída:        {fmt(pix_mes)}\n"
    if m_pix:    t += f"   ↳ via Pix:        {fmt(m_pix)}\n"
    if m_transf: t += f"   ↳ Transferência:  {fmt(m_transf)}\n"

    t += f"\n{saldo_emoji} *Saldo líquido: {fmt(saldo_liq)}*\n"
    t += f"{SEP}\n\n🧾 *Faturas Pendentes*\n{SEP}\n"

    if pend:
        total_pend = sum(float(r["valor"]) for r in pend)
        for r in sorted(pend, key=lambda x: x["vencimento"]):
            t += f"\n⏳ {r['nome']}\n    {fmt(r['valor'])}  ·  📅 Vence {r['vencimento']}\n"
        t += f"\n💸 *Total pendente: {fmt(total_pend)}*\n"
    else:
        t += "\n✅ Sem faturas pendentes\n"

    if pagos_m:
        total_pago = sum(float(r["valor"]) for r in pagos_m)
        t += f"{SEP}\n\n✅ *Pagos em {mes_nome}*\n{SEP}\n"
        for r in pagos_m:
            pago_str = r["pago_em"].astimezone(FUSO).strftime("%d/%m  %H:%M")
            met_str  = (
                f"  {EMOJI_METODO.get(r.get('metodo_pagamento',''), '')} {r.get('metodo_pagamento','')}"
                if r.get("metodo_pagamento") else ""
            )
            t += f"\n✅ {r['nome']}\n    {fmt(r['valor'])}  ·  🕐 {pago_str}{met_str}\n"
        t += f"\n💸 *Total pago: {fmt(total_pago)}*\n"

    t += f"{SEP}\n_/ajuda para ver todos os comandos_"
    await update.message.reply_text(t, parse_mode=ParseMode.MARKDOWN)


# ── /saldo ────────────────────────────────────────────────────────────────────
async def cmd_saldo(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)

    # db_saldo_agregado usa SUM no SQL — não busca todos os rows
    s = db_saldo_agregado(chat_id)
    if s["entradas"] == 0 and s["despesas"] == 0 and s["pixs"] == 0:
        await update.message.reply_text("📭 Nenhum registro ainda.")
        return

    saldo_emoji = "📈" if s["saldo"] >= 0 else "📉"
    await update.message.reply_text(
        f"💰 *Saldo Geral*\n{SEP}\n\n"
        f"🟢 Entradas:  {fmt(s['entradas'])}\n"
        f"🔴 Despesas:  {fmt(s['despesas'])}\n"
        f"🔵 Pix:       {fmt(s['pixs'])}\n\n"
        f"{saldo_emoji} *Saldo: {fmt(s['saldo'])}*\n{SEP}",
        parse_mode=ParseMode.MARKDOWN
    )


# ── /hoje ─────────────────────────────────────────────────────────────────────
async def cmd_hoje(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id  = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    hoje_str = agora_br().strftime("%d/%m/%Y")

    # Query filtrada por data — não carrega histórico inteiro
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM registros WHERE chat_id=%s AND data=%s ORDER BY id",
                (chat_id, hoje_str)
            )
            dados = [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)

    if not dados:
        await update.message.reply_text(f"📭 Nenhum registro hoje ({hoje_str}).")
        return

    s  = calcular_saldo(dados)
    pl = "s" if len(dados) > 1 else ""
    linhas = f"\n{SEP}\n\n".join(fmt_registro(r) for r in reversed(dados))

    await update.message.reply_text(
        f"📅 *Hoje — {hoje_str}*  ({len(dados)} registro{pl})\n{SEP}\n\n"
        f"{linhas}\n\n{SEP}\n"
        f"🟢 Entradas: {fmt(s['entradas'])}\n"
        f"🔴 Saídas:   {fmt(s['despesas'] + s['pixs'])}\n\n"
        f"💲 *Saldo do dia: {fmt(s['saldo'])}*",
        parse_mode=ParseMode.MARKDOWN
    )


# ── /mes ──────────────────────────────────────────────────────────────────────
async def cmd_mes(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    now     = agora_br()
    args    = context.args

    if args and re.match(r"^\d{1,2}$", args[0]):
        mes_num = int(args[0])
        ano     = now.year if mes_num <= now.month else now.year - 1
        mes_str = f"{mes_num:02d}/{ano}"
        titulo  = f"{MESES[mes_num-1]}/{ano}"
    else:
        mes_str = now.strftime("%m/%Y")
        titulo  = f"{MESES[now.month-1]}/{now.year}"

    dados = db_registros_mes(chat_id, mes_str)
    if not dados:
        await update.message.reply_text(f"📭 Nenhum registro em {titulo}.")
        return

    s = calcular_saldo(dados)
    t = f"📆 *{titulo}*  ({len(dados)} registros)\n{SEP}\n"

    for tipo in ("deposito", "despesa", "pix"):
        regs = [r for r in dados if r["tipo"] == tipo]
        if not regs:
            continue
        total = sum(float(r["valor"]) for r in regs)
        t += f"\n{EMOJI[tipo]} *{LABEL[tipo]}s*  ({len(regs)})  —  {fmt(total)}\n\n"
        for r in regs:
            met = f"  {EMOJI_METODO.get(r.get('metodo_pagamento',''),'')} " if r.get("metodo_pagamento") else ""
            t += f"  • {r['data']}  {r['hora']}  ·  {r['descricao']}  ·  {fmt(r['valor'])}{met}\n"

    t += (
        f"\n{SEP}\n"
        f"🟢 Entradas:  *{fmt(s['entradas'])}*\n"
        f"🔴 Saídas:    *{fmt(s['despesas'] + s['pixs'])}*\n\n"
        f"💵 *Saldo: {fmt(s['saldo'])}*"
    )

    for parte in enviar_em_partes(t):
        await update.message.reply_text(parte, parse_mode=ParseMode.MARKDOWN)


# ── /extrato ──────────────────────────────────────────────────────────────────
async def cmd_extrato(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id, conta_ativa)
    PAGE    = 20

    # 1 conexão, não 2
    recentes, total = db_recentes_com_total(chat_id, PAGE, 0)
    if not recentes:
        await update.message.reply_text("📭 Nenhum registro no extrato ainda.")
        return

    cabecalho = (
        f"📋 *Extrato*  —  últimos {len(recentes)} de {total} registros\n{SEP}\n\n"
    )
    corpo   = f"\n\n{SEP}\n\n".join(fmt_registro(r) for r in recentes)
    texto   = cabecalho + corpo
    teclado = teclado_extrato_paginado(0, total, PAGE)
    partes  = enviar_em_partes(texto)

    for i, parte in enumerate(partes):
        await update.message.reply_text(
            parte,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=teclado if i == len(partes) - 1 else None
        )


# ── helpers internos ──────────────────────────────────────────────────────────
async def _listar_tipo(update: Update, chat_id: int, tipo: str, emoji: str, label: str):
    """Lista registros de um tipo usando query filtrada — não varre histórico inteiro."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM registros WHERE chat_id=%s AND tipo=%s ORDER BY id DESC",
                (chat_id, tipo)
            )
            dados = [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)

    if not dados:
        await update.message.reply_text(f"📭 Nenhum {label.lower()} registrado ainda.")
        return
    total = sum(float(r["valor"]) for r in dados)
    cab   = (
        f"{emoji} *{label}s*  ({len(dados)})  —  total: {fmt(total)}\n"
        f"{SEP}\n\n"
    )
    linhas = "\n".join(
        f"• {r['data']}  {r['hora']}  ·  {fmt(r['valor'])}  ·  {r['descricao']}  ·  {r['destino']}"
        for r in dados
    )
    for parte in enviar_em_partes(cab + linhas):
        await update.message.reply_text(parte, parse_mode=ParseMode.MARKDOWN)


async def cmd_entradas(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    await _listar_tipo(update, get_chat_id_efetivo(update.effective_chat.id, conta_ativa),
                       "deposito", "🟢", "Entrada")

async def cmd_despesas(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    await _listar_tipo(update, get_chat_id_efetivo(update.effective_chat.id, conta_ativa),
                       "despesa", "🔴", "Despesa")

async def cmd_pixs(update: Update, context):
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    if not await verificar_acesso(update):
        return
    await _listar_tipo(update, get_chat_id_efetivo(update.effective_chat.id, conta_ativa),
                       "pix", "🔵", "Pix")
