"""
main.py — Entry point do FinBot v2.
"""
import asyncio
import logging
from collections import defaultdict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           CallbackQueryHandler, ContextTypes, filters)
from telegram.constants import ParseMode
from telegram.error import TelegramError

from config import TOKEN, FUSO, EMOJI, LABEL
from database import (init_pool, init_db, db_ativar_licenca, db_inserir_registro,
                      db_saldo_agregado, db_atualizar_registro, get_conn, release_conn)
from helpers import fmt, parsear_valor, fmt_registro, agora_br
from middleware import (verificar_acesso, get_chat_id_efetivo, cache_invalidar,
                        verificar_licenca_cache, estado_novo, limpar_estados_expirados)
from keyboards import teclado_tipo
from nlp import interpretar_frase, resumo_nlp
from server import keep_alive
from demo import popular_conta_demo

from handlers.core import (cmd_start, cmd_ajuda, cmd_cancelar, cmd_home,
                             cmd_saldo, cmd_hoje, cmd_mes, cmd_extrato,
                             cmd_entradas, cmd_despesas, cmd_pixs, menu_principal)
from handlers.registros import (cmd_desfazer, cmd_editar, cmd_retirar,
                                  handle_registros_callback)
from handlers.contas import (cmd_apagar, cmd_pendentes, cmd_pago,
                               handle_contas_callback, handle_contas_texto)
from handlers.admin import cmd_gerar_key, cmd_revogar
from handlers.broadcast import cmd_mensagem
from handlers.investimentos import (cmd_investimentos, cmd_inv_add, cmd_inv_del,
                                     handle_inv_callback, handle_inv_texto)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# A1 — Lock por chat_id para proteger acesso ao bot_data
_user_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# C5 — Rate limiting: máx 10 mensagens por usuário em 10 segundos
_rate_counters: dict[int, list] = defaultdict(list)
_RATE_LIMIT    = 10   # mensagens
_RATE_WINDOW   = 10   # segundos

# B5 — Lock para geração de conta demo (evita double-click)
_demo_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def _check_rate_limit(chat_id: int) -> bool:
    """Retorna True se usuário está dentro do limite. False se excedeu."""
    import time
    agora  = time.time()
    janela = _rate_counters[chat_id]
    # Remove registros fora da janela
    _rate_counters[chat_id] = [t for t in janela if agora - t < _RATE_WINDOW]
    if len(_rate_counters[chat_id]) >= _RATE_LIMIT:
        return False
    _rate_counters[chat_id].append(agora)
    return True


# ── Job periódico ─────────────────────────────────────────────────────────────
async def _job_limpar_estados(context: ContextTypes.DEFAULT_TYPE):
    """Roda a cada 5 minutos via JobQueue — não a cada mensagem."""
    estados = context.bot_data.get("estados", {})
    n = limpar_estados_expirados(estados)
    if n:
        logger.info("Job: %d estados expirados removidos", n)


# ── /conta ────────────────────────────────────────────────────────────────────
async def cmd_conta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await verificar_acesso(update):
        return
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    chat_id     = update.effective_chat.id
    args        = context.args

    if not args or args[0] not in ("1", "2"):
        atual = conta_ativa.get(chat_id, 1)
        await update.message.reply_text(
            f"Você está na conta *{atual}*.\n\n"
            "Use /conta 1 — conta real\n"
            "Use /conta 2 — demonstração",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    num = int(args[0])
    conta_ativa[chat_id] = num
    if num == 2:
        # B5 — lock por chat_id evita double-click gerando dados duplicados
        async with _demo_locks[chat_id]:
            await update.message.reply_text("🧪 Gerando dados de demonstração...")
            try:
                await asyncio.to_thread(popular_conta_demo, chat_id)
                await update.message.reply_text(
                    "✅ Conta demo ativa. Use /home para ver o painel.\nUse /conta 1 para voltar."
                )
            except Exception as e:
                logger.error("Erro ao gerar demo. chat_id=%d: %s", chat_id, e)
                await update.message.reply_text(f"⚠️ Erro ao gerar demo: {e}")
    else:
        await update.message.reply_text("✅ Conta real ativada.")


# ── Handler de texto principal ────────────────────────────────────────────────
async def handle_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    # C5 — Rate limiting: bloqueia spam antes de qualquer processamento
    if not _check_rate_limit(chat_id):
        try:
            await update.message.reply_text(
                "⚠️ Muitas mensagens em pouco tempo. Aguarde alguns segundos."
            )
        except TelegramError:
            pass
        return

    # A1 — Lock por chat_id: garante que mensagens do mesmo usuário
    #       não corrompam o estado uma da outra se chegarem em paralelo
    async with _user_locks[chat_id]:
        await _handle_texto_locked(update, context, chat_id)


async def _handle_texto_locked(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Lógica principal do handle_texto — executa dentro do lock do chat_id."""
    texto       = update.message.text.strip()
    estados     = context.bot_data.setdefault("estados", {})
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    estado      = estados.get(chat_id, {})
    etapa       = estado.get("etapa")
    chat_id_ef  = get_chat_id_efetivo(chat_id, conta_ativa)

    # ── Ativação de licença ───────────────────────────────────────────────────
    if etapa == "aguardando_key":
        resultado = db_ativar_licenca(texto, chat_id)
        if resultado == "ok":
            estados.pop(chat_id, None)
            cache_invalidar(chat_id)
            uname = update.effective_user.username or update.effective_user.first_name or ""
            conn  = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE licencas SET username=%s WHERE chat_id=%s", (uname, chat_id))
                conn.commit()
            finally:
                release_conn(conn)
            try:
                await update.message.reply_text(
                    "✅ *Acesso liberado!*\n\n" + menu_principal(),
                    parse_mode=ParseMode.MARKDOWN
                )
            except TelegramError as e:
                logger.warning("TelegramError ao enviar menu. chat_id=%d: %s", chat_id, e)
        elif resultado == "expirada":
            await update.message.reply_text("⚠️ Essa chave já expirou. Renove com @okamaji.")
        elif resultado == "ja_usada":
            await update.message.reply_text("❌ Essa chave já está sendo usada em outra conta.")
        else:
            await update.message.reply_text("❌ Chave inválida. Tente novamente ou contate @okamaji.")
        return

    # ── Verificação de licença via cache ──────────────────────────────────────
    status = verificar_licenca_cache(chat_id)
    if status == "expirada":
        await update.message.reply_text("⚠️ Seu plano expirou! Renove com @okamaji.")
        return
    if status == "invalida":
        estados[chat_id] = estado_novo({"etapa": "aguardando_key"})
        await update.message.reply_text("🔒 Insira sua chave de acesso:")
        return

    # ── Fluxo de investimentos ────────────────────────────────────────────────
    if etapa and etapa.startswith("inv_"):
        if await handle_inv_texto(update, estado, chat_id, chat_id_ef, estados):
            return

    # ── Fluxo de contas a pagar ───────────────────────────────────────────────
    if etapa in ("pagar_nome", "pagar_valor", "pagar_vencimento", "pagar_banco"):
        if await handle_contas_texto(update, estado, chat_id, chat_id_ef, estados):
            return

    # ── Fluxo de edição: descrição ────────────────────────────────────────────
    if etapa == "aguardando_descricao":
        estados[chat_id]["descricao"] = texto
        estados[chat_id]["etapa"]     = "aguardando_destino"
        estados[chat_id]["_ts"]       = agora_br().timestamp()
        perguntas = {
            "despesa":  "📍 Para onde foi? _ex: Nubank, iFood_",
            "deposito": "📍 De onde veio? _ex: salário, rendimento_",
            "pix":      "📍 Para quem? _ex: João, conta Itaú_",
        }
        await update.message.reply_text(perguntas[estado["tipo"]], parse_mode=ParseMode.MARKDOWN)
        return

    if etapa == "aguardando_destino":
        d = estados.pop(chat_id)
        r = db_inserir_registro(chat_id_ef, d["tipo"], d["valor"], d["descricao"], texto)
        s = db_saldo_agregado(chat_id_ef)
        try:
            await update.message.reply_text(
                f"✅ *Registrado!*\n\n{fmt_registro(r)}\n\n{'─'*28}\n"
                f"💰 *Saldo atual: {fmt(s['saldo'])}*\n"
                f"🟢 Entradas: {fmt(s['entradas'])}  🔴 Saídas: {fmt(s['despesas']+s['pixs'])}",
                parse_mode=ParseMode.MARKDOWN
            )
        except TelegramError as e:
            logger.warning("TelegramError ao confirmar registro. chat_id=%d: %s", chat_id, e)
            await update.message.reply_text("✅ Registrado com sucesso!")
        return

    if etapa == "editar_valor":
        reg_id = estado["reg_id"]
        campo  = estado["campo"]
        if campo == "valor":
            novo = parsear_valor(texto)
            if novo is None:
                await update.message.reply_text(
                    "❌ Valor inválido. Tente: `150` · `1460,90`",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            r = db_atualizar_registro(reg_id, "valor", novo)
        else:
            r = db_atualizar_registro(reg_id, campo, texto)
        estados.pop(chat_id, None)
        if not r:
            await update.message.reply_text("⚠️ Registro não encontrado.")
            return
        await update.message.reply_text(
            f"✅ *Registro atualizado!*\n\n{fmt_registro(r)}",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── NLP ───────────────────────────────────────────────────────────────────
    resultado_nlp = interpretar_frase(texto)
    if resultado_nlp:
        estados[chat_id] = estado_novo({"etapa": "nlp_confirmar", "nlp": resultado_nlp})
        teclado = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Confirmar", callback_data="nlp_confirmar"),
            InlineKeyboardButton("✏️ Corrigir",  callback_data="nlp_corrigir"),
            InlineKeyboardButton("❌ Cancelar",  callback_data="nlp_cancelar"),
        ]])
        await update.message.reply_text(
            resumo_nlp(resultado_nlp, fmt),
            reply_markup=teclado,
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Valor puro → escolher tipo ────────────────────────────────────────────
    valor = parsear_valor(texto)
    if valor is None:
        await update.message.reply_text(
            "💡 Envie um valor para registrar:\n`50` · `1460` · `1460,90`\n\n"
            "Ou diga naturalmente:\n_paguei 45 mercado_\n_recebi 1200 salario_\n_gastei 20 uber_",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    estados[chat_id] = estado_novo({"etapa": "aguardando_tipo", "valor": valor})
    await update.message.reply_text(
        f"💵 Valor: *{fmt(valor)}*\n\nQual o tipo?",
        reply_markup=teclado_tipo(),
        parse_mode=ParseMode.MARKDOWN
    )


# ── Handler de callbacks principal ───────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query       = update.callback_query
    await query.answer()
    chat_id     = query.message.chat_id
    data        = query.data
    estados     = context.bot_data.setdefault("estados", {})
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    chat_id_ef  = get_chat_id_efetivo(chat_id, conta_ativa)

    # ── NLP ───────────────────────────────────────────────────────────────────
    if data == "nlp_confirmar":
        estado = estados.get(chat_id, {})
        nlp    = estado.get("nlp")
        if not nlp:
            await query.edit_message_text("⚠️ Sessão expirada. Envie a frase novamente.")
            return
        estados.pop(chat_id, None)
        r = db_inserir_registro(chat_id_ef, nlp["tipo"], nlp["valor"],
                                 nlp["descricao"], nlp["destino"],
                                 metodo=nlp.get("metodo_pagamento"), origem="nlp")
        s = db_saldo_agregado(chat_id_ef)
        await query.edit_message_text(
            f"✅ *Registrado!*\n\n{fmt_registro(r)}\n\n{'─'*28}\n"
            f"💰 *Saldo atual: {fmt(s['saldo'])}*",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data == "nlp_corrigir":
        estado = estados.get(chat_id, {})
        nlp    = estado.get("nlp")
        if not nlp:
            await query.edit_message_text("⚠️ Sessão expirada.")
            return
        estados[chat_id] = estado_novo({
            "etapa": "aguardando_descricao",
            "tipo":  nlp["tipo"],
            "valor": nlp["valor"],
        })
        await query.edit_message_text(
            f"✏️ Vamos corrigir.\n\n💵 Valor: *{fmt(nlp['valor'])}*\n\n📝 Qual a descrição?",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data == "nlp_cancelar":
        estados.pop(chat_id, None)
        await query.edit_message_text("❌ Registro cancelado.")
        return

    # ── Seleção de tipo ───────────────────────────────────────────────────────
    if data.startswith("tipo:"):
        dado   = data.replace("tipo:", "")
        estado = estados.get(chat_id)
        if dado == "cancelar":
            estados.pop(chat_id, None)
            await query.edit_message_text("❌ Registro cancelado.")
            return
        if not estado:
            await query.edit_message_text("⚠️ Sessão expirada. Envie o valor novamente.")
            return
        estados[chat_id]["tipo"]  = dado
        estados[chat_id]["etapa"] = "aguardando_descricao"
        estados[chat_id]["_ts"]   = agora_br().timestamp()
        perguntas = {
            "despesa":  "📝 O que foi? _ex: aluguel, conta de luz_",
            "deposito": "📝 O que foi? _ex: salário, rendimento_",
            "pix":      "📝 O que foi? _ex: aluguel, transferência_",
        }
        await query.edit_message_text(
            f"{EMOJI[dado]} *{LABEL[dado]}* — {fmt(estado['valor'])}\n\n{perguntas[dado]}",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # ── Investimentos ─────────────────────────────────────────────────────────
    if await handle_inv_callback(query, chat_id, data, estados):
        return

    # ── Contas a pagar ────────────────────────────────────────────────────────
    if await handle_contas_callback(query, chat_id, data):
        return

    # ── Registros ─────────────────────────────────────────────────────────────
    if await handle_registros_callback(query, chat_id, data, estados, conta_ativa):
        return


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    init_pool()
    init_db()
    keep_alive()

    app = Application.builder().token(TOKEN).build()

    # Job de limpeza de estados — a cada 5 minutos
    app.job_queue.run_repeating(_job_limpar_estados, interval=300, first=60)

    cmds = [
        ("start",         cmd_start),
        ("ajuda",         cmd_ajuda),
        ("cancelar",      cmd_cancelar),
        ("home",          cmd_home),
        ("saldo",         cmd_saldo),
        ("hoje",          cmd_hoje),
        ("mes",           cmd_mes),
        ("extrato",       cmd_extrato),
        ("entradas",      cmd_entradas),
        ("despesas",      cmd_despesas),
        ("pixs",          cmd_pixs),
        ("desfazer",      cmd_desfazer),
        ("editar",        cmd_editar),
        ("retirar",       cmd_retirar),
        ("apagar",        cmd_apagar),
        ("pendentes",     cmd_pendentes),
        ("pago",          cmd_pago),
        ("investimentos", cmd_investimentos),
        ("inv_add",       cmd_inv_add),
        ("inv_del",       cmd_inv_del),
        ("conta",         cmd_conta),
        ("gerar_key",     cmd_gerar_key),
        ("revogar",       cmd_revogar),
        ("mensagem",      cmd_mensagem),
    ]
    for nome, func in cmds:
        app.add_handler(CommandHandler(nome, func))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_texto))

    logger.info("💰 FinBot v2 iniciado!")
    await app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    asyncio.run(main())
