"""
main.py — Entry point do FinBot v2.

Novidades:
- Rate limiting avançado: 1 msg/5s (silencioso) + spam agressivo → revoga licença
- Termos obrigatórios no /start e pós-ativação de key
- /key — registrar/trocar licença sem precisar de /start
- /termos — ver termos de uso
- Mensagem "invalidada" (key bloqueada por 3 tentativas)
- Novos comandos admin: /gerarkey, /stats, /users, /veruser, /mensagemuser
- /revogar all — revogar todas as licenças de uma vez
- Job diário de retenção de dados (40 dias após expiração)
- asyncio.run() compatível com Python 3.12+
- cancelar_fluxo — callback universal de cancelamento
- /manutencao — ativa modo manutenção (bloqueia todos os usuários)
- /normal — desativa modo manutenção e restaura o bot
"""
import asyncio
import logging
from collections import defaultdict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           CallbackQueryHandler, ContextTypes, filters)
from telegram.constants import ParseMode
from telegram.error import TelegramError

from config import TOKEN, DATABASE, FUSO, EMOJI, LABEL, ADMIN_ID
from database import (init_pool, init_db, db_ativar_licenca, db_inserir_registro,
                      db_saldo_agregado, db_atualizar_registro,
                      db_aceitar_termos, db_verificar_termos,
                      db_limpar_dados_expirados, get_conn, release_conn)
from helpers import fmt, parsear_valor, fmt_registro, agora_br
from middleware import (verificar_acesso, get_chat_id_efetivo, cache_invalidar,
                        verificar_licenca_cache, estado_novo,
                        limpar_estados_expirados, checar_spam)
from keyboards import teclado_tipo, teclado_nlp, teclado_termos
from nlp import interpretar_frase, resumo_nlp
from server import keep_alive
from demo import popular_conta_demo

from handlers.core import (cmd_start, cmd_ajuda, cmd_cancelar, cmd_home,
                             cmd_saldo, cmd_hoje, cmd_mes, cmd_extrato,
                             cmd_entradas, cmd_despesas, cmd_pixs,
                             cmd_key, cmd_termos, menu_principal, TERMOS_TEXTO)
from handlers.registros import (cmd_desfazer, cmd_editar, cmd_retirar,
                                  handle_registros_callback)
from handlers.contas import (cmd_apagar, cmd_pendentes, cmd_pago,
                               handle_contas_callback, handle_contas_texto)
from handlers.admin import (cmd_gerar_key, cmd_gerarkey, cmd_revogar,
                              cmd_veruser, cmd_stats, cmd_users, cmd_admin)
from handlers.broadcast import cmd_mensagem, cmd_mensagemuser
from handlers.investimentos import (cmd_investimentos, cmd_inv_add, cmd_inv_del,
                                     handle_inv_callback, handle_inv_texto)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Modo manutenção ───────────────────────────────────────────────────────────
MANUTENCAO: bool = False

_user_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_demo_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


# ── Jobs periódicos ────────────────────────────────────────────────────────────
async def _job_limpar_estados(context: ContextTypes.DEFAULT_TYPE):
    estados = context.bot_data.get("estados", {})
    n = limpar_estados_expirados(estados)
    if n:
        logger.info("Job: %d estados expirados removidos", n)


async def _job_limpar_dados_expirados(context: ContextTypes.DEFAULT_TYPE):
    """Roda diariamente — deleta dados de usuários que não renovaram em 40 dias."""
    n = db_limpar_dados_expirados(40)
    if n:
        logger.info("Job retenção: %d usuários deletados", n)


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

    # Modo manutenção — bloqueia todos exceto admin
    if MANUTENCAO and chat_id != ADMIN_ID:
        await update.message.reply_text("🔧 *FinBot em manutenção!*\nVoltamos em breve.", parse_mode=ParseMode.MARKDOWN)
        return

    # Rate limiting
    if await checar_spam(update):
        return

    async with _user_locks[chat_id]:
        await _handle_texto_locked(update, context, chat_id)


async def _handle_texto_locked(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    texto       = update.message.text.strip()
    estados     = context.bot_data.setdefault("estados", {})
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    estado      = estados.get(chat_id, {})
    etapa       = estado.get("etapa")
    chat_id_ef  = get_chat_id_efetivo(chat_id, conta_ativa)

    # ── Ativação de licença ───────────────────────────────────────────────────
    if etapa == "aguardando_key":
        resultado = db_ativar_licenca(texto, chat_id)
        msgs = {
            "expirada":   "⚠️ Essa chave já expirou. Renove com @okamaji.",
            "ja_usada":   "❌ Essa chave já está sendo usada em outra conta.",
            "invalidada": "🚫 Essa chave foi invalidada por tentativas inválidas. Contate @okamaji.",
            "invalida":   "❌ Chave inválida. Tente novamente ou contate @okamaji.",
        }
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
            if not db_verificar_termos(chat_id):
                await update.message.reply_text(
                    "✅ *Acesso liberado!*\n\nAntes de continuar, leia e aceite os termos:",
                    parse_mode=ParseMode.MARKDOWN
                )
                await update.message.reply_text(
                    f"{TERMOS_TEXTO}\n\nVocê precisa aceitar os termos para continuar.",
                    reply_markup=teclado_termos()
                )
            else:
                await update.message.reply_text(
                    "✅ *Acesso liberado!*\n\n" + menu_principal(),
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await update.message.reply_text(msgs.get(resultado, "❌ Erro desconhecido."))
        return

    # ── Verificação de licença ────────────────────────────────────────────────
    status = verificar_licenca_cache(chat_id)
    if status == "expirada":
        await update.message.reply_text(
            "⚠️ *Sua licença expirou!*\n\nUse /key para registrar uma nova chave.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    if status == "invalida":
        estados[chat_id] = estado_novo({"etapa": "aguardando_key"})
        await update.message.reply_text(
            "🔒 *Sua licença não está ativa.*\n\nUse /key para registrar uma nova licença.",
            parse_mode=ParseMode.MARKDOWN
        )
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
                f"✅ *Registrado!*\n\n{fmt_registro(r)}\n\n{'─'*19}\n"
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
        await update.message.reply_text(
            resumo_nlp(resultado_nlp, fmt),
            reply_markup=teclado_nlp(),
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

    # Modo manutenção — bloqueia todos exceto admin
    if MANUTENCAO and chat_id != ADMIN_ID:
        await query.answer("🔧 FinBot em manutenção! Voltamos em breve.", show_alert=True)
        return

    estados     = context.bot_data.setdefault("estados", {})
    conta_ativa = context.bot_data.setdefault("conta_ativa", {})
    chat_id_ef  = get_chat_id_efetivo(chat_id, conta_ativa)

    # ── Cancelar fluxo universal ──────────────────────────────────────────────
    if data == "cancelar_fluxo":
        estados.pop(chat_id, None)
        await query.edit_message_text("❌ Ação cancelada.")
        return

    # ── Termos ────────────────────────────────────────────────────────────────
    if data == "termos_aceitar":
        db_aceitar_termos(chat_id)
        await query.edit_message_text(
            "✅ *Termos aceitos!*\n\n" + menu_principal(),
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data == "termos_recusar":
        await query.edit_message_text(
            "❌ *Termos não aceitos.*\n\n"
            "Você não poderá usar o bot sem aceitar os termos.\n"
            "Use /termos para rever e /start para tentar novamente."
        )
        return

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
            f"✅ *Registrado!*\n\n{fmt_registro(r)}\n\n{'─'*19}\n"
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

    # ── Seleção de tipo ───────────────────────────────────────────────────────
    if data.startswith("tipo:"):
        dado   = data.replace("tipo:", "")
        estado = estados.get(chat_id)
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


# ── /manutencao e /normal ─────────────────────────────────────────────────────
async def cmd_manutencao(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ativa o modo manutenção — bloqueia todos os usuários."""
    global MANUTENCAO
    if update.effective_chat.id != ADMIN_ID:
        return
    MANUTENCAO = True
    await update.message.reply_text(
        "🔧 *Modo manutenção ATIVADO!*\n\n"
        "Todos os usuários receberão a mensagem de manutenção.\n"
        "Use /normal para restaurar o bot.",
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info("Modo manutenção ATIVADO pelo admin.")


async def cmd_normal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Desativa o modo manutenção — restaura o bot."""
    global MANUTENCAO
    if update.effective_chat.id != ADMIN_ID:
        return
    MANUTENCAO = False
    await update.message.reply_text(
        "✅ *Modo manutenção DESATIVADO!*\n\n"
        "O bot está funcionando normalmente.",
        parse_mode=ParseMode.MARKDOWN
    )
    logger.info("Modo manutenção DESATIVADO pelo admin.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not TOKEN:
        raise ValueError("❌ TOKEN não configurado!")
    if not DATABASE:
        raise ValueError("❌ DATABASE_URL não configurada!")

    init_pool()
    init_db()
    keep_alive()

    app = Application.builder().token(TOKEN).build()

    # Jobs
    app.job_queue.run_repeating(_job_limpar_estados,         interval=300,   first=60)
    app.job_queue.run_repeating(_job_limpar_dados_expirados, interval=86400, first=3600)

    cmds = [
        # Sempre permitidos (sem verificar licença)
        ("start",         cmd_start),
        ("key",           cmd_key),
        ("termos",        cmd_termos),
        # Usuário
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
        # Admin
        ("gerarkey",      cmd_gerarkey),
        ("gerar_key",     cmd_gerar_key),   # alias legado
        ("revogar",       cmd_revogar),
        ("veruser",       cmd_veruser),
        ("stats",         cmd_stats),
        ("users",         cmd_users),
        ("mensagem",      cmd_mensagem),
        ("mensagemuser",  cmd_mensagemuser),
        ("admin",         cmd_admin),
        ("manutencao",    cmd_manutencao),
        ("normal",        cmd_normal),
    ]
    for nome, func in cmds:
        app.add_handler(CommandHandler(nome, func))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_texto))

    async def error_handler(update, context):
        logger.error("❌ Exceção no handler: %s", context.error, exc_info=context.error)
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text("⚠️ Erro interno. Tente novamente.")
            except Exception:
                pass

    app.add_error_handler(error_handler)

    logger.info("💰 FinBot v2 iniciado!")

    async def _run():
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        # Aguarda indefinidamente até receber sinal de encerramento (SIGINT/SIGTERM)
        await asyncio.Event().wait()

    try:
        asyncio.run(_run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 FinBot encerrado.")


if __name__ == "__main__":
    main()
