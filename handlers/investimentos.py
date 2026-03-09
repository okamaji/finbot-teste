"""
handlers/investimentos.py — Carteira de investimentos com dados reais do BCB.

A5 — API do BCB rodada em asyncio.to_thread (não bloqueia o event loop)
B2 — Cache de cálculo por (inv_id, data) — recalcula uma vez por dia
"""
import re
import json
import asyncio
import logging
import urllib.request
from datetime import date, datetime
from threading import Lock

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from helpers import fmt, parsear_valor, enviar_em_partes
from database import db_investimentos, db_inserir_investimento, db_remover_investimento
from middleware import verificar_acesso, get_chat_id_efetivo

logger = logging.getLogger(__name__)

CACHE_HORAS    = 6
TIMEOUT_API    = 6
DIAS_UTEIS_ANO = 252

TIPOS_INV = ["CDB", "LCI", "LCA", "Tesouro Selic", "Tesouro IPCA+", "Poupança", "Ação/FII"]
EMOJI_TIPO = {
    "CDB":           "🏦",
    "LCI":           "🏡",
    "LCA":           "🌾",
    "Tesouro Selic": "🇧🇷",
    "Tesouro IPCA+": "📊",
    "Poupança":      "💰",
    "Ação/FII":      "📈",
}
_FALLBACK = {"cdi": 10.40, "selic": 10.50, "ipca": 4.83, "poupanca": 7.12}

SEP   = "─" * 30
_lock = Lock()
_cache_taxas:  dict[str, dict] = {}   # cache das taxas BCB
_cache_calculos: dict[tuple, dict] = {}  # B2 — cache de cálculos (inv_id, data)


def _cache_ok(chave: str) -> bool:
    e = _cache_taxas.get(chave)
    if not e or e["valor"] is None:
        return False
    return (datetime.now() - e["ts"]).seconds < CACHE_HORAS * 3600


def _set(chave: str, valor: float):
    _cache_taxas[chave] = {"valor": valor, "ts": datetime.now()}


def _get(chave: str) -> float | None:
    return _cache_taxas.get(chave, {}).get("valor")


# A5 — busca BCB em thread separada para não bloquear o event loop
def _bcb_sync(serie: int, n: int = 1) -> list[dict]:
    """Versão síncrona — chamada via asyncio.to_thread."""
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}"
           f"/dados/ultimos/{n}?formato=json")
    req = urllib.request.Request(url, headers={"User-Agent": "FinBot/2.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_API) as r:
        return json.loads(r.read())


async def _bcb(serie: int, n: int = 1) -> list[dict]:
    """Versão async — não bloqueia o event loop durante a chamada HTTP."""
    return await asyncio.to_thread(_bcb_sync, serie, n)


async def taxa_cdi() -> float:
    with _lock:
        if _cache_ok("cdi"): return _get("cdi")
    try:
        v   = float((await _bcb(12, 1))[0]["valor"])
        ano = round(((1 + v / 100) ** DIAS_UTEIS_ANO - 1) * 100, 4)
        with _lock: _set("cdi", ano)
        return ano
    except Exception as e:
        logger.warning("CDI falhou: %s → fallback", e)
        return _FALLBACK["cdi"]


async def taxa_selic() -> float:
    with _lock:
        if _cache_ok("selic"): return _get("selic")
    try:
        v   = float((await _bcb(11, 1))[0]["valor"])
        ano = round(((1 + v / 100) ** DIAS_UTEIS_ANO - 1) * 100, 4)
        with _lock: _set("selic", ano)
        return ano
    except Exception as e:
        logger.warning("Selic falhou: %s → fallback", e)
        return _FALLBACK["selic"]


async def taxa_ipca() -> float:
    with _lock:
        if _cache_ok("ipca"): return _get("ipca")
    try:
        dados = await _bcb(433, 12)
        acum  = 1.0
        for d in dados:
            acum *= (1 + float(d["valor"]) / 100)
        ano = round((acum - 1) * 100, 4)
        with _lock: _set("ipca", ano)
        return ano
    except Exception as e:
        logger.warning("IPCA falhou: %s → fallback", e)
        return _FALLBACK["ipca"]


async def taxa_poupanca() -> float:
    with _lock:
        if _cache_ok("poupanca"): return _get("poupanca")
    try:
        v   = float((await _bcb(196, 1))[0]["valor"])
        ano = round(((1 + v / 100) ** 12 - 1) * 100, 4)
        with _lock: _set("poupanca", ano)
        return ano
    except Exception as e:
        logger.warning("Poupança falhou: %s → fallback", e)
        return _FALLBACK["poupanca"]


async def _calcular(inv: dict) -> dict:
    """
    B2 — Cache de resultado por (inv_id, data de hoje).
    Recalcula no máximo uma vez por dia por investimento.
    """
    chave = (inv["id"], date.today())
    with _lock:
        if chave in _cache_calculos:
            return _cache_calculos[chave]

    tipo          = inv["tipo"]
    valor_inicial = float(inv["valor_inicial"])
    taxa_pct      = float(inv["taxa_cdi"])
    data_inicio   = (
        inv["data_inicio"] if isinstance(inv["data_inicio"], date)
        else datetime.strptime(str(inv["data_inicio"]), "%Y-%m-%d").date()
    )
    hoje       = date.today()
    dias_corr  = (hoje - data_inicio).days
    dias_uteis = max(round(dias_corr * DIAS_UTEIS_ANO / 365), 0)

    match tipo:
        case "CDB" | "LCI" | "LCA":
            base      = await taxa_cdi()
            efetiva   = taxa_pct / 100 * base
            taxa_desc = f"{taxa_pct:.0f}% CDI  ({base:.2f}% a.a.)"
        case "Tesouro Selic":
            base      = await taxa_selic()
            efetiva   = taxa_pct / 100 * base
            taxa_desc = f"{taxa_pct:.0f}% Selic  ({base:.2f}% a.a.)"
        case "Tesouro IPCA+":
            base      = await taxa_ipca()
            efetiva   = base + taxa_pct
            taxa_desc = f"IPCA+{taxa_pct:.2f}%  (IPCA={base:.2f}% a.a.)"
        case "Poupança":
            efetiva   = await taxa_poupanca()
            taxa_desc = f"Poupança  ({efetiva:.2f}% a.a.)"
        case _:
            resultado = {"valor_atual": valor_inicial, "rendimento": 0.0,
                         "rendimento_pct": 0.0, "dias": dias_corr, "taxa_desc": "rendimento variável"}
            with _lock: _cache_calculos[chave] = resultado
            return resultado

    if dias_uteis == 0 or efetiva == 0:
        resultado = {"valor_atual": valor_inicial, "rendimento": 0.0,
                     "rendimento_pct": 0.0, "dias": dias_corr, "taxa_desc": taxa_desc}
        with _lock: _cache_calculos[chave] = resultado
        return resultado

    fator       = (1 + efetiva / 100) ** (dias_uteis / DIAS_UTEIS_ANO)
    valor_atual = round(valor_inicial * fator, 2)
    rendimento  = round(valor_atual - valor_inicial, 2)
    rend_pct    = round((fator - 1) * 100, 4)
    resultado   = {"valor_atual": valor_atual, "rendimento": rendimento,
                   "rendimento_pct": rend_pct, "dias": dias_corr, "taxa_desc": taxa_desc}
    with _lock: _cache_calculos[chave] = resultado
    return resultado


async def _fmt_inv(inv: dict) -> str:
    emoji = EMOJI_TIPO.get(inv["tipo"], "💼")
    calc  = await _calcular(inv)
    venc  = f"\n📆 Vence: {inv['data_vencto']}" if inv.get("data_vencto") else ""
    rend  = (
        f"📈 Valor atual:   *{fmt(calc['valor_atual'])}*\n"
        f"✨ Rendimento:    *+{fmt(calc['rendimento'])}*  ({calc['rendimento_pct']:.2f}%)\n"
    ) if inv["tipo"] != "Ação/FII" else ""
    return (
        f"{emoji} *{inv['nome']}*  —  {inv['tipo']}\n"
        f"🏦 {inv['banco']}  ·  {calc['taxa_desc']}\n"
        f"💰 Aplicado: *{fmt(inv['valor_inicial'])}*{venc}\n"
        f"{rend}"
        f"📅 {calc['dias']} dias investido\n"
        f"🆔 ID: `{inv['id']}`"
    )


async def cmd_investimentos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id)
    invs    = db_investimentos(chat_id)

    if not invs:
        await update.message.reply_text(
            "📈 *Investimentos*\n\nNenhum investimento cadastrado.\n\nUse /inv\\_add para adicionar.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # A5/B2 — cálculos e taxas em paralelo, sem bloquear o event loop
    calculos = await asyncio.gather(*[_calcular(inv) for inv in invs])
    cdi, selic, ipca, poup = await asyncio.gather(
        taxa_cdi(), taxa_selic(), taxa_ipca(), taxa_poupanca()
    )

    total_aplicado = sum(float(i["valor_inicial"]) for i in invs)
    total_atual    = sum(c["valor_atual"] for c in calculos)
    total_rend     = total_atual - total_aplicado
    total_rend_pct = (total_rend / total_aplicado * 100) if total_aplicado > 0 else 0

    cabecalho = (
        f"📈 *Carteira de Investimentos*\n{SEP}\n\n"
        f"💰 Total aplicado:    *{fmt(total_aplicado)}*\n"
        f"📊 Valor atual:       *{fmt(total_atual)}*\n"
        f"✨ Rendimento total:  *+{fmt(total_rend)}*  ({total_rend_pct:.2f}%)\n\n"
        f"{SEP}\n📡 *Taxas de referência* _(BCB)_\n\n"
        f"🔵 CDI:    {cdi:.2f}% a.a.\n"
        f"🟢 Selic:  {selic:.2f}% a.a.\n"
        f"🔴 IPCA:   {ipca:.2f}% a.a. _(12m)_\n"
        f"💰 Poup:   {poup:.2f}% a.a.\n{SEP}\n\n"
    )
    textos_inv = await asyncio.gather(*[_fmt_inv(inv) for inv in invs])
    corpo  = f"\n\n{SEP}\n\n".join(textos_inv)
    rodape = f"\n\n{SEP}\n💡 /inv\\_add — adicionar  ·  /inv\\_del — remover"

    for parte in enviar_em_partes(cabecalho + corpo + rodape):
        await update.message.reply_text(parte, parse_mode=ParseMode.MARKDOWN)


async def cmd_inv_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await verificar_acesso(update):
        return
    estados = context.bot_data.setdefault("estados", {})
    chat_id = update.effective_chat.id
    estados[chat_id] = {"etapa": "inv_tipo"}
    teclado = InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"{EMOJI_TIPO[t]} {t}", callback_data=f"inv_tipo:{t}")]
         for t in TIPOS_INV]
        + [[InlineKeyboardButton("❌ Cancelar", callback_data="inv_cancelar")]]
    )
    await update.message.reply_text(
        "📈 *Novo Investimento*\n\nQual o tipo?",
        reply_markup=teclado,
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_inv_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await verificar_acesso(update):
        return
    chat_id = get_chat_id_efetivo(update.effective_chat.id)
    args    = context.args

    if args and args[0].isdigit():
        db_remover_investimento(int(args[0]))
        await update.message.reply_text(f"🗑️ Investimento #{args[0]} removido.")
        return

    invs = db_investimentos(chat_id)
    if not invs:
        await update.message.reply_text("📭 Nenhum investimento cadastrado.")
        return

    botoes = [
        [InlineKeyboardButton(
            f"{EMOJI_TIPO.get(i['tipo'], '💼')} {i['nome']}  —  {fmt(i['valor_inicial'])}",
            callback_data=f"inv_remover:{i['id']}"
        )] for i in invs
    ]
    botoes.append([InlineKeyboardButton("❌ Cancelar", callback_data="inv_cancelar")])
    await update.message.reply_text(
        "🗑️ *Qual investimento remover?*",
        reply_markup=InlineKeyboardMarkup(botoes),
        parse_mode=ParseMode.MARKDOWN
    )


async def handle_inv_callback(query, chat_id: int, data: str, estados: dict) -> bool:
    if data == "inv_cancelar":
        estados.pop(chat_id, None)
        await query.edit_message_text("❌ Operação cancelada.")
        return True

    if data.startswith("inv_tipo:"):
        tipo = data.replace("inv_tipo:", "")
        estados[chat_id] = {"etapa": "inv_nome", "tipo": tipo}
        await query.edit_message_text(
            f"{EMOJI_TIPO.get(tipo, '💼')} *{tipo}* selecionado.\n\n"
            f"📝 Qual o nome do investimento?\n_(ex: CDB Nubank 110%)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if data.startswith("inv_remover:"):
        inv_id = int(data.replace("inv_remover:", ""))
        db_remover_investimento(inv_id)
        await query.edit_message_text(f"🗑️ Investimento #{inv_id} removido com sucesso.")
        return True

    return False


async def handle_inv_texto(update: Update, estado: dict, chat_id: int,
                            chat_id_ef: int, estados: dict) -> bool:
    etapa = estado.get("etapa", "")
    texto = update.message.text.strip()

    if etapa == "inv_nome":
        estados[chat_id]["nome"]  = texto
        estados[chat_id]["etapa"] = "inv_valor"
        await update.message.reply_text(
            "💵 Qual o valor aplicado?\n_(ex: `1000` · `1.500,00`)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "inv_valor":
        valor = parsear_valor(texto)
        if valor is None:
            await update.message.reply_text(
                "❌ Valor inválido. Tente: `1000` ou `1.500,00`", parse_mode=ParseMode.MARKDOWN
            )
            return True
        estados[chat_id]["valor"] = valor
        tipo = estado.get("tipo", "")
        if tipo == "Poupança":
            estados[chat_id]["taxa_cdi"] = 100
            estados[chat_id]["etapa"]    = "inv_banco"
            await update.message.reply_text(
                "🏦 Em qual banco?\n_(ex: Caixa, Bradesco, Nubank)_", parse_mode=ParseMode.MARKDOWN
            )
        elif tipo == "Ação/FII":
            estados[chat_id]["taxa_cdi"] = 0
            estados[chat_id]["etapa"]    = "inv_banco"
            await update.message.reply_text(
                "🏦 Em qual corretora?\n_(ex: XP, Clear, Rico, BTG)_", parse_mode=ParseMode.MARKDOWN
            )
        elif tipo == "Tesouro IPCA+":
            ipca = await taxa_ipca()
            estados[chat_id]["etapa"] = "inv_taxa"
            await update.message.reply_text(
                f"📊 Qual o spread sobre o IPCA?\n_(ex: `6.5` para IPCA+6,5%)_\n\n"
                f"📡 IPCA atual: *{ipca:.2f}% a.a.*",
                parse_mode=ParseMode.MARKDOWN
            )
        elif tipo == "Tesouro Selic":
            selic = await taxa_selic()
            estados[chat_id]["etapa"] = "inv_taxa"
            await update.message.reply_text(
                f"📊 Qual o percentual da Selic?\n_(ex: `100` para 100% Selic)_\n\n"
                f"📡 Selic atual: *{selic:.2f}% a.a.*",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            cdi = await taxa_cdi()
            estados[chat_id]["etapa"] = "inv_taxa"
            await update.message.reply_text(
                f"📊 Qual o percentual do CDI?\n_(ex: `110` para 110% CDI)_\n\n"
                f"📡 CDI atual: *{cdi:.2f}% a.a.*",
                parse_mode=ParseMode.MARKDOWN
            )
        return True

    if etapa == "inv_taxa":
        try:
            taxa = float(texto.replace(",", ".").replace("%", ""))
            if not 0 < taxa <= 500:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ Taxa inválida.\nEx: `110` para 110% CDI  |  `6.5` para IPCA+6,5%",
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        estados[chat_id]["taxa_cdi"] = taxa
        estados[chat_id]["etapa"]    = "inv_banco"
        await update.message.reply_text(
            "🏦 Em qual banco/corretora?\n_(ex: Nubank, XP, BTG, Inter)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "inv_banco":
        estados[chat_id]["banco"] = texto
        estados[chat_id]["etapa"] = "inv_data"
        await update.message.reply_text(
            "📅 Data de início?\n_(ex: `01/01/2025` ou `hoje`)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "inv_data":
        data_inicio = _parse_data(texto)
        if data_inicio is None:
            await update.message.reply_text(
                "❌ Data inválida. Use `dd/mm/aaaa` ou `hoje`", parse_mode=ParseMode.MARKDOWN
            )
            return True
        estados[chat_id]["data_inicio"] = data_inicio
        estados[chat_id]["etapa"]       = "inv_vencto"
        await update.message.reply_text(
            "📆 Data de vencimento?\n_(ex: `31/12/2026` ou `sem` se não tiver)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    if etapa == "inv_vencto":
        if texto.lower() in ("sem", "nao", "não", "-", "s"):
            data_vencto = None
        else:
            data_vencto = _parse_data(texto)
            if data_vencto is None:
                await update.message.reply_text(
                    "❌ Data inválida. Use `dd/mm/aaaa` ou `sem`", parse_mode=ParseMode.MARKDOWN
                )
                return True
        d   = estados.pop(chat_id)
        inv = db_inserir_investimento(
            chat_id_ef, d["nome"], d.get("tipo", "CDB"),
            d["valor"], d.get("taxa_cdi", 100),
            d["data_inicio"], data_vencto, d["banco"]
        )
        await update.message.reply_text(
            f"✅ *Investimento cadastrado!*\n\n{_fmt_inv(inv)}\n\n"
            f"_Use /investimentos para ver toda a carteira._",
            parse_mode=ParseMode.MARKDOWN
        )
        return True

    return False


def _parse_data(texto: str) -> str | None:
    if texto.lower() == "hoje":
        return date.today().strftime("%Y-%m-%d")
    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", texto)
    if not m:
        return None
    d, mo, a = m.groups()
    a = "20" + a if len(a) == 2 else a
    return f"{a}-{int(mo):02d}-{int(d):02d}"
