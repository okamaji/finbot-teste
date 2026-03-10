"""
config.py — Configurações do FinBot.
"""
import os
import re as _re
from zoneinfo import ZoneInfo

# ── Variáveis obrigatórias ────────────────────────────────────────────────────
TOKEN    = os.environ.get("TOKEN", "")
DATABASE = os.environ.get("DATABASE_URL", "")

_admin_raw = os.environ.get("ADMIN_ID", "0")
ADMIN_ID   = int(_re.sub(r"[^0-9]", "", _admin_raw) or "0")

if not TOKEN:
    raise ValueError("❌ Variável de ambiente TOKEN não configurada!")
if not DATABASE:
    raise ValueError("❌ Variável de ambiente DATABASE_URL não configurada!")

# ── Fuso e tempo ──────────────────────────────────────────────────────────────
FUSO = ZoneInfo("America/Sao_Paulo")

# ── Cache de licenças ─────────────────────────────────────────────────────────
CACHE_TTL     = int(os.environ.get("CACHE_TTL",     "300"))
CACHE_MAXSIZE = int(os.environ.get("CACHE_MAXSIZE", "2000"))

# ── Pool de conexões ──────────────────────────────────────────────────────────
DB_POOL_MIN = int(os.environ.get("DB_POOL_MIN", "5"))
DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "20"))

# ── Limites ───────────────────────────────────────────────────────────────────
VALOR_MAX           = 999_999.00
ESTADOS_TTL_MINUTOS = int(os.environ.get("ESTADOS_TTL_MINUTOS", "30"))
DEMO_NUM_REGISTROS  = 18

# ── Rate limiting ─────────────────────────────────────────────────────────────
RATE_MSG_INTERVALO  = 5    # segundos mínimos entre mensagens
RATE_SPAM_AGRESSIVO = 10   # interações em <1s para spam agressivo

# ── Retenção de dados ─────────────────────────────────────────────────────────
DIAS_GRACA_RENOVACAO = 30
DIAS_DELETE_DADOS    = 40

# ── Textuais ──────────────────────────────────────────────────────────────────
MESES = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
         "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

EMOJI = {"despesa": "🔴", "deposito": "🟢", "pix": "🔵", "transferencia": "🔁"}
LABEL = {"despesa": "Despesa", "deposito": "Depósito", "pix": "Pix", "transferencia": "Transferência"}

METODOS_PAGAMENTO = ["Pix", "Transferência", "Cartão de Crédito", "Cartão de Débito"]
EMOJI_METODO = {
    "Pix":               "🔵",
    "Transferência":     "🔁",
    "Cartão de Crédito": "💳",
    "Cartão de Débito":  "🏧",
}

METODO_SHORT = {
    "Pix":               "pix",
    "Transferência":     "transf",
    "Cartão de Crédito": "credito",
    "Cartão de Débito":  "debito",
}
SHORT_METODO = {v: k for k, v in METODO_SHORT.items()}
