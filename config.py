import os
from zoneinfo import ZoneInfo

# ── Variáveis obrigatórias ────────────────────────────────────────────────────
TOKEN    = os.environ.get("TOKEN", "")
DATABASE = os.environ.get("DATABASE_URL", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

if not TOKEN:
    raise ValueError("❌ Variável de ambiente TOKEN não configurada!")
if not DATABASE:
    raise ValueError("❌ Variável de ambiente DATABASE_URL não configurada!")

# ── Fuso e tempo ──────────────────────────────────────────────────────────────
FUSO = ZoneInfo("America/Sao_Paulo")

# ── Cache de licenças ─────────────────────────────────────────────────────────
CACHE_TTL     = int(os.environ.get("CACHE_TTL",     "300"))   # segundos (5 min)
CACHE_MAXSIZE = int(os.environ.get("CACHE_MAXSIZE", "2000"))  # 2x maior para 1k users

# ── Pool de conexões ─────────────────────────────────────────────────────────
# Para 1k users: mínimo 5, máximo 20 (Railway suporta bem)
DB_POOL_MIN = int(os.environ.get("DB_POOL_MIN", "5"))
DB_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "20"))

# ── Limites ───────────────────────────────────────────────────────────────────
VALOR_MAX           = 999_999.00
ESTADOS_TTL_MINUTOS = int(os.environ.get("ESTADOS_TTL_MINUTOS", "30"))
DEMO_NUM_REGISTROS  = 18

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

# Mapeamento curto para callback_data (evita estouro de 64 bytes)
METODO_SHORT = {
    "Pix":               "pix",
    "Transferência":     "transf",
    "Cartão de Crédito": "credito",
    "Cartão de Débito":  "debito",
}
SHORT_METODO = {v: k for k, v in METODO_SHORT.items()}  # inverso
