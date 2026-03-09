import re
from datetime import datetime
from config import FUSO, EMOJI, LABEL, EMOJI_METODO, VALOR_MAX


def agora_br() -> datetime:
    return datetime.now(FUSO)


def fmt(v) -> str:
    """Formata float para R$ 1.234,56"""
    v       = float(v)
    partes  = f"{v:,.2f}".split(".")
    inteiro = partes[0].replace(",", ".")
    return "R$ " + inteiro + "," + partes[1]


def parsear_valor(texto: str) -> float | None:
    """
    Converte string para float no padrão brasileiro.
    Aceita: 50 · 1460 · 1460,90 · 1.460,90 · R$50 · R$ 50
    Rejeita: zero, negativo, acima de VALOR_MAX, formatos ambíguos.
    """
    if not texto:
        return None

    # Remove R$, espaços e caracteres invisíveis
    t = re.sub(r"[R$\s\u00a0]", "", texto.strip())

    # Rejeita vazio ou sem dígito
    if not t or not re.search(r"\d", t):
        return None

    # Rejeita múltiplos pontos sem vírgula (1.2.3)
    if t.count(".") > 1 and "," not in t:
        return None
    # Rejeita múltiplas vírgulas (1,2,3)
    if t.count(",") > 1:
        return None

    # Remove ponto/vírgula soltos no final (10. ou 10,)
    if t.endswith((".","," )):
        t = t[:-1]
        if not t:
            return None

    # Formato BR com milhar: 1.234 ou 1.234,56
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d{1,2})?$", t):
        t = t.replace(".", "").replace(",", ".")
    # Formato BR sem milhar com vírgula decimal: 1,50
    elif re.match(r"^\d+,\d{1,2}$", t):
        t = t.replace(",", ".")

    try:
        v = float(t)
    except ValueError:
        return None

    return v if 0 < v <= VALOR_MAX else None


# Limite seguro abaixo do máximo do Telegram (4096)
_MSG_LIMIT = 4000


def enviar_em_partes(texto: str) -> list[str]:
    """
    Divide texto em partes de até _MSG_LIMIT chars respeitando quebras de linha.
    Garante que nenhuma parte ultrapasse o limite, mesmo com linhas muito longas.
    """
    if len(texto) <= _MSG_LIMIT:
        return [texto]

    partes: list[str] = []
    atual = ""

    for linha in texto.splitlines(keepends=True):
        # Linha maior que o limite sozinha — quebra forçada por caractere
        while len(linha) > _MSG_LIMIT:
            espaco = _MSG_LIMIT - len(atual)
            atual += linha[:espaco]
            partes.append(atual)
            atual = ""
            linha = linha[espaco:]

        if len(atual) + len(linha) > _MSG_LIMIT:
            partes.append(atual)
            atual = ""

        atual += linha

    if atual:
        partes.append(atual)

    return partes


def calcular_saldo(lista: list) -> dict:
    ent  = sum(float(r["valor"]) for r in lista if r["tipo"] == "deposito")
    desp = sum(float(r["valor"]) for r in lista if r["tipo"] == "despesa")
    pix  = sum(float(r["valor"]) for r in lista if r["tipo"] == "pix")
    return {"entradas": ent, "despesas": desp, "pixs": pix, "saldo": ent - desp - pix}


def fmt_registro(r: dict) -> str:
    metodo_str = ""
    if r.get("metodo_pagamento"):
        em         = EMOJI_METODO.get(r["metodo_pagamento"], "💳")
        metodo_str = f"\n{em} {r['metodo_pagamento']}"
    origem_str = ""
    if r.get("origem") == "fatura":
        origem_str = "\n🧾 _Pagamento de fatura_"
    return (
        f"{EMOJI.get(r['tipo'], '⚪')} *{LABEL.get(r['tipo'], r['tipo'])}*"
        f" — {fmt(r['valor'])}\n"
        f"📝 {r['descricao']}\n"
        f"📍 {r['destino']}"
        f"{metodo_str}"
        f"{origem_str}\n"
        f"🕐 {r['data']} às {r['hora']}"
    )


def fmt_conta(r: dict) -> str:
    emoji = "⏳" if r["status"] == "PENDENTE" else "✅"
    txt   = (
        f"{emoji} *{r['nome']}* — {fmt(r['valor'])}\n"
        f"📅 Vence: {r['vencimento']}  🏦 {r['banco']}\n"
        f"🔖 *{r['status']}*"
    )
    if r.get("metodo_pagamento"):
        em   = EMOJI_METODO.get(r["metodo_pagamento"], "💳")
        txt += f"  {em} {r['metodo_pagamento']}"
    if r.get("pago_em"):
        pago_dt  = r["pago_em"]
        pago_str = (
            pago_dt.astimezone(FUSO).strftime("%d/%m/%Y %H:%M")
            if hasattr(pago_dt, "astimezone") else str(pago_dt)
        )
        txt += f"\n_Pago em {pago_str}_"
    return txt


def termometro(atual: float, meta: float, largura: int = 12) -> str:
    """
    Barra visual de progresso.
    Verde até 74% · Amarelo 75–99% · Vermelho 100%+
    """
    if meta <= 0:
        return f"{'⬜' * largura} 0%"
    pct    = min(atual / meta, 1.0)
    cheios = round(pct * largura)
    vazios = largura - cheios
    cor    = "🟥" if pct >= 1.0 else ("🟨" if pct >= 0.75 else "🟩")
    return f"{cor * cheios}{'⬜' * vazios} {pct * 100:.0f}%"
