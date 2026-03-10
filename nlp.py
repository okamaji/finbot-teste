"""
bot/nlp/parser.py — Interpretação de frases financeiras em linguagem natural.

Melhorias:
- Sem duplicação de campos 📝 e 📍
- Extração de produto + local/plataforma separados
- Exemplos corrigidos:
    "gastei 10 no cartão de crédito"     → descrição: "Gasto geral", método: Cartão de Crédito
    "gastei 130 no crédito no restaurante" → descrição: "Restaurante", método: Cartão de Crédito
    "comprei uma camisa na shopee por 130" → descrição: "Camisa", destino: "Shopee"
"""
import re
from helpers import parsear_valor
from config import EMOJI_METODO, EMOJI, LABEL

_P = r"(?:pro?\s+|pra\s+(?:(?:o|a)\s+)?|para\s+(?:(?:o|a)\s+)?|ao?\s+|à\s+)?"

_METODOS = [
    (r"cart[aã]o\s+de\s+cr[eé]dito|cart[aã]o\s+cr[eé]dito", "Cartão de Crédito"),
    (r"cart[aã]o\s+de\s+d[eé]bito|cart[aã]o\s+d[eé]bito",   "Cartão de Débito"),
    (r"cr[eé]dito",                                            "Cartão de Crédito"),
    (r"d[eé]bito",                                             "Cartão de Débito"),
    (r"transfer[eê]ncia",                                      "Transferência"),
    (r"\bpix\b",                                               "Pix"),
]

# Plataformas/lojas conhecidas para extração de destino
_PLATAFORMAS = [
    "shopee", "amazon", "mercado livre", "mercadolivre", "shein", "aliexpress",
    "americanas", "casas bahia", "magazine luiza", "magalu", "submarino",
    "rappi", "ifood", "uber eats", "uber", "99", "cabify",
    "netflix", "spotify", "youtube", "disney", "hbo", "prime",
    "nubank", "itaú", "bradesco", "santander", "inter", "c6",
    "supermercado", "mercado", "padaria", "farmácia", "farmacia",
    "posto", "loja", "restaurante", "bar", "academia",
]

# Palavras que indicam compra de produto
_VERBOS_COMPRA = [
    "comprei", "comprou", "adquiri", "pedi", "encomendei"
]

# Palavras de preposição de local a remover
_PREPS_LOCAL = r"\b(?:no|na|nos|nas|em|pelo|pela|do|da|de)\b\s+"


def _detectar_metodo(texto: str) -> tuple[str | None, str]:
    for pat, metodo in _METODOS:
        novo = re.sub(
            rf"\s+(?:no|na|pelo|pela|com|via)\s+(?:{pat})",
            "", texto, flags=re.IGNORECASE
        )
        if novo != texto:
            return metodo, novo.strip()
        novo = re.sub(rf"\s*\b(?:{pat})\b\s*", " ", texto, flags=re.IGNORECASE).strip()
        if novo != texto.strip():
            return metodo, re.sub(r"\s{2,}", " ", novo).strip()
    return None, texto.strip()


def _detectar_plataforma(texto: str) -> tuple[str | None, str]:
    """Extrai plataforma/loja do texto e retorna (plataforma, texto_limpo)."""
    t_lower = texto.lower()
    for plat in _PLATAFORMAS:
        # Tenta encontrar "na/no/em shopee", "pela shopee", etc.
        match = re.search(
            rf"\b(?:no|na|nos|nas|em|pelo|pela|da|do|de|n[ao])\s+{re.escape(plat)}\b",
            t_lower
        )
        if match:
            span  = match.span()
            limpo = texto[:span[0]] + texto[span[1]:]
            return plat.capitalize(), re.sub(r"\s{2,}", " ", limpo).strip()
        # Tenta só o nome da plataforma no final
        match = re.search(rf"\b{re.escape(plat)}\b", t_lower)
        if match:
            span  = match.span()
            limpo = texto[:span[0]] + texto[span[1]:]
            return plat.capitalize(), re.sub(r"\s{2,}", " ", limpo).strip()
    return None, texto


def _extrair_produto(texto: str, verbo: str = "") -> str:
    """Extrai o produto de frases de compra como 'comprei uma camisa'."""
    # Remove o verbo do início
    t = re.sub(rf"^{re.escape(verbo)}\s+", "", texto, flags=re.IGNORECASE).strip()
    # Remove artigos iniciais
    t = re.sub(r"^(um|uma|umas|uns|o|a|os|as)\s+", "", t, flags=re.IGNORECASE).strip()
    # Remove preposição de valor no final ("por 130", "de 50")
    t = re.sub(r"\s+(?:por|de|no valor de)\s+[\d.,]+$", "", t, flags=re.IGNORECASE).strip()
    return t.capitalize() if t else ""


def _nome(raw: str) -> str:
    return raw.strip().capitalize() if raw else ""


_PIX = [
    rf"^(?:mandei|enviei)\s+(?:R\$\s*)?[\d.,]+\s+no\s+pix\s+{_P}(.+)$",
    rf"^(?:mandei|enviei)\s+pix\s+de\s+(?:R\$\s*)?[\d.,]+\s+{_P}(.+)$",
    rf"^(?:mandei|enviei|transferi\s+por\s+pix)\s+(?:R\$\s*)?[\d.,]+\s+(?:de\s+)?pix\s+{_P}(.+)$",
    rf"^(?:mandei\s+)?pix\s+(?:R\$\s*)?[\d.,]+\s+{_P}(.+)$",
    rf"^pix\s+{_P}(.+?)\s+(?:R\$\s*)?[\d.,]+$",
]

_DEPOSITO = [
    r"^(recebi|entrou|caiu|ganhei|recebido)\s+(?:R\$\s*)?([\d.,]+)\s*(?:de\s+)?(.*)$",
    r"^(salario|salário|freelance|rendimento|dividendo)\s+(?:R\$\s*)?([\d.,]+)$",
]

_DESPESA = [
    r"^(paguei|gastei|usei|pago|gasto|cobr(?:ou|aram))\s+(?:R\$\s*)?([\d.,]+)\s+(?:n[oa]s?\s+)?(.+)$",
    r"^(comprei|comprou|adquiri|pedi|encomendei)\s+(.+?)\s+(?:por\s+)?(?:R\$\s*)?([\d.,]+)$",
    r"^(?:R\$\s*)?([\d.,]+)\s+n[oa]\s+(.+)$",
    r"^([a-zA-ZÀ-ú\s]{3,})\s+(?:R\$\s*)?([\d.,]+)$",
]

_TRANSFERENCIA = [
    rf"^(transferi|mandei|enviei)\s+(?:R\$\s*)?([\d.,]+)\s+(?:para\s+(?:o\s+|a\s+)?)?(.+)$",
]


def _match_first(patterns, texto):
    for pat in patterns:
        m = re.match(pat, texto, re.IGNORECASE)
        if m:
            return m, pat
    return None, None


def interpretar_frase(texto: str) -> dict | None:
    t = texto.strip()

    # ── Pix ───────────────────────────────────────────────────────────────────
    for i, pat in enumerate(_PIX):
        m = re.match(pat, t, re.IGNORECASE)
        if not m:
            continue
        # extrai valor do texto original
        valor_match = re.search(r"(?:R\$\s*)?([\d.,]+)", t)
        if not valor_match:
            continue
        valor = parsear_valor(valor_match.group(1))
        g     = m.groups()
        nome  = _nome(g[-1]) if g else "Pix enviado"
        if valor:
            return {"tipo": "pix", "valor": valor,
                    "descricao": f"Pix — {nome}", "destino": nome,
                    "metodo_pagamento": None}

    metodo, t_limpo = _detectar_metodo(t)

    # ── Depósito ──────────────────────────────────────────────────────────────
    m, _ = _match_first(_DEPOSITO, t_limpo)
    if m:
        g = m.groups()
        if len(g) == 2:
            keyword, valor_str = g
            desc, dest = keyword.capitalize(), "Conta"
        else:
            _, valor_str, desc = g
            desc = desc.strip().capitalize() if desc and desc.strip() else "Recebimento"
            dest = "Conta"
        valor = parsear_valor(valor_str)
        if valor:
            return {"tipo": "deposito", "valor": valor,
                    "descricao": desc, "destino": dest,
                    "metodo_pagamento": metodo}

    # ── Transferência ─────────────────────────────────────────────────────────
    m, _ = _match_first(_TRANSFERENCIA, t_limpo)
    if m:
        _, valor_str, dest = m.groups()
        dest = dest.strip().capitalize() if dest else ""
        if dest and not re.match(r"^\d", dest):
            valor = parsear_valor(valor_str)
            if valor:
                return {"tipo": "pix", "valor": valor,
                        "descricao": f"Transferência — {dest}", "destino": dest,
                        "metodo_pagamento": metodo}

    # ── Despesa / Compra ──────────────────────────────────────────────────────
    # Tenta compra com produto explícito: "comprei uma camisa na shopee por 130"
    m_compra = re.match(
        r"^(comprei|comprou|adquiri|pedi|encomendei)\s+(.+?)\s+(?:por\s+)?(?:R\$\s*)?([\d.,]+)$",
        t_limpo, re.IGNORECASE
    )
    if m_compra:
        verbo, produto_raw, valor_str = m_compra.groups()
        valor = parsear_valor(valor_str)
        if valor:
            plataforma, produto_limpo = _detectar_plataforma(produto_raw)
            produto = _extrair_produto(produto_limpo)
            dest    = plataforma if plataforma else (produto or "Compra")
            desc    = produto if produto else "Compra"
            return {"tipo": "despesa", "valor": valor,
                    "descricao": desc, "destino": dest,
                    "metodo_pagamento": metodo}

    # Despesa genérica
    for pat in [
        r"^(paguei|gastei|usei|pago|gasto|cobr(?:ou|aram))\s+(?:R\$\s*)?([\d.,]+)\s+(?:n[oa]s?\s+)?(.+)$",
        r"^(?:R\$\s*)?([\d.,]+)\s+n[oa]\s+(.+)$",
        r"^([a-zA-ZÀ-ú\s]{3,})\s+(?:R\$\s*)?([\d.,]+)$",
    ]:
        m = re.match(pat, t_limpo, re.IGNORECASE)
        if not m:
            continue
        g = m.groups()

        if len(g) == 3:
            verbo, valor_str, local_raw = g
            valor = parsear_valor(valor_str)
            if not valor:
                continue
            plataforma, local_limpo = _detectar_plataforma(local_raw)
            local_limpo = re.sub(_PREPS_LOCAL, "", local_limpo, flags=re.IGNORECASE).strip()
            desc = local_limpo.capitalize() if local_limpo else "Gasto geral"
            dest = plataforma if plataforma else desc

        elif len(g) == 2:
            a, b = g
            if parsear_valor(str(a)):
                valor_str, local_raw = a, b
                valor = parsear_valor(valor_str)
                if not valor:
                    continue
                plataforma, local_limpo = _detectar_plataforma(local_raw)
                local_limpo = re.sub(_PREPS_LOCAL, "", local_limpo, flags=re.IGNORECASE).strip()
                desc = local_limpo.capitalize() if local_limpo else "Gasto geral"
                dest = plataforma if plataforma else desc
            else:
                local_raw, valor_str = a, b
                valor = parsear_valor(valor_str)
                if not valor:
                    continue
                plataforma, local_limpo = _detectar_plataforma(local_raw)
                local_limpo = re.sub(_PREPS_LOCAL, "", local_limpo, flags=re.IGNORECASE).strip()
                desc = local_limpo.capitalize() if local_limpo else "Gasto geral"
                dest = plataforma if plataforma else desc
        else:
            continue

        # Se só tem método e nenhuma descrição real, usa "Gasto geral"
        if not desc or desc.lower() in ("gasto", "gastei", "paguei", "usei"):
            desc = "Gasto geral"
        if not dest or dest.lower() in ("gasto", "gastei", "paguei", "usei"):
            dest = desc

        return {"tipo": "despesa", "valor": valor,
                "descricao": desc, "destino": dest,
                "metodo_pagamento": metodo}

    return None


def resumo_nlp(resultado: dict, fmt_fn) -> str:
    _EMOJI_TIPO = {"despesa": "🔴", "deposito": "🟢", "pix": "🔵"}
    _LABEL_TIPO = {"despesa": "Despesa detectada", "deposito": "Depósito detectado", "pix": "Pix detectado"}
    tipo        = resultado["tipo"]
    metodo      = resultado.get("metodo_pagamento")
    desc        = resultado.get("descricao", "")
    dest        = resultado.get("destino", "")
    metodo_str  = f"\n{EMOJI_METODO.get(metodo, '💳')} *Método:* {metodo}" if metodo else ""

    # Evita repetição de descrição e destino
    dest_str = f"\n📍 *Destino:* {dest}" if dest and dest != desc else ""

    return (
        f"{_EMOJI_TIPO[tipo]} *{_LABEL_TIPO[tipo]}!*\n\n"
        f"💵 *Valor:* {fmt_fn(resultado['valor'])}\n"
        f"📝 *Descrição:* {desc}"
        f"{dest_str}"
        f"{metodo_str}\n\n"
        f"_Confirma o registro?_"
    )
