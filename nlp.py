"""
nlp.py — Interpretação de frases financeiras em linguagem natural.

Correção: EMOJI_METODO importado no topo (era importado dentro de resumo_nlp a cada chamada).
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


def _nome(raw: str) -> str:
    return raw.strip().capitalize() if raw else ""


_PIX = [
    rf"^(?:mandei|enviei)\s+(?:R\$\s*)?([\d.,]+)\s+no\s+pix\s+{_P}(.+)$",
    rf"^(?:mandei|enviei)\s+pix\s+de\s+(?:R\$\s*)?([\d.,]+)\s+{_P}(.+)$",
    rf"^(?:mandei|enviei|transferi\s+por\s+pix)\s+(?:R\$\s*)?([\d.,]+)\s+(?:de\s+)?pix\s+{_P}(.+)$",
    rf"^(?:mandei\s+)?pix\s+(?:R\$\s*)?([\d.,]+)\s+{_P}(.+)$",
    rf"^pix\s+{_P}(.+?)\s+(?:R\$\s*)?([\d.,]+)$",
]

_DEPOSITO = [
    r"^(recebi|entrou|caiu|ganhei|recebido)\s+(?:R\$\s*)?([\d.,]+)\s*(?:de\s+)?(.*)$",
    r"^(salario|salário|freelance|rendimento|dividendo)\s+(?:R\$\s*)?([\d.,]+)$",
]

_DESPESA = [
    r"^(paguei|gastei|comprei|usei|pago|gasto|cobr(?:ou|aram))\s+(?:R\$\s*)?([\d.,]+)\s+(?:n[oa]s?\s+)?(.+)$",
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
            return m
    return None


def interpretar_frase(texto: str) -> dict | None:
    t = texto.strip()

    # Pix como tipo — roda no texto original (antes de remover "pix")
    for i, pat in enumerate(_PIX):
        m = re.match(pat, t, re.IGNORECASE)
        if not m:
            continue
        g = m.groups()
        if i == len(_PIX) - 1:
            nome_raw, valor_str = g
        else:
            valor_str, nome_raw = g[0], g[-1]
        valor = parsear_valor(valor_str)
        nome  = _nome(nome_raw) if nome_raw else "Pix enviado"
        if valor:
            desc = nome or "Pix enviado"
            return {"tipo": "pix", "valor": valor,
                    "descricao": f"Pix — {desc}", "destino": desc,
                    "metodo_pagamento": None}

    metodo, t_limpo = _detectar_metodo(t)

    m = _match_first(_DEPOSITO, t_limpo)
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

    m = _match_first(_TRANSFERENCIA, t_limpo)
    if m:
        _, valor_str, dest = m.groups()
        dest = dest.strip().capitalize() if dest else ""
        if dest and not re.match(r"^\d", dest):
            valor = parsear_valor(valor_str)
            if valor:
                return {"tipo": "pix", "valor": valor,
                        "descricao": f"Transferência — {dest}", "destino": dest,
                        "metodo_pagamento": metodo}

    m = _match_first(_DESPESA, t_limpo)
    if m:
        g = m.groups()
        if len(g) == 3:
            _, valor_str, desc = g
            dest = (desc or "Despesa").strip().capitalize()
        elif len(g) == 2:
            a, b = g
            if parsear_valor(str(a)):
                valor_str, desc = a, b
            else:
                desc, valor_str = a, b
            dest = (desc or "Despesa").strip().capitalize()
        else:
            return None
        valor = parsear_valor(str(valor_str))
        if valor:
            return {"tipo": "despesa", "valor": valor,
                    "descricao": dest, "destino": dest,
                    "metodo_pagamento": metodo}

    return None


def resumo_nlp(resultado: dict, fmt_fn) -> str:
    _EMOJI_TIPO  = {"despesa": "🔴", "deposito": "🟢", "pix": "🔵"}
    _LABEL_TIPO  = {"despesa": "Despesa", "deposito": "Depósito", "pix": "Pix"}
    tipo         = resultado["tipo"]
    metodo       = resultado.get("metodo_pagamento")
    metodo_str   = f"\n{EMOJI_METODO.get(metodo, '💳')} {metodo}" if metodo else ""
    return (
        f"{_EMOJI_TIPO[tipo]} *{_LABEL_TIPO[tipo]}* detectado!\n\n"
        f"💵 Valor: *{fmt_fn(resultado['valor'])}*\n"
        f"📝 {resultado['descricao']}\n"
        f"📍 {resultado['destino']}"
        f"{metodo_str}\n\n"
        f"_Confirma o registro?_"
    )
