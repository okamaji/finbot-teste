"""
demo.py — Geração de dados fictícios para a conta demonstração.

Correção: todos os inserts em uma única conexão (era 18 conexões separadas).
"""
import random
import logging
from helpers import agora_br
from database import get_conn, release_conn, db_inserir_conta
from config import DEMO_NUM_REGISTROS

logger = logging.getLogger(__name__)

_DESCRICOES = {
    "deposito": ["Salário", "Freelance", "Rendimento CDB", "Transferência recebida"],
    "despesa":  ["Supermercado", "Farmácia", "Conta de luz", "Internet", "Combustível"],
    "pix":      ["Transferência amigo", "Pagamento serviço", "Racha do almoço"],
}
_DESTINOS = {
    "deposito": ["Nubank", "Inter", "Bradesco"],
    "despesa":  ["Cartão Nubank", "Débito Inter", "Dinheiro"],
    "pix":      ["João Silva", "Maria Souza", "Pedro Costa"],
}
_CONTAS = [
    ("Cartão Nubank", 150,  900,  "10/03", "Nubank"),
    ("Conta de Luz",   80,  220,  "12/03", "Cemig"),
    ("Internet Vivo",  99,  149,  "15/03", "Vivo"),
]


def popular_conta_demo(chat_id: int) -> None:
    """Apaga e recria dados fictícios da conta demo — tudo em uma única conexão."""
    demo_id = -(chat_id)
    now     = agora_br()
    mes_str = now.strftime("%m/%Y")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Limpa dados anteriores
            cur.execute("DELETE FROM registros   WHERE chat_id=%s", (demo_id,))
            cur.execute("DELETE FROM contas_pagar WHERE chat_id=%s", (demo_id,))

            # Batch insert de registros
            for _ in range(DEMO_NUM_REGISTROS):
                tipo  = random.choice(["deposito","deposito","despesa","despesa","despesa","pix"])
                valor = round(random.uniform(
                    800  if tipo == "deposito" else (30  if tipo == "despesa" else 10),
                    4500 if tipo == "deposito" else (600 if tipo == "despesa" else 300),
                ), 2)
                desc = random.choice(_DESCRICOES[tipo])
                dest = random.choice(_DESTINOS[tipo])
                dia  = random.randint(1, now.day)
                hora = f"{random.randint(7,22):02d}:{random.randint(0,59):02d}"
                data = f"{dia:02d}/{now.month:02d}/{now.year}"
                cur.execute("""
                    INSERT INTO registros (chat_id, tipo, valor, descricao, destino, data, hora, mes)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (demo_id, tipo, valor, desc, dest, data, hora, mes_str))

            # Contas a pagar
            for nome, vmin, vmax, venc, banco in _CONTAS:
                cur.execute("""
                    INSERT INTO contas_pagar (chat_id, nome, valor, vencimento, banco)
                    VALUES (%s,%s,%s,%s,%s)
                """, (demo_id, nome, round(random.uniform(vmin, vmax), 2), venc, banco))

        conn.commit()
    finally:
        release_conn(conn)

    logger.info("Demo populado. demo_id=%d registros=%d", demo_id, DEMO_NUM_REGISTROS)
