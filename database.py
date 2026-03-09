"""
database.py — Camada de acesso ao banco de dados.

Correções vs versão anterior:
- db_saldo_agregado(): nova função — calcula saldo com SUM no SQL, sem buscar todos os rows
- db_registros(): mantida para compatibilidade, mas uso direto foi removido dos handlers
- init_db(): migrations separadas em _run_migrations(), executa apenas DDL necessário
- Pool padrão aumentado para 5-20 conexões (suporte a 1k users)
"""

import logging
from datetime import datetime, date
from psycopg2 import pool
import psycopg2.extras
import psycopg2
from config import DATABASE, FUSO, DB_POOL_MIN, DB_POOL_MAX

logger = logging.getLogger(__name__)

_pool: pool.ThreadedConnectionPool | None = None


# ── Pool ──────────────────────────────────────────────────────────────────────
def init_pool():
    global _pool
    _pool = pool.ThreadedConnectionPool(
        DB_POOL_MIN, DB_POOL_MAX, DATABASE,
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    logger.info("✅ Pool iniciado (min=%d max=%d)", DB_POOL_MIN, DB_POOL_MAX)


def get_conn():
    try:
        return _pool.getconn()
    except pool.PoolError as e:
        logger.error("Pool esgotado: %s", e)
        raise RuntimeError("Banco temporariamente indisponível. Tente em instantes.")


def release_conn(conn):
    try:
        _pool.putconn(conn)
    except Exception:
        pass


# ── Init / Migrations ─────────────────────────────────────────────────────────
def init_db():
    """Cria tabelas e roda migrations idempotentes."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            _create_tables(cur)
            _run_migrations(cur)
            _create_indexes(cur)
        conn.commit()
    finally:
        release_conn(conn)
    logger.info("✅ Banco iniciado!")


def _create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS registros (
            id                SERIAL PRIMARY KEY,
            chat_id           BIGINT NOT NULL,
            tipo              VARCHAR(10) NOT NULL,
            valor             NUMERIC(12,2) NOT NULL,
            descricao         TEXT NOT NULL,
            destino           TEXT NOT NULL,
            data              VARCHAR(10) NOT NULL,
            hora              VARCHAR(5)  NOT NULL,
            mes               VARCHAR(7)  NOT NULL,
            metodo_pagamento  VARCHAR(30) DEFAULT NULL,
            origem            VARCHAR(20) DEFAULT 'manual',
            criado_em         TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contas_pagar (
            id                SERIAL PRIMARY KEY,
            chat_id           BIGINT NOT NULL,
            nome              TEXT NOT NULL,
            valor             NUMERIC(12,2) NOT NULL,
            vencimento        TEXT NOT NULL,
            banco             TEXT NOT NULL,
            status            VARCHAR(10) DEFAULT 'PENDENTE',
            metodo_pagamento  VARCHAR(30) DEFAULT NULL,
            registro_id       INTEGER DEFAULT NULL,
            criado_em         TIMESTAMPTZ DEFAULT NOW(),
            pago_em           TIMESTAMPTZ
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS licencas (
            id         SERIAL PRIMARY KEY,
            key        VARCHAR(32) UNIQUE NOT NULL,
            chat_id    BIGINT,
            validade   DATE NOT NULL,
            ativo      BOOLEAN DEFAULT TRUE,
            username   TEXT,
            criado_em  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS investimentos (
            id            SERIAL PRIMARY KEY,
            chat_id       BIGINT NOT NULL,
            nome          TEXT NOT NULL,
            tipo          VARCHAR(20) NOT NULL,
            valor_inicial NUMERIC(12,2) NOT NULL,
            taxa_cdi      NUMERIC(5,2) DEFAULT 100,
            data_inicio   DATE NOT NULL,
            data_vencto   DATE,
            banco         TEXT NOT NULL,
            ativo         BOOLEAN DEFAULT TRUE,
            criado_em     TIMESTAMPTZ DEFAULT NOW()
        )
    """)


_SCHEMA_VERSION = 2   # incrementar aqui ao adicionar novas migrations

# Lista de migrations indexada por versão (índice 0 = versão 1, índice 1 = versão 2...)
_MIGRATIONS: list[list[str]] = [
    # v1 — colunas adicionadas no passado
    [
        "ALTER TABLE registros     ADD COLUMN IF NOT EXISTS metodo_pagamento VARCHAR(30) DEFAULT NULL",
        "ALTER TABLE registros     ADD COLUMN IF NOT EXISTS origem VARCHAR(20) DEFAULT 'manual'",
        "ALTER TABLE contas_pagar  ADD COLUMN IF NOT EXISTS metodo_pagamento VARCHAR(30) DEFAULT NULL",
        "ALTER TABLE contas_pagar  ADD COLUMN IF NOT EXISTS registro_id INTEGER DEFAULT NULL",
        "ALTER TABLE licencas      ADD COLUMN IF NOT EXISTS username TEXT",
    ],
    # v2 — índices de performance
    [
        "CREATE INDEX IF NOT EXISTS idx_registros_data   ON registros(chat_id, data)",
        "CREATE INDEX IF NOT EXISTS idx_registros_tipo   ON registros(chat_id, tipo)",
        "CREATE INDEX IF NOT EXISTS idx_contas_status    ON contas_pagar(chat_id, status)",
    ],
]


def _run_migrations(cur):
    """
    A3 — Só roda migrations que ainda não foram aplicadas.
    Usa tabela schema_version para rastrear versão atual.
    Evita ALTER TABLE a cada boot.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("SELECT version FROM schema_version LIMIT 1")
    row = cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO schema_version (version) VALUES (0)")
        versao_atual = 0
    else:
        versao_atual = row["version"]

    if versao_atual >= _SCHEMA_VERSION:
        return   # nada a fazer — não toca nas tabelas

    for i in range(versao_atual, _SCHEMA_VERSION):
        logger.info("Rodando migration v%d→v%d", i, i + 1)
        for sql in _MIGRATIONS[i]:
            cur.execute(sql)

    cur.execute("UPDATE schema_version SET version=%s", (_SCHEMA_VERSION,))
    logger.info("Schema atualizado para v%d", _SCHEMA_VERSION)


def _create_indexes(cur):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_registros_chat_id ON registros(chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_registros_mes     ON registros(chat_id, mes)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_contas_chat_id    ON contas_pagar(chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_chat          ON investimentos(chat_id)")
    # C1 — índice por data (cmd_hoje)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_registros_data    ON registros(chat_id, data)")
    # C2 — índice por tipo (cmd_entradas/despesas/pixs)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_registros_tipo    ON registros(chat_id, tipo)")
    # C3 — índice por status (cmd_pendentes/pago)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_contas_status     ON contas_pagar(chat_id, status)")


# ── Licenças ──────────────────────────────────────────────────────────────────
def db_verificar_licenca(chat_id: int) -> str:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT validade FROM licencas WHERE chat_id=%s AND ativo=TRUE",
                (chat_id,)
            )
            row = cur.fetchone()
            if not row:
                return "invalida"
            return "expirada" if row["validade"] < date.today() else "ok"
    finally:
        release_conn(conn)


def db_ativar_licenca(key: str, chat_id: int) -> str:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, chat_id, validade, ativo FROM licencas WHERE key=%s",
                (key,)
            )
            row = cur.fetchone()
            if not row:
                logger.warning("Key inválida. chat_id=%d", chat_id)
                return "invalida"
            if not row["ativo"] or row["validade"] < date.today():
                return "expirada"
            if row["chat_id"] and row["chat_id"] != chat_id:
                logger.warning("Key em uso por outro chat_id. tentativa=%d", chat_id)
                return "ja_usada"
            cur.execute("UPDATE licencas SET chat_id=%s WHERE key=%s", (chat_id, key))
        conn.commit()
        logger.info("Licença ativada. chat_id=%d", chat_id)
        return "ok"
    finally:
        release_conn(conn)


# ── Registros ─────────────────────────────────────────────────────────────────
_CAMPOS_REGISTRO = {"tipo", "valor", "descricao", "destino"}


def db_registros(chat_id: int, limit: int | None = None, offset: int = 0) -> list[dict]:
    """Busca registros. Prefira db_saldo_agregado() para calcular saldo."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if limit is not None:
                cur.execute(
                    "SELECT * FROM registros WHERE chat_id=%s ORDER BY id LIMIT %s OFFSET %s",
                    (chat_id, limit, offset)
                )
            else:
                cur.execute(
                    "SELECT * FROM registros WHERE chat_id=%s ORDER BY id",
                    (chat_id,)
                )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def db_saldo_agregado(chat_id: int) -> dict:
    """
    Calcula saldo com agregação SQL — NÃO busca todos os rows.
    Use sempre que precisar apenas do saldo (não dos registros em si).
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN tipo='deposito' THEN valor ELSE 0 END), 0) AS entradas,
                    COALESCE(SUM(CASE WHEN tipo='despesa'  THEN valor ELSE 0 END), 0) AS despesas,
                    COALESCE(SUM(CASE WHEN tipo='pix'      THEN valor ELSE 0 END), 0) AS pixs
                FROM registros WHERE chat_id=%s
            """, (chat_id,))
            row = dict(cur.fetchone())
            row["saldo"] = float(row["entradas"]) - float(row["despesas"]) - float(row["pixs"])
            return row
    finally:
        release_conn(conn)


def db_registro_por_id(reg_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM registros WHERE id=%s", (reg_id,))
            r = cur.fetchone()
            return dict(r) if r else None
    finally:
        release_conn(conn)


def db_inserir_registro(chat_id: int, tipo: str, valor: float, descricao: str,
                        destino: str, metodo: str | None = None,
                        origem: str = "manual") -> dict:
    from helpers import agora_br
    now  = agora_br()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO registros
                    (chat_id, tipo, valor, descricao, destino, data, hora, mes, metodo_pagamento, origem)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
            """, (chat_id, tipo, valor, descricao, destino,
                  now.strftime("%d/%m/%Y"), now.strftime("%H:%M"),
                  now.strftime("%m/%Y"), metodo, origem))
            r = cur.fetchone()
        conn.commit()
        return dict(r)
    finally:
        release_conn(conn)


def db_atualizar_registro(reg_id: int, campo: str, valor) -> dict | None:
    if campo not in _CAMPOS_REGISTRO:
        logger.warning("Campo inválido para update: %s", campo)
        return None
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE registros SET {campo}=%s WHERE id=%s RETURNING *",
                (valor, reg_id)
            )
            r = cur.fetchone()
        conn.commit()
        return dict(r) if r else None
    finally:
        release_conn(conn)


def db_deletar_registro(reg_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM registros WHERE id=%s RETURNING *", (reg_id,))
            r = cur.fetchone()
        conn.commit()
        return dict(r) if r else None
    finally:
        release_conn(conn)


def db_ultimo_registro(chat_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM registros WHERE chat_id=%s ORDER BY id DESC LIMIT 1",
                (chat_id,)
            )
            r = cur.fetchone()
            return dict(r) if r else None
    finally:
        release_conn(conn)


def db_recentes(chat_id: int, n: int = 20) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM registros WHERE chat_id=%s ORDER BY id DESC LIMIT %s",
                (chat_id, n)
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def db_recentes_com_total(chat_id: int, limit: int, offset: int) -> tuple[list[dict], int]:
    """Retorna (registros, total) em uma única conexão — para paginação do extrato."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM registros WHERE chat_id=%s ORDER BY id DESC LIMIT %s OFFSET %s",
                (chat_id, limit, offset)
            )
            rows = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) AS total FROM registros WHERE chat_id=%s", (chat_id,))
            total = cur.fetchone()["total"]
        return rows, total
    finally:
        release_conn(conn)


def db_registros_mes(chat_id: int, mes_str: str, limit: int = 200) -> list[dict]:
    """A2 — Limitado a 200 registros por mês para evitar OOM com 1k users simultâneos."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM registros WHERE chat_id=%s AND mes=%s ORDER BY id LIMIT %s",
                (chat_id, mes_str, limit)
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


# ── Contas a pagar ────────────────────────────────────────────────────────────
def db_contas(chat_id: int) -> list[dict]:
    """Busca pendentes + últimas 20 pagas — evita crescimento ilimitado."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM contas_pagar WHERE chat_id=%s AND status='PENDENTE' ORDER BY vencimento",
                (chat_id,)
            )
            pendentes = [dict(r) for r in cur.fetchall()]
            cur.execute(
                "SELECT * FROM contas_pagar WHERE chat_id=%s AND status='PAGO' ORDER BY pago_em DESC LIMIT 20",
                (chat_id,)
            )
            pagas = [dict(r) for r in cur.fetchall()]
        return pendentes + pagas
    finally:
        release_conn(conn)


def db_contas_pendentes(chat_id: int) -> list[dict]:
    """Somente pendentes — para teclados de seleção rápida."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM contas_pagar WHERE chat_id=%s AND status='PENDENTE' ORDER BY vencimento",
                (chat_id,)
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def db_conta_por_id(conta_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM contas_pagar WHERE id=%s", (conta_id,))
            r = cur.fetchone()
            return dict(r) if r else None
    finally:
        release_conn(conn)


def db_marcar_pago(conta_id: int, metodo: str) -> dict | None:
    """
    B3 — Marca conta como paga e cria registro no extrato usando CTE.
    Uma única query substitui o SELECT + INSERT + UPDATE anterior.
    """
    from helpers import agora_br
    now  = agora_br()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH conta_atualizada AS (
                    UPDATE contas_pagar
                    SET status='PAGO', pago_em=NOW(), metodo_pagamento=%s
                    WHERE id=%s AND status='PENDENTE'
                    RETURNING *
                ),
                reg_inserido AS (
                    INSERT INTO registros
                        (chat_id, tipo, valor, descricao, destino, data, hora, mes, metodo_pagamento, origem)
                    SELECT
                        chat_id, 'despesa', valor,
                        'Fatura: ' || nome, banco,
                        %s, %s, %s, %s, 'fatura'
                    FROM conta_atualizada
                    RETURNING id
                )
                UPDATE contas_pagar SET registro_id = (SELECT id FROM reg_inserido)
                WHERE id = %s
                RETURNING *
            """, (metodo, conta_id,
                  now.strftime("%d/%m/%Y"), now.strftime("%H:%M"), now.strftime("%m/%Y"),
                  metodo, conta_id))
            r = cur.fetchone()
            if not r:
                return None
        conn.commit()
        logger.info("Conta paga. conta_id=%d metodo=%s", conta_id, metodo)
        return dict(r)
    finally:
        release_conn(conn)


def db_inserir_conta(chat_id: int, nome: str, valor: float,
                     vencimento: str, banco: str) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO contas_pagar (chat_id, nome, valor, vencimento, banco)
                VALUES (%s,%s,%s,%s,%s) RETURNING *
            """, (chat_id, nome, valor, vencimento, banco))
            r = cur.fetchone()
        conn.commit()
        return dict(r)
    finally:
        release_conn(conn)


# ── Investimentos ─────────────────────────────────────────────────────────────
def db_investimentos(chat_id: int) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM investimentos WHERE chat_id=%s AND ativo=TRUE ORDER BY id",
                (chat_id,)
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def db_inserir_investimento(chat_id: int, nome: str, tipo: str, valor: float,
                             taxa_cdi: float, data_inicio: str,
                             data_vencto: str | None, banco: str) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO investimentos
                    (chat_id, nome, tipo, valor_inicial, taxa_cdi, data_inicio, data_vencto, banco)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
            """, (chat_id, nome, tipo, valor, taxa_cdi,
                  data_inicio, data_vencto or None, banco))
            r = cur.fetchone()
        conn.commit()
        return dict(r)
    finally:
        release_conn(conn)


def db_remover_investimento(inv_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE investimentos SET ativo=FALSE WHERE id=%s", (inv_id,))
        conn.commit()
        return True
    finally:
        release_conn(conn)


# ── Painel /home (query única) ────────────────────────────────────────────────
def db_home_data(chat_id: int, mes_str: str) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN tipo='deposito' THEN valor ELSE 0 END),0)                                     AS ent_total,
                    COALESCE(SUM(CASE WHEN tipo='despesa'  THEN valor ELSE 0 END),0)                                     AS desp_total,
                    COALESCE(SUM(CASE WHEN tipo='pix'      THEN valor ELSE 0 END),0)                                     AS pix_total,
                    COALESCE(SUM(CASE WHEN tipo='deposito' AND mes=%(mes)s THEN valor ELSE 0 END),0)                     AS ent_mes,
                    COALESCE(SUM(CASE WHEN tipo='despesa'  AND mes=%(mes)s THEN valor ELSE 0 END),0)                     AS desp_mes,
                    COALESCE(SUM(CASE WHEN tipo='pix'      AND mes=%(mes)s THEN valor ELSE 0 END),0)                     AS pix_mes,
                    COALESCE(SUM(CASE WHEN metodo_pagamento='Pix'               AND mes=%(mes)s THEN valor ELSE 0 END),0) AS metodo_pix,
                    COALESCE(SUM(CASE WHEN metodo_pagamento='Transferência'     AND mes=%(mes)s THEN valor ELSE 0 END),0) AS metodo_transf,
                    COALESCE(SUM(CASE WHEN metodo_pagamento='Cartão de Crédito' AND mes=%(mes)s THEN valor ELSE 0 END),0) AS metodo_credito,
                    COALESCE(SUM(CASE WHEN metodo_pagamento='Cartão de Débito'  AND mes=%(mes)s THEN valor ELSE 0 END),0) AS metodo_debito
                FROM registros WHERE chat_id=%(chat_id)s
            """, {"mes": mes_str, "chat_id": chat_id})
            row = dict(cur.fetchone())
            cur.execute(
                "SELECT * FROM contas_pagar WHERE chat_id=%s ORDER BY vencimento",
                (chat_id,)
            )
            row["contas"] = [dict(r) for r in cur.fetchall()]
        return row
    finally:
        release_conn(conn)
