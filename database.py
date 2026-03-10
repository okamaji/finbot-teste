"""
database.py — Camada de acesso ao banco de dados.

Novidades vs versão anterior:
- termos_aceitos / termos_aceitos_em na tabela licencas
- tentativas_key — bloqueia key após 3 tentativas inválidas por outro usuário
- spam_log — rastreia spam para o painel admin
- db_stats() — estatísticas para /stats
- db_todos_usuarios() / db_usuarios_ativos() — para /users e /mensagem
- db_gerar_key() — cria licença (admin)
- db_revogar_por_chat/key/username — revogar flexível
- db_licenca_por_chat() — detalhes do usuário para /veruser
- db_log_spam() / db_revogar_licenca() — anti-spam automático
- db_limpar_dados_expirados() — política de retenção 40 dias
- Schema v3
"""
import logging
from datetime import datetime, date, timedelta
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
            id                SERIAL PRIMARY KEY,
            key               VARCHAR(32) UNIQUE NOT NULL,
            chat_id           BIGINT,
            validade          DATE NOT NULL,
            ativo             BOOLEAN DEFAULT TRUE,
            username          TEXT,
            termos_aceitos    BOOLEAN DEFAULT FALSE,
            termos_aceitos_em TIMESTAMPTZ,
            tentativas_key    INTEGER DEFAULT 0,
            criado_em         TIMESTAMPTZ DEFAULT NOW()
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS spam_log (
            id        SERIAL PRIMARY KEY,
            chat_id   BIGINT NOT NULL,
            tipo      VARCHAR(20) NOT NULL,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL DEFAULT 0
        )
    """)


_SCHEMA_VERSION = 3

_MIGRATIONS: list[list[str]] = [
    # v1
    [
        "ALTER TABLE registros     ADD COLUMN IF NOT EXISTS metodo_pagamento VARCHAR(30) DEFAULT NULL",
        "ALTER TABLE registros     ADD COLUMN IF NOT EXISTS origem VARCHAR(20) DEFAULT 'manual'",
        "ALTER TABLE contas_pagar  ADD COLUMN IF NOT EXISTS metodo_pagamento VARCHAR(30) DEFAULT NULL",
        "ALTER TABLE contas_pagar  ADD COLUMN IF NOT EXISTS registro_id INTEGER DEFAULT NULL",
        "ALTER TABLE licencas      ADD COLUMN IF NOT EXISTS username TEXT",
    ],
    # v2
    [
        "CREATE INDEX IF NOT EXISTS idx_registros_data   ON registros(chat_id, data)",
        "CREATE INDEX IF NOT EXISTS idx_registros_tipo   ON registros(chat_id, tipo)",
        "CREATE INDEX IF NOT EXISTS idx_contas_status    ON contas_pagar(chat_id, status)",
    ],
    # v3 — termos, tentativas_key, spam_log
    [
        "ALTER TABLE licencas ADD COLUMN IF NOT EXISTS termos_aceitos BOOLEAN DEFAULT FALSE",
        "ALTER TABLE licencas ADD COLUMN IF NOT EXISTS termos_aceitos_em TIMESTAMPTZ",
        "ALTER TABLE licencas ADD COLUMN IF NOT EXISTS tentativas_key INTEGER DEFAULT 0",
        """CREATE TABLE IF NOT EXISTS spam_log (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            tipo VARCHAR(20) NOT NULL,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        )""",
    ],
]


def _run_migrations(cur):
    cur.execute("SELECT version FROM schema_version LIMIT 1")
    row = cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO schema_version (version) VALUES (0)")
        versao_atual = 0
    else:
        versao_atual = row["version"]

    if versao_atual >= _SCHEMA_VERSION:
        return

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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_registros_data    ON registros(chat_id, data)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_registros_tipo    ON registros(chat_id, tipo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_contas_status     ON contas_pagar(chat_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_spam_chat         ON spam_log(chat_id)")


# ── Licenças ──────────────────────────────────────────────────────────────────
def db_verificar_licenca(chat_id: int) -> str:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT validade, ativo FROM licencas WHERE chat_id=%s AND ativo=TRUE",
                (chat_id,)
            )
            row = cur.fetchone()
            if not row:
                return "invalida"
            return "expirada" if row["validade"] < date.today() else "ok"
    finally:
        release_conn(conn)


def db_verificar_termos(chat_id: int) -> bool:
    """Retorna True se o usuário já aceitou os termos."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT termos_aceitos FROM licencas WHERE chat_id=%s AND ativo=TRUE",
                (chat_id,)
            )
            row = cur.fetchone()
            return bool(row and row["termos_aceitos"])
    finally:
        release_conn(conn)


def db_aceitar_termos(chat_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE licencas SET termos_aceitos=TRUE, termos_aceitos_em=NOW() WHERE chat_id=%s",
                (chat_id,)
            )
        conn.commit()
    finally:
        release_conn(conn)


def db_ativar_licenca(key: str, chat_id: int) -> str:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, chat_id, validade, ativo, tentativas_key FROM licencas WHERE key=%s",
                (key,)
            )
            row = cur.fetchone()
            if not row:
                return "invalida"
            if not row["ativo"]:
                return "invalida"
            if row["validade"] < date.today():
                return "expirada"
            if row["chat_id"] and row["chat_id"] != chat_id:
                tentativas = (row["tentativas_key"] or 0) + 1
                cur.execute(
                    "UPDATE licencas SET tentativas_key=%s WHERE key=%s",
                    (tentativas, key)
                )
                conn.commit()
                if tentativas >= 3:
                    cur.execute("UPDATE licencas SET ativo=FALSE WHERE key=%s", (key,))
                    conn.commit()
                    logger.warning("Key invalidada por múltiplas tentativas: %s", key)
                    return "invalidada"
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


# ── Painel /home ──────────────────────────────────────────────────────────────
def db_home_data(chat_id: int, mes_str: str) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN tipo='deposito' THEN valor ELSE 0 END),0) AS ent_total,
                    COALESCE(SUM(CASE WHEN tipo='despesa'  THEN valor ELSE 0 END),0) AS desp_total,
                    COALESCE(SUM(CASE WHEN tipo='pix'      THEN valor ELSE 0 END),0) AS pix_total,
                    COALESCE(SUM(CASE WHEN tipo='deposito' AND mes=%(mes)s THEN valor ELSE 0 END),0) AS ent_mes,
                    COALESCE(SUM(CASE WHEN tipo='despesa'  AND mes=%(mes)s THEN valor ELSE 0 END),0) AS desp_mes,
                    COALESCE(SUM(CASE WHEN tipo='pix'      AND mes=%(mes)s THEN valor ELSE 0 END),0) AS pix_mes,
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


# ── Spam ──────────────────────────────────────────────────────────────────────
def db_log_spam(chat_id: int, tipo: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO spam_log (chat_id, tipo) VALUES (%s, %s)",
                (chat_id, tipo)
            )
        conn.commit()
    finally:
        release_conn(conn)


def db_revogar_licenca(chat_id: int):
    """Revoga licença por spam agressivo."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE licencas SET ativo=FALSE WHERE chat_id=%s",
                (chat_id,)
            )
        conn.commit()
        logger.warning("Licença revogada por spam agressivo. chat_id=%d", chat_id)
    finally:
        release_conn(conn)


# ── Admin queries ─────────────────────────────────────────────────────────────
def db_stats() -> dict:
    """Estatísticas gerais para o painel admin."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE chat_id IS NOT NULL) AS total_usuarios,
                    COUNT(*) FILTER (WHERE ativo=TRUE AND validade >= CURRENT_DATE AND chat_id IS NOT NULL) AS usuarios_ativos,
                    COUNT(*) FILTER (WHERE ativo=TRUE) AS licencas_ativas,
                    COUNT(*) FILTER (WHERE ativo=FALSE OR validade < CURRENT_DATE) AS licencas_expiradas,
                    COUNT(*) FILTER (WHERE ativo=TRUE AND validade < CURRENT_DATE) AS aguardando_renovacao
                FROM licencas
            """)
            stats = dict(cur.fetchone())
            cur.execute("SELECT COUNT(*) AS total FROM registros")
            stats["total_registros"] = cur.fetchone()["total"]
            cur.execute(
                "SELECT COUNT(*) AS spam_hoje FROM spam_log WHERE criado_em > NOW() - INTERVAL '24h'"
            )
            stats["spam_hoje"] = cur.fetchone()["spam_hoje"]
        return stats
    finally:
        release_conn(conn)


def db_todos_usuarios() -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chat_id, username, validade, ativo, termos_aceitos, criado_em
                FROM licencas
                WHERE chat_id IS NOT NULL
                ORDER BY criado_em DESC
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def db_usuarios_ativos() -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT chat_id, username
                FROM licencas
                WHERE ativo=TRUE AND chat_id IS NOT NULL
                  AND validade >= CURRENT_DATE
                ORDER BY chat_id
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def db_gerar_key(dias: int, key: str) -> dict:
    validade = date.today() + timedelta(days=dias)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO licencas (key, validade) VALUES (%s, %s) RETURNING *",
                (key, validade)
            )
            r = cur.fetchone()
        conn.commit()
        return dict(r)
    finally:
        release_conn(conn)


def db_revogar_por_chat(chat_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE licencas SET ativo=FALSE WHERE chat_id=%s AND ativo=TRUE RETURNING id",
                (chat_id,)
            )
            ok = cur.fetchone() is not None
        conn.commit()
        return ok
    finally:
        release_conn(conn)


def db_revogar_por_key(key: str) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE licencas SET ativo=FALSE WHERE key=%s AND ativo=TRUE RETURNING chat_id",
                (key,)
            )
            r = cur.fetchone()
        conn.commit()
        return dict(r) if r else None
    finally:
        release_conn(conn)


def db_revogar_por_username(username: str) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE licencas SET ativo=FALSE WHERE username=%s AND ativo=TRUE RETURNING chat_id",
                (username,)
            )
            r = cur.fetchone()
        conn.commit()
        return dict(r) if r else None
    finally:
        release_conn(conn)


def db_licenca_por_chat(chat_id: int) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM licencas WHERE chat_id=%s", (chat_id,))
            r = cur.fetchone()
            return dict(r) if r else None
    finally:
        release_conn(conn)


# ── Retenção de dados ─────────────────────────────────────────────────────────
def db_limpar_dados_expirados(dias_apos_expiracao: int = 40) -> int:
    """
    Deleta permanentemente dados de usuários que não renovaram
    após N dias do vencimento. Retorna número de chat_ids deletados.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT chat_id FROM licencas
                WHERE chat_id IS NOT NULL
                  AND (ativo=FALSE OR validade < CURRENT_DATE - INTERVAL '%s days')
            """, (dias_apos_expiracao,))
            victims = [r["chat_id"] for r in cur.fetchall()]
            for cid in victims:
                cur.execute("DELETE FROM registros    WHERE chat_id=%s", (cid,))
                cur.execute("DELETE FROM contas_pagar WHERE chat_id=%s", (cid,))
                cur.execute("DELETE FROM investimentos WHERE chat_id=%s", (cid,))
                logger.info("Dados deletados por expiração. chat_id=%d", cid)
        conn.commit()
        return len(victims)
    finally:
        release_conn(conn)
