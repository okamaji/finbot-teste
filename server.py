"""
server.py — Keep-alive HTTP server para manter o container ativo no Railway.
C7 — Loop de restart automático caso a thread HTTP morra por qualquer motivo.
"""

import os
import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

logger = logging.getLogger(__name__)


class KeepAlive(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        if self.path == "/health":
            self.wfile.write(b'{"status":"ok","bot":"FinBot v2"}')
        else:
            self.wfile.write(b"FinBot v2 rodando!")

    def log_message(self, *a):
        pass  # silencia logs de acesso HTTP


def _servidor_loop(porta: int) -> None:
    """C7 — Reinicia o HTTPServer automaticamente se cair."""
    while True:
        try:
            logger.info("🌐 Iniciando HTTP keep-alive na porta %d", porta)
            HTTPServer(("0.0.0.0", porta), KeepAlive).serve_forever()
        except Exception as e:
            logger.error("HTTP keep-alive caiu: %s — reiniciando em 5s", e)
            time.sleep(5)


def keep_alive() -> None:
    """Inicia o servidor HTTP em thread daemon com restart automático."""
    porta = int(os.environ.get("PORT", 8080))
    t = threading.Thread(target=_servidor_loop, args=(porta,), daemon=True)
    t.start()
    logger.info("🌐 Keep-alive HTTP na porta %d (auto-restart ativo)", porta)
