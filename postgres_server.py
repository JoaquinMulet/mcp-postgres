import os
import sys
import logging
import argparse
import re
from typing import Any, Optional, List, Dict

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, Field, field_validator
from fastmcp import FastMCP, Context

# --- 1. CONFIGURACIÓN INICIAL (Sin cambios) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fp-agent-mcp-server')

# --- 2. MANEJO DE ARGUMENTOS (Añadido --mount) ---
parser = argparse.ArgumentParser(description="FP-Agent PostgreSQL MCP Server")
parser.add_argument(
    "--conn", dest="conn", default=os.getenv("DATABASE_URL"),
    help="PostgreSQL connection string (DSN)"
)
parser.add_argument(
    "--transport", dest="transport", default="sse",
    help="Transport protocol (default: sse for web deployments)"
)
parser.add_argument(
    "--host", dest="host", default="0.0.0.0",
    help="Host to bind for HTTP transport (default: 0.0.0.0 for containers)"
)
parser.add_argument(
    "--port", dest="port", type=int, default=8000,
    help="Port to bind for HTTP transport (default: 8000)"
)
# --- ¡NUEVO ARGUMENTO AQUÍ! ---
parser.add_argument(
    "--mount", dest="mount", default="/mcp",
    help="Mount path for SSE transport (e.g., /mcp)"
)
args, _ = parser.parse_known_args()
CONNECTION_STRING: Optional[str] = args.conn

# --- SECCIONES 3, 4 y 5 (Sin cambios) ---
class QueryInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = Field(default=100, ge=1)
    @field_validator('sql')
    def validate_allowed_operations(cls, value: str) -> str:
        sql_cleaned = value.strip().removesuffix(';').lower()
        if sql_cleaned.startswith(('select', 'with')): return value
        if sql_cleaned.startswith('insert into'): return value
        update_pattern = re.compile(r"^\s*update\s+transactions\s+set\s+status\s*=\s*'(void|superseded)'.*$", re.IGNORECASE)
        if update_pattern.match(value.strip()): return value
        if sql_cleaned.startswith('update'): raise ValueError("Operación UPDATE no permitida.")
        dangerous_keywords = ['delete', 'drop', 'create', 'alter', 'truncate']
        if any(sql_cleaned.startswith(keyword) for keyword in dangerous_keywords): raise ValueError(f"Operación SQL peligrosa '{sql_cleaned.split()[0]}' está prohibida.")
        raise ValueError("Tipo de consulta SQL no permitida.")

mcp = FastMCP("FP-Agent PostgreSQL Server", log_level="INFO")

@mcp.tool()
def run_query_json(input: QueryInput, ctx: Context) -> Dict[str, Any]:
    # (La lógica de la herramienta no cambia)
    ctx.info(f"Ejecutando consulta validada. Límite: {input.row_limit}")
    if not CONNECTION_STRING:
        return {"error": "Servidor no configurado para conectar a la base de datos."}
    try:
        with psycopg.connect(CONNECTION_STRING) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(input.sql, input.parameters)
                if cur.description is None:
                    conn.commit()
                    return {"status": "success", "message": cur.statusmessage or "Comando exitoso.", "rows_affected": cur.rowcount}
                rows = cur.fetchmany(input.row_limit)
                return {"status": "success", "data": rows}
    except Exception as e:
        error_message = f"Error de base de datos: {getattr(e, 'diag', {}).get('message_primary', str(e))}"
        ctx.error(error_message)
        return {"error": error_message}


# --- 6. PUNTO DE ENTRADA (Llamada a run() actualizada) ---
if __name__ == "__main__":
    if args.host: mcp.settings.host = args.host
    if args.port: mcp.settings.port = args.port

    logger.info(
        "Iniciando FP-Agent MCP Server en %s:%s, montado en '%s' usando transporte %s",
        mcp.settings.host, mcp.settings.port, args.mount, args.transport
    )

    # --- ¡CORRECCIÓN FINAL AQUÍ! ---
    # Pasamos el mount_path a la función run().
    mcp.run(transport=args.transport, mount_path=args.mount)
