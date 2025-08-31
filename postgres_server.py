# postgres_server.py

from typing import Any, Optional, List, Dict
import psycopg
from psycopg.rows import dict_row
from mcp.server.fastmcp import FastMCP
import sys
import logging
import os
import argparse
import time
import json
import base64
import re
from pydantic import BaseModel, Field, validator
from typing import Literal

# --- CONFIGURACIÓN ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('postgres-mcp-server')
mcp = FastMCP("PostgreSQL Explorer", log_level="INFO")

parser = argparse.ArgumentParser(description="PostgreSQL Explorer MCP server")
parser.add_argument("--conn", dest="conn", default=os.getenv("POSTGRES_CONNECTION_STRING"), help="PostgreSQL connection string or DSN")
parser.add_argument("--transport", dest="transport", choices=["stdio", "sse", "streamable-http"], default=os.getenv("MCP_TRANSPORT", "stdio"), help="Transport protocol")
parser.add_argument("--host", dest="host", default=os.getenv("MCP_HOST"), help="Host to bind for SSE/HTTP transports")
parser.add_argument("--port", dest="port", type=int, default=os.getenv("MCP_PORT"), help="Port to bind for SSE/HTTP transports")
parser.add_argument("--mount", dest="mount", default=os.getenv("MCP_SSE_MOUNT"), help="Optional mount path for SSE transport")
args, _ = parser.parse_known_args()
CONNECTION_STRING: Optional[str] = args.conn

READONLY: bool = os.getenv("POSTGRES_READONLY", "false").lower() in {"1", "true", "yes"}
STATEMENT_TIMEOUT_MS: Optional[int] = None
try:
    if os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS"):
        STATEMENT_TIMEOUT_MS = int(os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS"))
except ValueError:
    logger.warning("Invalid POSTGRES_STATEMENT_TIMEOUT_MS; ignoring")

logger.info(
    "Starting PostgreSQL MCP server – connection %s",
    ("to " + CONNECTION_STRING.split('@')[1]) if CONNECTION_STRING and '@' in CONNECTION_STRING else "(not set)"
)

# --- LÓGICA DE CONEXIÓN ---
def get_connection():
    if not CONNECTION_STRING:
        raise RuntimeError("POSTGRES_CONNECTION_STRING is not set.")
    try:
        conn = psycopg.connect(CONNECTION_STRING)
        with conn.cursor() as cur:
            cur.execute("SET application_name = %s", ("mcp-postgres",))
            if STATEMENT_TIMEOUT_MS and STATEMENT_TIMEOUT_MS > 0:
                cur.execute("SET statement_timeout = %s", (STATEMENT_TIMEOUT_MS,))
            conn.commit()
        return conn
    except Exception as e:
        logger.error(f"Failed to establish database connection: {str(e)}")
        raise

# --- MODELOS PYDANTIC (SIMPLIFICADOS) ---
class QueryInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = 500
    format: Literal["markdown", "json"] = "markdown"

class QueryJSONInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = 500

# --- FUNCIÓN DE VALIDACIÓN CENTRALIZADA ---
def validate_and_sanitize_sql(sql: str) -> str:
    """
    Valida y sanitiza el SQL. Lanza ValueError con mensajes específicos si falla.
    """
    sanitized_sql = sql.strip().removesuffix(';')
    sql_lower = sanitized_sql.lower()

    if sql_lower.startswith(('select', 'with', 'insert into')):
        pass
    elif re.match(r"^\s*update\s+transactions\s+set\s+status\s*=\s*'(void|superseded)'.*$", sql_lower):
        pass
    else:
        first_word = sql_lower.split()[0] if sql_lower else ''
        dangerous_keywords = ['update', 'delete', 'drop', 'create', 'alter', 'truncate']
        if first_word in dangerous_keywords:
            raise ValueError(f"Operación '{first_word}' no permitida. Solo se permiten SELECT, INSERT y UPDATE de status de transacciones.")
        else:
            raise ValueError("Tipo de consulta SQL no reconocida o no permitida por seguridad.")

    table_names = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', sanitized_sql, re.IGNORECASE)
    for name in table_names:
        if not name.islower() and not name.lower().startswith('pg_'):
            raise ValueError(f"Nombre de tabla inválido: '{name}'. Todos los nombres de tablas deben estar en minúsculas.")
            
    return sanitized_sql

# --- FUNCIÓN DE EJECUCIÓN DE QUERIES ---
def _exec_query(sql: str, parameters: Optional[List[Any]], row_limit: int, as_json: bool) -> Any:
    conn = None
    try:
        conn = get_connection()
        with conn.cursor(row_factory=dict_row) as cur:
            if parameters:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            
            if cur.description is None:
                conn.commit()
                return [] if as_json else f"Query executed successfully. Rows affected: {cur.rowcount}"

            rows = cur.fetchmany(row_limit)
            if as_json:
                return [dict(r) for r in rows]
            
            if not rows:
                return "No results found"
            
            keys = list(rows[0].keys())
            header = " | ".join(keys)
            separator = " | ".join(["---"] * len(keys))
            body_lines = []
            for row in rows:
                vals = [str(row.get(k, "NULL")) for k in keys]
                body_lines.append(" | ".join(vals))
            body = "\n".join(body_lines)
            return f"Results:\n{header}\n{separator}\n{body}"

    except Exception as e:
        error_message = f"Error de base de datos: {str(e)}"
        logger.error(error_message)
        raise Exception(error_message)
    finally:
        if conn:
            conn.close()

# --- HERRAMIENTA PRINCIPAL ---
@mcp.tool()
def run_query_json(input: QueryJSONInput) -> List[Dict[str, Any]]:
    """
    Valida, sanitiza y ejecuta una consulta SQL, devolviendo filas JSON.
    """
    try:
        sanitized_sql = validate_and_sanitize_sql(input.sql)
        
        if not CONNECTION_STRING:
            return []
        
        res = _exec_query(sanitized_sql, input.parameters, input.row_limit, as_json=True)
        return res if isinstance(res, list) else []

    except ValueError as ve:
        logger.error(f"Error de validación de SQL: {str(ve)}")
        raise Exception(f"Error de validación: {str(ve)}")

# --- OTRAS HERRAMIENTAS ÚTILES (OPCIONALES PERO RECOMENDADAS) ---
@mcp.tool()
def db_identity() -> Dict[str, Any]:
    """Return current DB identity details."""
    conn = None
    try:
        conn = get_connection()
        info: Dict[str, Any] = {}
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT current_database() AS database, current_user AS \"user\", "
                "inet_server_addr()::text AS host, inet_server_port() AS port"
            )
            row = cur.fetchone()
            if row: info.update(dict(row))
            cur.execute("SELECT current_schemas(true) AS search_path")
            row = cur.fetchone()
            if row and "search_path" in row: info["search_path"] = row["search_path"]
        return info
    finally:
        if conn: conn.close()

@mcp.tool()
def list_tables(db_schema: Optional[str] = 'public') -> str:
    """List all tables in a specific schema."""
    sql = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name"
    return _exec_query(sql, [db_schema], 1000, as_json=False)

# --- BLOQUE DE EJECUCIÓN ---
if __name__ == "__main__":
    try:
        if args.host: mcp.settings.host = args.host
        if args.port: mcp.settings.port = int(args.port)
        logger.info("Starting MCP Postgres server using %s transport on %s:%s", args.transport, mcp.settings.host, mcp.settings.port)
        if args.transport == "sse":
            mcp.run(transport="sse", mount_path=args.mount)
        else:
            mcp.run(transport=args.transport)
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        sys.exit(1)
