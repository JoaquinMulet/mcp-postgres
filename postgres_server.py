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
from pydantic import BaseModel, Field
from typing import Literal

# --- CONFIGURACIÓN ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('postgres-mcp-server')
mcp = FastMCP("PostgreSQL Explorer", log_level="INFO")

# --- DEFINICIÓN DE ARGUMENTOS (SECCIÓN CORREGIDA) ---
parser = argparse.ArgumentParser(description="PostgreSQL Explorer MCP server")
parser.add_argument("--conn", dest="conn", default=os.getenv("POSTGRES_CONNECTION_STRING"), help="PostgreSQL connection string or DSN")
parser.add_argument("--transport", dest="transport", choices=["stdio", "sse", "streamable-http"], default=os.getenv("MCP_TRANSPORT", "stdio"), help="Transport protocol")
parser.add_argument("--host", dest="host", default=os.getenv("MCP_HOST", "127.0.0.1"), help="Host to bind for SSE/HTTP transports")
parser.add_argument("--port", dest="port", type=int, default=os.getenv("MCP_PORT", 8000), help="Port to bind for SSE/HTTP transports")
parser.add_argument("--mount", dest="mount", default=os.getenv("MCP_SSE_MOUNT"), help="Optional mount path for SSE transport")
args, _ = parser.parse_known_args()
CONNECTION_STRING: Optional[str] = args.conn

# ... (El resto del código hasta el bloque de ejecución se mantiene igual) ...

# --- LÓGICA DE CONEXIÓN ---
def get_connection():
    if not CONNECTION_STRING:
        raise RuntimeError("POSTGRES_CONNECTION_STRING is not set.")
    try:
        conn = psycopg.connect(CONNECTION_STRING)
        with conn.cursor() as cur:
            cur.execute("SET application_name = %s", ("mcp-postgres",))
        return conn
    except Exception as e:
        logger.error(f"Failed to establish database connection: {str(e)}")
        raise

# --- MODELOS PYDANTIC ---
class QueryJSONInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = 500

# --- FUNCIÓN DE VALIDACIÓN ---
def validate_and_sanitize_sql(sql: str) -> str:
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
            raise ValueError(f"Operación '{first_word}' no permitida.")
        else:
            raise ValueError("Tipo de consulta SQL no reconocida o no permitida por seguridad.")

    table_names = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', sanitized_sql, re.IGNORECASE)
    for name in table_names:
        if not name.islower() and not name.lower().startswith('pg_'):
            raise ValueError(f"Nombre de tabla inválido: '{name}'. Todos los nombres de tablas deben estar en minúsculas.")
            
    return sanitized_sql

# --- FUNCIÓN DE EJECUCIÓN ---
def _exec_query(sql: str, parameters: Optional[List[Any]], row_limit: int) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = get_connection()
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, parameters)
            if cur.description is None:
                conn.commit()
                return []
            return [dict(r) for r in cur.fetchmany(row_limit)]
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
    try:
        logger.info(f"Recibido SQL crudo: \"{input.sql}\"")
        sanitized_sql = validate_and_sanitize_sql(input.sql)
        logger.info(f"Ejecutando SQL sanitizado: \"{sanitized_sql}\"")
        
        if not CONNECTION_STRING:
            return []
        
        return _exec_query(sanitized_sql, input.parameters, input.row_limit)

    except ValueError as ve:
        logger.error(f"Error de validación de SQL: {str(ve)}")
        raise Exception(f"Error de validación: {str(ve)}")

# --- BLOQUE DE EJECUCIÓN ---
if __name__ == "__main__":
    try:
        if args.host: mcp.settings.host = args.host
        if args.port: mcp.settings.port = int(args.port)
        logger.info(f"Starting MCP Postgres server using {args.transport} transport on {mcp.settings.host}:{mcp.settings.port}")
        if args.transport == "sse":
            mcp.run(transport="sse", mount_path=args.mount)
        else:
            mcp.run(transport=args.transport)
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        sys.exit(1)
