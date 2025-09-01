import os
import sys
import logging
import argparse
import re
import json
from typing import Any, Optional, List, Dict

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, Field, field_validator
from fastmcp import FastMCP, Context
# Ya no necesitamos JSONResponse ni Request de Starlette

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fp-agent-mcp-server')

parser = argparse.ArgumentParser(description="FP-Agent PostgreSQL MCP Server")
parser.add_argument("--conn", dest="conn", default=os.getenv("DATABASE_URL"), help="PostgreSQL connection string")
parser.add_argument("--transport", dest="transport", default="sse", help="Transport protocol")
parser.add_argument("--host", dest="host", default="0.0.0.0", help="Host to bind")
parser.add_argument("--port", dest="port", type=int, default=8000, help="Port to bind")
args, _ = parser.parse_known_args()
CONNECTION_STRING: Optional[str] = args.conn

class QueryInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = Field(default=100, ge=1)
    @field_validator('sql')
    def validate_allowed_operations(cls, value: str) -> str:
        sql_cleaned = value.strip().removesuffix(';').lower()
        if sql_cleaned.startswith(('select', 'with')): return value
        if sql_cleaned.startswith('insert into'): return value
        update_pattern = re.compile(r"^\s*update\s+transactions\s+set\s*=\s*'(void|superseded)'.*$", re.IGNORECASE)
        if update_pattern.match(value.strip()): return value
        if sql_cleaned.startswith('update'): raise ValueError("Operación UPDATE no permitida.")
        dangerous_keywords = ['delete', 'drop', 'create', 'alter', 'truncate']
        if any(sql_cleaned.startswith(keyword) for keyword in dangerous_keywords): raise ValueError(f"Operación SQL peligrosa '{sql_cleaned.split()[0]}' está prohibida.")
        raise ValueError("Tipo de consulta SQL no permitida.")

mcp = FastMCP("FP-Agent PostgreSQL Server", log_level="INFO")

@mcp.tool()
def run_query_json(input: QueryInput, ctx: Context) -> str: # <-- El tipo de retorno es 'str'
    """
    Ejecuta una consulta SQL y devuelve los resultados como un string JSON.
    """
    request_id = ctx.request_id
    result_data = {}

    if not CONNECTION_STRING:
        result_data = {"error": {"code": -32000, "message": "Servidor no configurado para conectar a la base de datos."}}
    else:
        try:
            with psycopg.connect(CONNECTION_STRING) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(input.sql, input.parameters)
                    if cur.description is None:
                        conn.commit()
                        result_obj = {"status": "success", "message": cur.statusmessage or "Comando exitoso.", "rows_affected": cur.rowcount}
                        result_data = {"result": result_obj}
                    else:
                        rows = list(cur.fetchmany(input.row_limit))
                        success_obj = {"status": "success", "data": rows}
                        result_data = {"result": success_obj}
        except Exception as e:
            error_message = f"Error de base de datos: {getattr(e, 'diag', {}).get('message_primary', str(e))}"
            result_data = {"error": {"code": -32001, "message": error_message}}

    final_response_obj = { "jsonrpc": "2.0", "id": request_id, **result_data }
    
    # --- ¡LA SOLUCIÓN A TODO! ---
    # Serializamos manualmente a un string JSON, usando `default=str` para manejar
    # tipos de datos especiales como UUID, datetime, y Decimal.
    # Esto resuelve tanto el error de serialización del UUID como el bug de FastMCP.
    return json.dumps(final_response_obj, default=str)

if __name__ == "__main__":
    if args.host: mcp.settings.host = args.host
    if args.port: mcp.settings.port = args.port
    logger.info("Iniciando FP-Agent MCP Server en %s:%s usando transporte %s", mcp.settings.host, mcp.settings.port, args.transport)
    mcp.run(transport=args.transport)
