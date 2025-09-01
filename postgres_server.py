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

# No necesitamos nada de starlette, lo que simplifica el código.

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
def run_query_json(input: QueryInput, ctx: Context) -> Dict[str, Any]: # <-- El tipo de retorno es un simple dict
    """
    Ejecuta una consulta SQL y devuelve un diccionario de Python limpio y serializable.
    """
    result_obj = {}
    if not CONNECTION_STRING:
        result_obj = {"error": "Servidor no configurado para conectar a la base de datos."}
    else:
        try:
            with psycopg.connect(CONNECTION_STRING) as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(input.sql, input.parameters)
                    if cur.description is None:
                        conn.commit()
                        result_obj = {"status": "success", "message": cur.statusmessage or "Comando exitoso.", "rows_affected": cur.rowcount}
                    else:
                        rows = list(cur.fetchmany(input.row_limit))
                        result_obj = {"status": "success", "data": rows}
        except Exception as e:
            result_obj = {"error": f"Error de base de datos: {str(e)}"}

    # --- ¡LA SOLUCIÓN A TODO! ---
    # 1. Convertimos el objeto de Python (que puede tener UUIDs, etc.) a un string JSON.
    #    `default=str` convierte cualquier tipo de dato no estándar a su representación de string.
    json_string = json.dumps(result_obj, default=str)
    
    # 2. Convertimos ese string JSON de vuelta a un objeto de Python.
    #    Este nuevo objeto es "limpio": todos los UUIDs, etc., son ahora strings.
    clean_python_object = json.loads(json_string)
    
    # 3. Devolvemos este objeto limpio. FastMCP ahora podrá serializarlo sin problemas.
    return clean_python_object

@mcp.tool()
def get_system_context(ctx: Context) -> Dict[str, Any]:
    """
    Recupera toda la información contextual clave (cuentas, categorías, comercios, tags)
    en una sola llamada para inyectarla en el system prompt del agente.
    """
    if not CONNECTION_STRING:
        return {"error": "Servidor no configurado para conectar a la base de datos."}

    context_data = {}
    try:
        with psycopg.connect(CONNECTION_STRING) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Obtenemos las cuentas
                cur.execute("SELECT account_id, account_name, account_type, currency_code FROM accounts ORDER BY account_name;")
                context_data['accounts'] = list(cur.fetchall())
                
                # Obtenemos las categorías
                cur.execute("SELECT category_id, category_name FROM categories ORDER BY category_name;")
                context_data['categories'] = list(cur.fetchall())

                # Obtenemos los comercios
                cur.execute("SELECT merchant_id, merchant_name FROM merchants ORDER BY merchant_name;")
                context_data['merchants'] = list(cur.fetchall())

                # Obtenemos los tags
                cur.execute("SELECT tag_id, tag_name FROM tags ORDER BY tag_name;")
                context_data['tags'] = list(cur.fetchall())
    except Exception as e:
        return {"error": f"Error de base de datos al obtener contexto: {str(e)}"}

    # Usamos la misma técnica de "limpieza" para asegurar que todo sea serializable
    json_string = json.dumps(context_data, default=str)
    return json.loads(json_string)

if __name__ == "__main__":
    if args.host: mcp.settings.host = args.host
    if args.port: mcp.settings.port = args.port
    logger.info("Iniciando FP-Agent MCP Server en %s:%s usando transporte %s", mcp.settings.host, mcp.settings.port, args.transport)
    mcp.run(transport=args.transport)
