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

# --- 1. CONFIGURACIÓN INICIAL ---
# Configura un logging claro que verás en los logs de Railway.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fp-agent-mcp-server')

# --- 2. MANEJO DE ARGUMENTOS Y VARIABLES DE ENTORNO ---
# Esta sección permite configurar el servidor mediante argumentos de línea de comandos
# o variables de entorno, lo cual es ideal para Railway.
parser = argparse.ArgumentParser(description="FP-Agent PostgreSQL MCP Server")
parser.add_argument(
    "--conn",
    dest="conn",
    default=os.getenv("DATABASE_URL"), # Usamos DATABASE_URL como convención
    help="PostgreSQL connection string (DSN)"
)
parser.add_argument(
    "--transport", dest="transport", default="streamable-http",
    help="Transport protocol (default: streamable-http for web deployments)"
)
parser.add_argument(
    "--host", dest="host", default="0.0.0.0",
    help="Host to bind for HTTP transport (default: 0.0.0.0 for containers)"
)
parser.add_argument(
    "--port", dest="port", type=int, default=8000,
    help="Port to bind for HTTP transport (default: 8000)"
)
args, _ = parser.parse_known_args()
CONNECTION_STRING: Optional[str] = args.conn

# --- 3. MODELO DE DATOS CON VALIDACIÓN DE SEGURIDAD ---
# Este modelo Pydantic valida la entrada de la herramienta `run_query_json`.
# Es nuestra principal capa de seguridad.
class QueryInput(BaseModel):
    sql: str
    parameters: Optional[List[Any]] = None
    row_limit: int = Field(default=100, ge=1)

    @field_validator('sql')
    def validate_allowed_operations(cls, value: str) -> str:
        """
        Validador de seguridad que solo permite las operaciones definidas
        en el SYSTEM_PROMPT del agente.
        """
        # Limpiamos y normalizamos la consulta para una validación robusta.
        sql_cleaned = value.strip().removesuffix(';').lower()

        # Permitir SELECT y WITH (para consultas complejas de solo lectura)
        if sql_cleaned.startswith(('select', 'with')):
            return value

        # Permitir INSERT INTO
        if sql_cleaned.startswith('insert into'):
            return value

        # Permitir UPDATE, pero SOLO para cambiar el status de 'transactions'
        update_pattern = re.compile(r"^\s*update\s+transactions\s+set\s+status\s*=\s*'(void|superseded)'.*$", re.IGNORECASE)
        if update_pattern.match(value.strip()):
            return value

        # Bloquear cualquier otro tipo de UPDATE
        if sql_cleaned.startswith('update'):
            raise ValueError("Operación UPDATE no permitida. Solo se permite actualizar el 'status' de las transacciones.")

        # Bloquear explícitamente todas las demás operaciones peligrosas.
        dangerous_keywords = ['delete', 'drop', 'create', 'alter', 'truncate']
        if any(sql_cleaned.startswith(keyword) for keyword in dangerous_keywords):
            raise ValueError(f"Operación SQL peligrosa '{sql_cleaned.split()[0]}' está estrictamente prohibida.")

        # Si no es ninguna de las operaciones permitidas, se rechaza.
        raise ValueError("Tipo de consulta SQL no permitida por las reglas de seguridad.")


# --- 4. INICIALIZACIÓN DEL SERVIDOR MCP ---
mcp = FastMCP(
    "FP-Agent PostgreSQL Server",
    log_level="INFO"
)

# --- 5. DEFINICIÓN DE LA ÚNICA HERRAMIENTA ---
@mcp.tool()
def run_query_json(input: QueryInput, ctx: Context) -> Dict[str, Any]:
    """
    Ejecuta una consulta SQL segura en la base de datos de finanzas personales.
    La consulta es validada antes de la ejecución.
    Devuelve los resultados o un estado de error en formato JSON.
    """
    ctx.info(f"Ejecutando consulta validada. Límite de filas: {input.row_limit}")

    if not CONNECTION_STRING:
        ctx.error("La variable de entorno DATABASE_URL no está configurada.")
        return {"error": "El servidor no está configurado para conectarse a la base de datos."}

    try:
        # El bloque 'with' asegura que la conexión se cierre automáticamente.
        with psycopg.connect(CONNECTION_STRING) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(input.sql, input.parameters)

                # Si la consulta no devuelve filas (ej. INSERT, UPDATE)
                if cur.description is None:
                    conn.commit() # ¡Importante! Confirmar la transacción para escrituras.
                    message = cur.statusmessage or "Comando ejecutado con éxito."
                    ctx.info(message)
                    return {"status": "success", "message": message, "rows_affected": cur.rowcount}

                # Si es un SELECT, recuperamos las filas.
                rows = cur.fetchmany(input.row_limit)
                ctx.info(f"Consulta SELECT exitosa. Devueltas {len(rows)} filas.")
                return {"status": "success", "data": rows}

    except psycopg.Error as e:
        # Captura errores específicos de la base de datos (sintaxis, etc.)
        # y los devuelve de forma estructurada para el bucle de autocorrección del bot.
        error_message = f"Error de base de datos: {e.diag.message_primary or str(e)}"
        ctx.error(error_message)
        return {"error": error_message}
    except Exception as e:
        # Captura cualquier otro error inesperado.
        error_message = f"Error inesperado del servidor: {str(e)}"
        ctx.error(error_message)
        return {"error": error_message}

# --- 6. PUNTO DE ENTRADA PARA LA EJECUCIÓN ---
if __name__ == "__main__":
    logger.info(
        "Iniciando FP-Agent MCP Server en %s:%s usando transporte %s",
        args.host, args.port, args.transport
    )
    # FastMCP usará los argumentos parseados para configurar el servidor.
    mcp.run(
        transport=args.transport,
        host=args.host,
        port=args.port
    )
