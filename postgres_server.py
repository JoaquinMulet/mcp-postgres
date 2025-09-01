import os
import json
from typing import List, Dict, Any

import psycopg
from pydantic import BaseModel, Field
from fastmcp import FastMCP, Context

# --- CONFIGURACIÓN ---
# Carga la URL de la base de datos desde las variables de entorno.
# Es una práctica de seguridad fundamental no tener credenciales en el código.
# Tu plataforma de despliegue (como Railway o Render) te permitirá configurar esto.
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("La variable de entorno DATABASE_URL no está configurada.")

# --- MODELOS DE DATOS (PYDANTIC) ---
# Definimos un modelo para la entrada de nuestra herramienta.
# FastMCP usará esto para validar automáticamente las solicitudes entrantes.
# Esto resuelve el problema original de "Invalid request parameters".
class QueryInput(BaseModel):
    """Modelo de entrada para ejecutar una consulta SQL."""
    sql: str = Field(
        ..., # '...' indica que este campo es obligatorio.
        description="La consulta SQL completa a ejecutar en la base de datos PostgreSQL."
    )
    row_limit: int = Field(
        default=100,
        description="El número máximo de filas a devolver. Por defecto es 100."
    )

# --- INICIALIZACIÓN DEL SERVIDOR MCP ---
# Instanciamos el servidor FastMCP. El nombre es visible en herramientas como Claude Desktop.
# Especificamos las dependencias que `fastmcp` debe instalar al desplegar el servidor.
mcp = FastMCP(
    "Postgres Finance Agent Server",
    dependencies=["psycopg[binary]>=3.1.0", "pydantic>=2.6.0"]
)

# --- DEFINICIÓN DE HERRAMIENTAS ---
@mcp.tool()
def run_query_json(input: QueryInput, ctx: Context) -> Dict[str, Any]:
    """
    Ejecuta de forma segura una consulta SQL en la base de datos PostgreSQL y devuelve
    los resultados en formato JSON. Solo se deben ejecutar consultas de lectura (SELECT)
    o de escritura seguras (INSERT, UPDATE) según lo indique el agente.
    """
    ctx.info(f"Recibida solicitud para ejecutar SQL. Límite de filas: {input.row_limit}")
    ctx.info(f"SQL: {input.sql}")

    # Utilizamos un bloque try...except para manejar cualquier error que pueda
    # ocurrir durante la conexión o ejecución de la consulta.
    try:
        # `psycopg.connect` usa la URL de la base de datos para conectarse.
        # El bloque `with` asegura que la conexión se cierre correctamente.
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Ejecutamos la consulta SQL proporcionada.
                cur.execute(input.sql)

                # Si la consulta no devuelve filas (como un INSERT o UPDATE),
                # `cur.description` será None.
                if cur.description is None:
                    status_message = cur.statusmessage or "Comando ejecutado con éxito."
                    ctx.info(f"Consulta sin resultados (posiblemente INSERT/UPDATE). Estado: {status_message}")
                    return {
                        "status": "success",
                        "message": status_message,
                        "rows_affected": cur.rowcount
                    }

                # Obtenemos los nombres de las columnas para construir los diccionarios.
                column_names = [desc[0] for desc in cur.description]
                
                # `fetchmany` recupera un número limitado de filas para evitar sobrecargar
                # la memoria o el contexto del LLM.
                rows = cur.fetchmany(input.row_limit)
                
                # Convertimos las filas (que son tuplas) en una lista de diccionarios.
                # Esto es mucho más fácil de interpretar para un LLM que una lista de tuplas.
                results = [dict(zip(column_names, row)) for row in rows]

                ctx.info(f"Consulta exitosa. Devueltas {len(results)} filas.")
                
                return {
                    "status": "success",
                    "data": results
                }

    except psycopg.Error as e:
        # Si ocurre un error de PostgreSQL (ej. sintaxis inválida),
        # lo capturamos y lo devolvemos en un formato estructurado.
        # Esto es lo que el agente usará para intentar corregir su propia consulta.
        error_message = f"Error de base de datos: {e.diag.message_primary or str(e)}"
        ctx.error(error_message)
        return {
            "status": "error",
            "error": error_message
        }
    except Exception as e:
        # Capturamos cualquier otro error inesperado.
        error_message = f"Error inesperado del servidor: {str(e)}"
        ctx.error(error_message)
        return {
            "status": "error",
            "error": error_message
        }

# --- PUNTO DE ENTRADA (PARA EJECUCIÓN DIRECTA) ---
# Este bloque permite ejecutar el servidor directamente con `python postgres_server.py`.
# El CLI de `fastmcp` (ej. `fastmcp dev`) también usa esto.
if __name__ == "__main__":
    mcp.run()
