import os
import re
import logging
import pandas as pd
from sqlalchemy import create_engine

# ---------------------------------------------------------
# 1. CONFIGURACIÓN INICIAL
# ---------------------------------------------------------
# Configuramos el sistema de logs para registrar lo que pasa
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Rutas de carpetas
INPUT_DIR = "data/input"
OUTPUT_DIR = "data/output"

# Conexión a la Base de Datos (con PostgreSQL)
DB_URL = "postgresql://usuario:password@localhost:5432/nombre_bd"

# Expresiones regulares para validar exactamente el patrón del nombre
PATRON_CLIENTES = re.compile(r"^Clientes-\d{4}-\d{2}-\d{2}\.csv$")
PATRON_TARJETAS = re.compile(r"^Tarjetas-\d{4}-\d{2}-\d{2}\.csv$")

# ---------------------------------------------------------
# 2. FUNCIONES DE LIMPIEZA Y ANONIMIZACIÓN
# ---------------------------------------------------------
def anonimizar_correo(correo):
    """Oculta parte del correo electrónico (ej: m***@example.com)"""
    if pd.isna(correo) or "@" not in str(correo):
        return correo
    nombre, dominio = str(correo).split("@", 1)
    return f"{nombre[0]}***@{dominio}"

def anonimizar_tarjeta(numero):
    """Oculta la tarjeta dejando solo los últimos 4 dígitos"""
    if pd.isna(numero):
        return numero
    num_limpio = str(numero).replace("-", "").replace(" ", "")
    if len(num_limpio) >= 4:
        return f"****-****-****-{num_limpio[-4:]}"
    return "****"

def limpiar_dataframe(df):
    """Elimina espacios en blanco y normaliza nulos"""
    # Trim de espacios en todas las columnas de tipo texto
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Rellenar valores nulos (NaN) con un string vacío o 'NO_DATA'
    df = df.fillna("NO_DATA")
    return df

# ---------------------------------------------------------
# 3. LÓGICA PRINCIPAL DEL ETL
# ---------------------------------------------------------
def procesar_etl():
    # Aseguramos que las carpetas existen
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Motor de base de datos
    engine = create_engine(DB_URL)
