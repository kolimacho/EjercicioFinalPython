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

    archivos_en_input = os.listdir(INPUT_DIR)
    
    if not archivos_en_input:
        logging.warning("No hay ficheros en la carpeta de entrada.")
        return

    for archivo in archivos_en_input:
        ruta_archivo = os.path.join(INPUT_DIR, archivo)
        
        # Ignorar directorios, procesar solo archivos
        if not os.path.isfile(ruta_archivo):
            continue

        # Validar si es un archivo de Clientes
        es_cliente = PATRON_CLIENTES.match(archivo)
        # Validar si es un archivo de Tarjetas
        es_tarjeta = PATRON_TARJETAS.match(archivo)

        if not (es_cliente or es_tarjeta):
            logging.info(f"IGNORADO: {archivo} no cumple el patrón YYYY-MM-DD.")
            continue

        logging.info(f"PROCESANDO: {archivo}")

        try:
            # --- EXTRACCIÓN (Extract) ---
            # Lectura forzando tipo string para evitar pérdida de ceros a la izquierda
            df = pd.read_csv(ruta_archivo, sep=';', encoding='utf-8', dtype=str, on_bad_lines='warn')

            # --- TRANSFORMACIÓN (Transform) ---
            df = limpiar_dataframe(df)

            # Anonimización según el tipo de archivo
            if es_cliente:
                # Estandarizamos los nombres de las columnas a minúsculas
                df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
                if 'correo' in df.columns:
                    df['correo'] = df['correo'].apply(anonimizar_correo)
                nombre_tabla = 'clientes'

            elif es_tarjeta:
                df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
                if 'numero_tarjeta' in df.columns:
                    df['numero_tarjeta'] = df['numero_tarjeta'].apply(anonimizar_tarjeta)
                if 'cvv' in df.columns:
                    df['cvv'] = "***" # Destruimos el CVV por completo por seguridad
                nombre_tabla = 'tarjetas'

            # Guardar fichero transformado en output/
            ruta_salida = os.path.join(OUTPUT_DIR, f"Transformado_{archivo}")
            df.to_csv(ruta_salida, sep=';', index=False, encoding='utf-8')
            logging.info(f"Fichero transformado guardado en: {ruta_salida}")

            # --- CARGA (Load) ---
            # if_exists='append' inserta los datos. Si la tabla no existe, pandas la crea.
            df.to_sql(name=nombre_tabla, con=engine, if_exists='append', index=False)
            logging.info(f"Datos insertados correctamente en la tabla '{nombre_tabla}' de la BD.")

        except UnicodeDecodeError:
            logging.error(f"Error de codificación en {archivo}. Asegúrate de que es UTF-8.")
        except Exception as e:
            logging.error(f"Error inesperado al procesar {archivo}: {e}")

if __name__ == "__main__":
    logging.info("--- INICIANDO PIPELINE ETL ---")
    procesar_etl()
    logging.info("--- PIPELINE FINALIZADO ---")