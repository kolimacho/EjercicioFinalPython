# Proyecto ETL - Clientes y Tarjetas

## 🧾 Descripción

Este proyecto implementa un pipeline ETL en Python que procesa archivos CSV de clientes y tarjetas.

El sistema detecta automáticamente los archivos correctos, limpia los datos, anonimiza información sensible y genera nuevos archivos listos para su uso o carga en base de datos.

---

## ⚙️ Qué hace el programa

* Detecta archivos con nombres válidos:

  * `Clientes-YYYY-MM-DD.csv`
  * `Tarjetas-YYYY-MM-DD.csv`
* Limpia los datos:

  * Elimina espacios
  * Rellena valores nulos
* Anonimiza datos sensibles:

  * Correos (ej: m***@email.com)
  * Tarjetas (solo muestra últimos 4 dígitos)
* Genera archivos nuevos en `data/output/`
* Prepara conexión a base de datos (PostgreSQL)
* Registra información del proceso mediante logs

---

## 📁 Estructura del proyecto

```
data/
 ┣ input/     -> archivos originales
 ┗ output/    -> archivos procesados

main.py       -> script principal
requirements.txt -> dependencias del proyecto
README.md
```

---

## ▶️ Cómo usarlo

1. Instalar dependencias:

```
pip install -r requirements.txt
```

2. Colocar los archivos CSV en:

```
data/input/
```

3. Ejecutar el script:

```
python main.py
```

4. Los archivos procesados se guardan en:

```
data/output/
```

---

## 📌 Reglas de los archivos

* Formato: CSV separado por `;`
* Codificación: UTF-8
* Nombres obligatorios:

  * Clientes-YYYY-MM-DD.csv
  * Tarjetas-YYYY-MM-DD.csv
* Archivos incorrectos se ignoran

---

## 🔐 Tratamiento de datos

* Correos parcialmente ocultos
* Tarjetas enmascaradas (solo últimos 4 dígitos)
* No se almacenan datos sensibles completos

---

## 📊 Estado del proyecto

En desarrollo.
Actualmente implementada la limpieza, anonimización y generación de archivos.

---

##  Equipo

- Pablo Ojeda
- Iker Corral
- Javier Albarracin
