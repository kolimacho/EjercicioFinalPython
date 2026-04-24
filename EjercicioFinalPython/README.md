# Proyecto ETL básico

##  Descripción
Este proyecto consiste en un pequeño pipeline ETL en Python que procesa archivos CSV de clientes y tarjetas.

Se encarga de limpiar los datos, validar algunos campos y generar un nuevo archivo con la información corregida.

---

##  Qué hace

- Lee archivos CSV
- Limpia datos (espacios, formato, etc.)
- Valida:
  - DNI
  - Teléfono
  - Correo
- Añade columnas indicando si los datos son correctos o no
- Genera un CSV limpio en la carpeta `output/`

---

##  Estructura
- input/ -> archivos originales
- output/ -> archivos procesados
- main.py -> script principal
- README.md

---

##  Cómo usarlo

1. Colocar los CSV en la carpeta `input/`
2. Ejecutar: python main.py


3. El resultado se genera en `output/`

---

##  Estado

En desarrollo.  
Por ahora está implementada la limpieza y validación de datos.

---

##  Equipo

- Pablo Ojeda
- Iker Corral
- Javier Albarracin
