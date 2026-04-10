# 🚀 ETL Pipeline: Sistema de Gestión Académica (Data Science & Full Stack)

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=for-the-badge&logo=sqlalchemy&logoColor=white)

## 📖 Descripción General

Este proyecto consiste en un **Pipeline ETL (Extract, Transform, Load)** completo construido en Python. Su objetivo es procesar datos crudos (archivos CSV) provenientes de la gestión de un bootcamp (alumnos, profesores, campus y resultados de proyectos), transformarlos aplicando reglas de negocio estrictas, y cargarlos en una base de datos relacional normalizada en **PostgreSQL** (alojada en Render).

## 🏗️ Arquitectura y Modelado de Datos

El proyecto transforma datos planos y desestructurados en un modelo relacional robusto, resolviendo desafíos clásicos de ingeniería de datos:

* **Integridad Referencial:** Generación dinámica de Claves Primarias (PK) y Foráneas (FK) en Python antes de la inserción en SQL.
* **Transformación de Formato (Wide to Long):** Uso avanzado de `pd.melt()` para transponer columnas de proyectos en registros transaccionales individuales para la tabla `RESULTADOS`.
* **Resolución de Relaciones Muchos a Muchos:** Construcción automatizada de la tabla intermedia `PROFESOR_PROMOCION` cruzando datos de dimensiones sin *hardcodear* identificadores.
* **Protección de Constraints:** Estandarización de nomenclaturas y corrección de formatos para cumplir con las reglas estrictas (`CHECK`, `UNIQUE`) del motor PostgreSQL.

## ⚙️ Tecnologías Utilizadas

* **Lenguaje:** Python 3.x
* **Manipulación de Datos:** Pandas, NumPy
* **Base de Datos:** PostgreSQL (pgAdmin4 para administración local, Render para producción)
* **Conexión DB:** SQLAlchemy, Psycopg2

## 🚀 Instalación y Despliegue

### 1. Clonar el repositorio
\`\`\`bash
git clone https://github.com/tu_usuario/tu_repositorio.git
cd tu_repositorio
\`\`\`

### 2. Crear y activar el entorno virtual
\`\`\`bash
python -m venv venv
# En Windows:
venv\Scripts\activate
# En macOS/Linux:
source venv/bin/activate
\`\`\`

### 3. Instalar dependencias
Asegúrate de tener un archivo `requirements.txt` con `pandas`, `numpy`, `sqlalchemy` y `psycopg2-binary`.
\`\`\`bash
pip install -r requirements.txt
\`\`\`

### 4. Configurar la Base de Datos
1. Ejecuta el script SQL de creación de tablas (`schema.sql`) en tu servidor PostgreSQL para inicializar la estructura.
2. Asegúrate de que las tablas estén **completamente vacías** antes de la carga inicial.
3. Configura tu cadena de conexión en el script principal (o mediante un archivo `.env`):
\`\`\`python
URL_RENDER = "postgresql://usuario:password@host:puerto/nombre_bd"
\`\`\`

## 🔄 Ejecución del Pipeline

Para ejecutar el proceso ETL completo de limpieza, transformación y carga:

\`\`\`bash
python src/utils/main.py
\`\`\`
*El script notificará por consola el progreso de inserción de cada tabla respetando el orden de dependencias de las claves foráneas.*

## ⚠️ Consideraciones de Ejecución

* **Idempotencia:** Actualmente, el pipeline está diseñado para una **carga inicial masiva** (`if_exists='append'`). Ejecutar el script múltiples veces sin vaciar la base de datos resultará en un error de `UniqueViolation` debido a las restricciones de la base de datos.
* **Case Sensitivity:** El pipeline adapta automáticamente las columnas de los DataFrames a minúsculas en el momento de la carga para asegurar compatibilidad nativa con PostgreSQL.

## Resultado final pgAdmin

![Resultado final pgAdmin](./image.png)

## Queries de prueba

Puedes lanzar las comprobaciones con Python usando `src/utils/queries_prueba.py`.

Ejecutar todas:

```bash
python src/utils/queries_prueba.py
```

Ejecutar solo una:

```bash
python src/utils/queries_prueba.py --query conteo_tablas
```

Queries disponibles:
- comprobar que todas las tablas tienen registros,
- revisar joins entre promociones, alumnos, profesores y proyectos,
- verificar resultados por alumno y por proyecto,
- detectar inconsistencias que deberian devolver 0 filas.

Si prefieres SQL puro, tambien queda disponible `src/sql/queries_prueba.sql`.
