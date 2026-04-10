import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# 1. CARGA DE DATOS
df_clase1 = pd.read_csv(r"src\data\clase_1.csv", sep=';')
df_clase2 = pd.read_csv(r"src\data\clase_2.csv", sep=';')
df_clase3 = pd.read_csv(r"src\data\clase_3.csv", sep=';')
df_clase4 = pd.read_csv(r"src\data\clase_4.csv", sep=';')
df_claustro = pd.read_csv(r"src\data\claustro.csv", sep=';')

# 2. LIMPIEZA Y ESTANDARIZACIÓN
def limpiar_df(df):
    # Emails
    if "Email" in df.columns:
        df["Email"] = df["Email"].str.lower().str.strip().str.replace('á', 'a').str.replace('é', 'e').str.replace('í', 'i').str.replace('ó', 'o').str.replace('ú', 'u').str.replace('ü', 'u')

    # Renombres
    if "Proyecto_FullSatck" in df.columns:
        df = df.rename(columns={"Proyecto_FullSatck": "Proyecto_FullStack"})
    if "Vertical" in df.columns:
        df = df.rename(columns={"Vertical": "Curso"})

    # Protecciones para Constraints de SQL (Asegurar 'LI' y 'TA')
    if "Rol" in df.columns:
        df["Rol"] = df["Rol"].replace({"Lead Instructor": "LI", "Teacher Assistant": "TA", "Instructor": "LI"})

    return df

df_1 = limpiar_df(df_clase1)
df_2 = limpiar_df(df_clase2)
df_3 = limpiar_df(df_clase3)
df_4 = limpiar_df(df_clase4)
df_5 = limpiar_df(df_claustro)

# 3. CONSTRUCCIÓN DE TABLAS
def generar_modelo_relacional():
    # --- TABLA ESPECIALIDAD ---
    df_especialidad = pd.DataFrame({
        "id_especialidad": [1, 2],
        "Nombre_Especialidad": ["DS", "FS"] # Mismo nombre que en SQL
    })
    # --- TABLA CAMPUS ---
    df_campus = pd.DataFrame({
        "ID_Campus": [1, 2],
        "Nombre_Campus": ["Madrid", "Valencia"]
    })
    # --- TABLA PROMOCIONES ---
    df_promociones = pd.DataFrame({
        "ID_Promocion": [1, 2, 3, 4],
        "Nombre_Promocion": ["DS_Sept", "DS_Feb", "FS_Sept", "FS_Feb"],
        "Fecha_Comienzo": ["2023-09-18", "2024-02-12", "2023-09-18", "2024-02-12"],
        "ID_Campus": [1, 1, 1, 2],       # INFERENCIA: 1=Madrid, 2=Valencia
        "id_especialidad": [1, 1, 2, 2]  # 1=DS, 2=FS
    })

    # --- TABLA PROFESORES ---
    df_profesores = df_5[["Nombre", "Rol", "Modalidad"]].copy()
    df_profesores.columns = ["Nombre_Profesor", "Rol_Profesor", "Modalidad"]
    df_profesores.insert(0, 'ID_Profesor', range(1, 1 + len(df_profesores)))

    # --- TABLA PROYECTOS ---
    proyectos_data = [
        ("Proyecto_HLF", 1), ("Proyecto_EDA", 1), ("Proyecto_BBDD", 1), ("Proyecto_ML", 1), 
        ("Proyecto_Deployment", 1), ("Proyecto_WebDev", 2), ("Proyecto_FrontEnd", 2), 
        ("Proyecto_Backend", 2), ("Proyecto_React", 2), ("Proyecto_FullStack", 2)
    ]
    df_proyectos = pd.DataFrame(proyectos_data, columns=["Nombre_Proyecto", "id_especialidad"])
    df_proyectos.insert(0, 'ID_Proyecto', range(1, 1 + len(df_proyectos)))

    # --- TABLA ALUMNOS ---
    df_1['ID_Promocion'] = 1
    df_2['ID_Promocion'] = 2
    df_3['ID_Promocion'] = 3
    df_4['ID_Promocion'] = 4

    df_alumnos_raw = pd.concat([df_1, df_2, df_3, df_4], ignore_index=True)
    
    df_alumnos = df_alumnos_raw[["Nombre", "Email", "ID_Promocion"]].copy()
    df_alumnos.columns = ["Nombre_Alumno", "Email_Alumno", "ID_Promocion"]
    df_alumnos.insert(0, 'ID_Alumno', range(1, 1 + len(df_alumnos)))

    # --- TABLA RESULTADOS ---
        # 1. Unimos el df crudo con los IDs de alumno
    df_raw_con_ids = df_alumnos_raw.copy()
    df_raw_con_ids['ID_Alumno'] = df_alumnos['ID_Alumno']

        # 2. Extraemos solo el ID y las columnas de los proyectos
    cols_proyectos = [p for p in df_proyectos['Nombre_Proyecto'].tolist() if p in df_raw_con_ids.columns]
    df_a_derretir = df_raw_con_ids[['ID_Alumno'] + cols_proyectos]

        # 3. El MELT: Convertimos las columnas de proyectos en filas
    df_resultados_melt = df_a_derretir.melt(
        id_vars=['ID_Alumno'], 
        value_vars=cols_proyectos, 
        var_name='Nombre_Proyecto', 
        value_name='Resultado_Final'
    )

        # 4. Limpiamos: Quitamos donde el alumno no tenga nota (NaN) o espacios en blanco
    df_resultados_melt = df_resultados_melt.dropna(subset=['Resultado_Final'])
    df_resultados_melt = df_resultados_melt[df_resultados_melt['Resultado_Final'].isin(['Apto', 'No Apto'])]

        # 5. Cruzamos con df_proyectos para obtener el ID_Proyecto y descartar el Nombre_Proyecto
    df_resultados = pd.merge(df_resultados_melt, df_proyectos[['ID_Proyecto', 'Nombre_Proyecto']], on='Nombre_Proyecto', how='left')
    df_resultados = df_resultados[['ID_Alumno', 'ID_Proyecto', 'Resultado_Final']]
    df_resultados.insert(0, 'ID_Resultado', range(1, 1 + len(df_resultados)))

    # --- TABLA PROFESOR_PROMOCION ---
    df_puente = df_5.copy()
    df_puente = pd.merge(df_puente, df_profesores[['Nombre_Profesor', 'ID_Profesor']], left_on='Nombre', right_on='Nombre_Profesor')
    df_puente['Mes_Corto'] = df_puente['Promocion'].replace({"Septiembre": "Sept", "Febrero": "Feb"})
    df_puente['Clave_Promo'] = df_puente['Curso'] + "_" + df_puente['Mes_Corto']
    
    df_puente = pd.merge(df_puente, df_promociones[['Nombre_Promocion', 'ID_Promocion']], left_on='Clave_Promo', right_on='Nombre_Promocion')
    
    df_profesor_promocion = df_puente[['ID_Profesor', 'ID_Promocion']].drop_duplicates().reset_index(drop=True)
    df_profesor_promocion.insert(0, 'ID_Profesor_Promocion', range(1, 1 + len(df_profesor_promocion)))

    return (df_especialidad, df_campus, df_promociones, df_profesores, df_proyectos, df_alumnos, df_resultados, df_profesor_promocion)

# EJECUCIÓN
(df_especialidad, df_campus, df_promociones, df_profesores, 
 df_proyectos, df_alumnos, df_resultados, df_prof_prom) = generar_modelo_relacional()



def subir_a_postgres(tuplas_tablas, db_url):
    """
    Sube los DataFrames a una base de datos PostgreSQL respetando la integridad referencial.
    
    tuplas_tablas: Una tupla con los 8 DataFrames en un orden específico.
    db_url: El string de conexión a tu base de datos en Render.
    """
    
    (df_especialidad, df_campus, df_promociones, df_profesores, 
     df_proyectos, df_alumnos, df_resultados, df_profesor_promocion) = tuplas_tablas

    # Creamos el motor de conexión
    print("Conectando a la base de datos...")
    engine = create_engine(db_url)

    # DICCIONARIO DE CARGA: Define el orden estricto de inserción y el nombre exacto de la tabla en SQL
    # Las llaves (keys) deben coincidir EXACTAMENTE con los nombres de tabla en CREATE TABLE
    orden_de_carga = {
        "especialidad": df_especialidad,
        "campus": df_campus,
        "profesores": df_profesores,       # Independiente
        "promocion": df_promociones,       # Depende de campus y especialidad
        "proyectos": df_proyectos,         # Depende de especialidad
        "alumnos": df_alumnos,             # Depende de promocion
        "profesor_promocion": df_profesor_promocion, # Depende de profesor y promocion
        "resultados": df_resultados        # Depende de alumno y proyecto
    }

    print("Iniciando la carga de datos...")
    
    try:
        # Abrimos una conexión para poder hacer todo el bloque
        with engine.begin() as conexion:
            for nombre_tabla, df in orden_de_carga.items():
                print(f"Subiendo tabla: {nombre_tabla.upper()} ({len(df)} filas)...")
                df.columns = df.columns.str.lower()

                # to_sql:
                # name = nombre de la tabla en postgres
                # con = conexión
                # if_exists='append' = Añade los datos a las tablas vacías que ya creaste en pgAdmin
                # index=False = No sube el índice automático de Pandas, usamos el tuyo
                df.to_sql(name=nombre_tabla, con=conexion, if_exists='append', index=False)
                
        print("¡Éxito! Todas las tablas han sido cargadas sin violar claves foráneas.")
        
    except Exception as e:
        print("\n[ERROR CRÍTICO] La carga ha fallado. Postgres ha bloqueado la operación y se ha hecho un ROLLBACK (no se guardó nada a medias).")
        print("Motivo del rechazo:\n")
        print(e)





# 1. Ejecutas tu función de transformación
tablas_generadas = generar_modelo_relacional()

# 2. Configuras tus credenciales de Render
URL_RENDER = os.getenv("URL_RENDER")
if not URL_RENDER:
    raise ValueError("¡Error! No se encontró URL_RENDER.")

# 3. Disparas la carga
subir_a_postgres(tablas_generadas, URL_RENDER)