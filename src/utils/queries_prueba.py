import argparse
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()


QUERIES = {
    "conteo_tablas": """
        SELECT 'especialidad' AS tabla, COUNT(*) AS total FROM especialidad
        UNION ALL
        SELECT 'campus', COUNT(*) FROM campus
        UNION ALL
        SELECT 'promocion', COUNT(*) FROM promocion
        UNION ALL
        SELECT 'profesores', COUNT(*) FROM profesores
        UNION ALL
        SELECT 'proyectos', COUNT(*) FROM proyectos
        UNION ALL
        SELECT 'alumnos', COUNT(*) FROM alumnos
        UNION ALL
        SELECT 'profesor_promocion', COUNT(*) FROM profesor_promocion
        UNION ALL
        SELECT 'resultados', COUNT(*) FROM resultados
        ORDER BY tabla
    """,
    "promociones_detalle": """
        SELECT
            p.id_promocion,
            p.nombre_promocion,
            p.fecha_comienzo,
            c.nombre_campus,
            e.nombre_especialidad
        FROM promocion p
        JOIN campus c ON c.id_campus = p.id_campus
        JOIN especialidad e ON e.id_especialidad = p.id_especialidad
        ORDER BY p.id_promocion
    """,
    "alumnos_por_promocion": """
        SELECT
            p.id_promocion,
            p.nombre_promocion,
            c.nombre_campus,
            e.nombre_especialidad,
            COUNT(a.id_alumno) AS total_alumnos
        FROM promocion p
        JOIN campus c ON c.id_campus = p.id_campus
        JOIN especialidad e ON e.id_especialidad = p.id_especialidad
        LEFT JOIN alumnos a ON a.id_promocion = p.id_promocion
        GROUP BY
            p.id_promocion,
            p.nombre_promocion,
            c.nombre_campus,
            e.nombre_especialidad
        ORDER BY p.id_promocion
    """,
    "profesores_por_promocion": """
        SELECT
            p.id_promocion,
            p.nombre_promocion,
            c.nombre_campus,
            e.nombre_especialidad,
            pr.id_profesor,
            pr.nombre_profesor,
            pr.rol_profesor,
            pr.modalidad
        FROM profesor_promocion pp
        JOIN profesores pr ON pr.id_profesor = pp.id_profesor
        JOIN promocion p ON p.id_promocion = pp.id_promocion
        JOIN campus c ON c.id_campus = p.id_campus
        JOIN especialidad e ON e.id_especialidad = p.id_especialidad
        ORDER BY p.id_promocion, pr.nombre_profesor
    """,
    "resultados_detalle": """
        SELECT
            a.id_alumno,
            a.nombre_alumno,
            promo.nombre_promocion,
            esp.nombre_especialidad,
            py.nombre_proyecto,
            r.resultado_final
        FROM resultados r
        JOIN alumnos a ON a.id_alumno = r.id_alumno
        JOIN promocion promo ON promo.id_promocion = a.id_promocion
        JOIN especialidad esp ON esp.id_especialidad = promo.id_especialidad
        JOIN proyectos py ON py.id_proyecto = r.id_proyecto
        ORDER BY a.id_alumno, py.id_proyecto
        LIMIT 50
    """,
    "resumen_resultados": """
        SELECT
            py.nombre_proyecto,
            COUNT(*) AS total_resultados,
            SUM(CASE WHEN r.resultado_final = 'Apto' THEN 1 ELSE 0 END) AS total_aptos,
            SUM(CASE WHEN r.resultado_final = 'No Apto' THEN 1 ELSE 0 END) AS total_no_aptos
        FROM resultados r
        JOIN proyectos py ON py.id_proyecto = r.id_proyecto
        GROUP BY py.nombre_proyecto
        ORDER BY py.nombre_proyecto
    """,
    "validacion_especialidad": """
        SELECT
            a.id_alumno,
            a.nombre_alumno,
            promo.nombre_promocion,
            esp_alumno.nombre_especialidad AS especialidad_alumno,
            py.nombre_proyecto,
            esp_proyecto.nombre_especialidad AS especialidad_proyecto
        FROM resultados r
        JOIN alumnos a ON a.id_alumno = r.id_alumno
        JOIN promocion promo ON promo.id_promocion = a.id_promocion
        JOIN especialidad esp_alumno ON esp_alumno.id_especialidad = promo.id_especialidad
        JOIN proyectos py ON py.id_proyecto = r.id_proyecto
        JOIN especialidad esp_proyecto ON esp_proyecto.id_especialidad = py.id_especialidad
        WHERE promo.id_especialidad <> py.id_especialidad
    """,
    "validacion_promociones_huerfanas": """
        SELECT
            a.id_alumno,
            a.nombre_alumno,
            a.id_promocion
        FROM alumnos a
        LEFT JOIN promocion p ON p.id_promocion = a.id_promocion
        WHERE p.id_promocion IS NULL
    """,
}


def ejecutar_query(engine, nombre_query: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"QUERY: {nombre_query}")
    print(f"{'=' * 70}")

    df = pd.read_sql(text(QUERIES[nombre_query]), engine)

    if df.empty:
        print("Sin filas devueltas.")
    else:
        print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Ejecuta consultas de prueba sobre la base de datos PostgreSQL."
    )
    parser.add_argument(
        "--query",
        choices=list(QUERIES.keys()) + ["all"],
        default="all",
        help="Nombre de la query a ejecutar. Por defecto ejecuta todas.",
    )
    args = parser.parse_args()

    db_url = os.getenv("URL_RENDER")
    if not db_url:
        raise ValueError("No se encontro la variable de entorno URL_RENDER.")

    engine = create_engine(db_url)

    if args.query == "all":
        for nombre_query in QUERIES:
            ejecutar_query(engine, nombre_query)
    else:
        ejecutar_query(engine, args.query)


if __name__ == "__main__":
    main()
