try:
    from dotenv import load_dotenv
    import os
    import pyodbc
except Exception as e:
    print(f"Error al importar las librerias en data_base.py")

load_dotenv()

def data_base_conn():
    try:
        conn = pyodbc.connect(
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={os.getenv("DATABASE_SERVER")};'
            f'DATABASE={os.getenv("DATABASE_NAME")};'
            f'UID={os.getenv("DATABASE_USER")};'
            f'PWD={os.getenv("DATABASE_PASSWORD")}'
        )
        cursor = conn.cursor()
        return cursor
    except Exception as e:
        print(f"ERROR al realizar la conexión con la base de datos, {e}")

def log_to_db(id_group, log_level, message, endpoint=None, status_code=None):
    with data_base_conn() as cursor:
        cursor.execute("""
            INSERT INTO Logs_Info (id_group, log_level, message, endpoint, status_code)
            VALUES (?, ?, ?, ?, ?)
        """, id_group, log_level, message, endpoint, status_code)
        data_base_conn().commit()

cursor = data_base_conn()

user_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'user_mail'"""

password_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'password_mail'"""

server_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'server'"""

port_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'port'"""

user_mail_sis_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 5 AND prm_descripcion = 'user_mail_sis'"""

def get_value_from_db(query):
    """
    Función genérica para obtener un valor único de la base de datos.
    """
    try:
        conn = data_base_conn() 
        result = conn.execute(query).fetchone()
        if result:
            return str(result[0])
        else:
            print(f"Error: No se encontró ningún resultado para la consulta: {query}")
            return None
    except Exception as e:
        print(f"Ocurrió un error al ejecutar la consulta: {e}")
        return None

def get_user_mail():
    query = user_mail_query
    return get_value_from_db(query)

def get_password_mail():
    query = password_mail_query
    return get_value_from_db(query)

def get_server_mail():
    query = server_mail_query
    return get_value_from_db(query)

def get_port_mail():
    query = port_mail_query
    return get_value_from_db(query)

def get_mail_sis():
    query = user_mail_sis_query
    return get_mail_sis(query)