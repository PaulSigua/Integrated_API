try:
    from utils.data_base import get_value_from_db, cursor
except Exception as e:
    print(f"ERROR, importacione en data_base Unosof Invoices, {e}")

def delete_table_RollBack():
    try:
        with cursor:
            cursor.execute("""
                    IF EXISTS (SELECT * FROM sysobjects WHERE name='rptUnosof_API_RollBack_Malima_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rptUnosof_API_RollBack_Malima_Dev
                    END
            """)
            cursor.commit()
            print("Tabla rollback borrada")
    except Exception as e:
        print(f"ERROR, error al eliminar la tabla en get_RB: {e}")

def create_table_RollBack():
    try:
        with cursor:
            cursor.execute("""
                    CREATE TABLE rptUnosof_API_RollBack_Malima_Dev(
                        roll_id INT IDENTITY(1,1) PRIMARY KEY,
                        roll_tx_username NVARCHAR(100),
                        roll_id_purchaseorder NVARCHAR(150),
                        roll_gu_purchaseorder NVARCHAR(200),
                        roll_tx_reason NVARCHAR(MAX),
                        roll_gu_user NVARCHAR(200),
                        roll_id_invoice NVARCHAR(100),
                        roll_id_domain INT,
                        roll_dt_creation NVARCHAR(150),
                        roll_gu_invoice NVARCHAR(150)
                    )
            """)
            cursor.commit()
            print("Tabla facturas creada correctamente")
    except Exception as e:
        print(f"ERROR, al crear la tabla RollBack para get_RB: {e}")
    
url_api_getRB_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 13 AND prm_descripcion = 'url_api_getRB'"""

api_key_getRB_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 13 AND prm_descripcion = 'api_key_getRB'"""

insert_roll_query = """INSERT INTO rptUnosof_API_RollBack_Malima_Dev VALUES (?,?,?,?,?,?,?,?,?)"""


def get_url_getRB():
    return get_value_from_db(url_api_getRB_query)

def get_api_key_getRB():
    return get_value_from_db(api_key_getRB_query)

def get_insert_roll():
    return insert_roll_query