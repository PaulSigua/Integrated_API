try:
    from utils.data_base import get_value_from_db, cursor
except Exception as e:
    print(f"ERROR, importacione en data_base Unosof Invoices, {e}")

def delete_table_facturas():
    try:
        with cursor:
            cursor.execute("""
                    IF EXISTS (SELECT * FROM sysobjects WHERE name='rpt_Unosof_API_Venture_Facturas_Malima_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rpt_Unosof_API_Venture_Facturas_Malima_Dev
                    END
            """)
            cursor.commit()
            print("Tabla facturas borrada")
    except Exception as e:
        print(f"ERROR, error al eliminar la tabla en get_venture_json_1: {e}")

def create_table_facturas():
    try:
        with cursor:
            cursor.execute("""
                    CREATE TABLE rpt_Unosof_API_Venture_Facturas_Malima_Dev(
                        fact_id INT PRIMARY KEY,
                        fact_gu_invoice NVARCHAR(200),
                        fact_gu_purchaseorder NVARCHAR(200),
                        fact_cliente NVARCHAR(200),
                        fact_empresa NVARCHAR(100),
                        fact_t_venta NVARCHAR(100),
                        fact_ciudad NVARCHAR(75),
                        fact_paisDestino INT,
                        fact_customer_Address NVARCHAR(200),
                        fact_customer_phone NVARCHAR(200),
                        fact_port NVARCHAR(20),
                        fact_l_aerea NVARCHAR(30),
                        fact_destinatario NVARCHAR(100),
                        fact_pais_cliente NVARCHAR(100),
                        fact_agencia NVARCHAR(100),
                        fact_sucursal INT,
                        fact_fecha_f DATETIME,
                        fact_fecha_s DATETIME,
                        fact_fecha_v DATETIME,
                        fact_fecha_p DATETIME,
                        fact_fecha_sri DATETIME,
                        fact_dae NVARCHAR(100),
                        fact_guia_m NVARCHAR(100),
                        fact_guia_h NVARCHAR(100),
                        fact_species NVARCHAR(100),
                        fact_tallos INT,
                        fact_precioUniario FLOAT,
                        fact_subtotal FLOAT,
                        fact_iva FLOAT,
                        fact_total FLOAT,
                        fact_numero NVARCHAR(100),
                        fact_clave_a NVARCHAR(MAX),
                        fact_autorizacion NVARCHAR(MAX),
                        fact_fecha_au DATETIME
                    )
            """)
            cursor.commit()
            print("Tabla facturas creada correctamente")
    except Exception as e:
        print(f"ERROR, al crear la tabla facturas para get_venture_json_1: {e}")

def delete_table_cajas():
    try:
        with cursor:
            cursor.execute("""
                IF EXISTS (SELECT * FROM sysobjects WHERE name='rpt_Unosof_API_Venture_Cajas_Malima_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rpt_Unosof_API_Venture_Cajas_Malima_Dev
                    END
            """)
            cursor.commit()
            print("Tabla cajas borrada")
    except Exception as e:
        print(f"ERROR, error al eliminar la tabla en get_venture_json_1: {e}")

def create_table_cajas():
    try:
        with cursor:
            cursor.execute("""
                    CREATE TABLE rpt_Unosof_API_Venture_Cajas_Malima_Dev(
                        caj_fact_id INT,
                        caj_id INT PRIMARY KEY,
                        caj_tipo NVARCHAR(100),
                        caj_bodega NVARCHAR(100),
                        caj_cantidad INT,
                        CONSTRAINT FK_caja_factura FOREIGN KEY (caj_fact_id) REFERENCES rpt_Unosof_API_Venture_Facturas_Malima_Dev(fact_id)
                    )
            """)
            cursor.commit()
            print("Tabla cajas creada correctamente")
    except Exception as e:
        print(f"ERROR, al crear la tabla Cajas: {e}")

def delete_table_productos():
    try:
        with cursor:
            cursor.execute("""
                IF EXISTS (SELECT * FROM sysobjects WHERE name='rpt_Unosof_API_Venture_Productos_Malima_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rpt_Unosof_API_Venture_Productos_Malima_Dev
                    END
            """)
            cursor.commit()
            print("Tabla productos borrada")
    except Exception as e:
        print(f"ERROR, error al eliminar la tabla en get_venture_json_1: {e}")

def create_table_productos():
    try:
        with cursor:
            cursor.execute("""
                    CREATE TABLE rpt_Unosof_API_Venture_Productos_Malima_Dev(
                        pro_caj_id INT,
                        pro_id NVARCHAR(100),
                        pro_cantidad INT,
                        pro_cantidad_t INT,
                        pro_precio_t FLOAT,
                        CONSTRAINT FK_producto_caja FOREIGN KEY (pro_caj_id) REFERENCES rpt_Unosof_API_Venture_Cajas_Malima_Dev(caj_id)
                    )  
            """)
            cursor.commit()
            print("Tabla productos creada correctamente")
    except Exception as e:
        print(f"ERROR, al crear la tabla Cajas: {e}")

url_api_getVen2JSON_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 13 AND prm_descripcion = 'url_api_getV2'"""

api_key_getCVen2JSON_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 13 AND prm_descripcion = 'api_key_getV2'"""

insert_facturas_query = """INSERT INTO rpt_Unosof_API_Venture_Facturas_Malima_Dev VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

insert_cajas_query = """INSERT INTO rpt_Unosof_API_Venture_Cajas_Malima_Dev VALUES (?,?,?,?,?)"""

insert_productos_query = """INSERT INTO rpt_Unosof_API_Venture_Productos_Malima_Dev VALUES (?,?,?,?,?)"""

def get_url_getVen2JSON():
    return get_value_from_db(url_api_getVen2JSON_query)

def get_api_key_getCVen2JSON():
    return get_value_from_db(api_key_getCVen2JSON_query)

def get_insert_facturas():
    return insert_facturas_query

def get_insert_cajas():
    return insert_cajas_query

def get_insert_productos():
    return insert_productos_query