try:
    from utils.data_base import get_value_from_db, cursor
except Exception as e:
    print(f"ERROR, importacione en data_base Unosof Invoices, {e}")

def delete_table_invoices():
    try:
        with cursor:
            cursor.execute("""
                        IF EXISTS (SELECT * FROM sysobjects WHERE name='rptUnosof_API_GetCustomer_Invoices_Dev' AND xtype='U')
                        BEGIN
                            DROP TABLE rptUnosof_API_GetCustomer_Invoices_Dev
                        END
                    """   
                )
            cursor.commit()
            print("Tabla invoices eliminada")
    except Exception as e:
        print(f"ERROR, al eliminar la tabla invoices, {e}")

def create_table_invoices():
    try:
        with cursor:
            cursor.execute(
            """CREATE TABLE rptUnosof_API_GetCustomer_Invoices_Dev (
                inv_id_invoice INT PRIMARY KEY,
                inv_tx_customer_ref NVARCHAR(200),
                inv_mny_freight INT,
                inv_id_purchaseorder INT,
                inv_gu_purchaseorder NVARCHAR(200),
                inv_tx_awb NVARCHAR(50),
                inv_dt_invoice DATETIME,
                inv_nm_cargo NVARCHAR(50),
                inv_id_customer_elite_shipping NVARCHAR(20),
                inv_ks_CustomerID NVARCHAR(20),
                inv_nm_bill NVARCHAR(200),
                inv_dt_truck DATETIME,
                inv_id_customer INT,
                inv_mny_sri_price FLOAT,
                inv_nm_incoterm NVARCHAR(20),
                inv_dt_purchaseorder DATETIME,
                inv_nu_fulls FLOAT,
                inv_nu_boxes INT,
                inv_gu_invoice NVARCHAR(200) UNIQUE,
                inv_gu_bill_customer NVARCHAR(200),
                inv_dt_fly DATETIME,
                inv_nm_client_type NVARCHAR(50),
                inv_nu_totalstemsPO INT,
                inv_nm_ship NVARCHAR(200),
                inv_nm_truck VARCHAR(20),
                inv_tx_oe NVARCHAR(50),
                inv_tx_hawb NVARCHAR(200),
                inv_mny_flower FLOAT,
                inv_ks_PO NVARCHAR(20),
                inv_tx_company NVARCHAR(75),
                inv_dt_posted DATETIME,
                inv_id_customer_elite_billing NVARCHAR(20),
                inv_nm_market NVARCHAR(200),
                inv_id_customer_floricode NVARCHAR(20),
                inv_id_customer_ship NVARCHAR(20),
                inv_mny_total INT
            )
            """
            )
            cursor.commit()
            print("Tabla invoices creada")
    except Exception as e:
        print(f"ERROR al crear la tabla Invoices, {e}")

def delete_table_boxes():
    try:
        with cursor:
            cursor.execute("""
                    IF EXISTS (SELECT * FROM sysobjects WHERE name='rptUnosof_API_GetCustomer_Boxes_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rptUnosof_API_GetCustomer_Boxes_Dev
                    END
                """   
            )
            cursor.commit()
            print("Tabla boxes eliminada")
    except Exception as e:
        print(f"ERROR, al eliminar la tabla boxes, {e}")

# delete_table_boxes()
# delete_table_invoices()

def create_table_boxes():
    try:
        with cursor:
            cursor.execute(
                """CREATE TABLE rptUnosof_API_GetCustomer_Boxes_Dev (
                    box_id_invoice INT,
                    box_id_box NVARCHAR(100) UNIQUE,
                    box_ks_BoxTipeID NVARCHAR(50),
                    box_tp_box NVARCHAR(50),
                    box_nu_width FLOAT,
                    box_tx_label NVARCHAR(100),
                    box_nu_box_weight FLOAT,
                    box_nu_length FLOAT,
                    box_mny_sri_final FLOAT,
                    box_ks_BoxCode NVARCHAR(50),
                    box_ks_LocationID NVARCHAR(50),
                    box_nm_box NVARCHAR(100),
                    box_id_box_elite NVARCHAR(50),
                    box_nu_height FLOAT,
                    box_tx_box_message NVARCHAR(255),
                    CONSTRAINT FK_Box_Invoice FOREIGN KEY (box_id_invoice) REFERENCES rptUnosof_API_GetCustomer_Invoices_Dev(inv_id_invoice)
                )
                """
            )
            cursor.commit()
            print("Tabla boxes creada")
    except Exception as e:
        print(f"ERROR al crear la tabla Boxes, {e}")

def delete_table_products():
    try:
        cursor.execute("""
                    IF EXISTS (SELECT * FROM sysobjects WHERE name='rptUnosof_API_GetCustomer_Products_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rptUnosof_API_GetCustomer_Products_Dev
                    END
                """   
            )
        cursor.commit()
        print("Tabla products eliminado")
    except Exception as e:
        print(f"ERROR, al eliminar la tabla productos, {e}")

def create_table_products():
    try:
        with cursor:
            cursor.execute("""
                CREATE TABLE rptUnosof_API_GetCustomer_Products_Dev (
                    pro_id_product INT IDENTITY(1,1) PRIMARY KEY,
                    pro_id_box NVARCHAR(100),
                    pro_gu_product NVARCHAR(100),
                    pro_mny_rate_stem FLOAT,
                    pro_mny_freight_unit FLOAT,
                    pro_id_floricode INT,
                    pro_ks_ProductID NVARCHAR(50),
                    pro_nu_length INT,
                    pro_nu_stems_bunch INT,
                    pro_id_elite_grade NVARCHAR(20),
                    pro_nm_brand NVARCHAR(20),
                    pro_nm_location NVARCHAR(10),
                    pro_nu_weight INT,
                    pro_barcodes NVARCHAR(MAX),
                    pro_mny_sri_final FLOAT,
                    pro_nm_alternate NVARCHAR(200),
                    pro_nm_variety NVARCHAR(100),
                    pro_id_migros_variety NVARCHAR(30),
                    pro_nu_bunches INT,
                    pro_id_elite_final_product NVARCHAR(50),
                    pro_nm_species NVARCHAR(100),
                    pro_nm_product NVARCHAR(200),
                    CONSTRAINT FK_Product_Box FOREIGN KEY (pro_id_box) REFERENCES rptUnosof_API_GetCustomer_Boxes_Dev(box_id_box)
                )
            """)
            cursor.commit()
            print("Tabla products creada")
    except Exception as e:
        print(f"ERROR al crear la tabla Products, {e}")

def delete_table_compounds():
    try:
        cursor.execute("""
                    IF EXISTS (SELECT * FROM sysobjects WHERE name='rptUnosof_API_GetCustomer_Compounds_Dev' AND xtype='U')
                    BEGIN
                        DROP TABLE rptUnosof_API_GetCustomer_Compounds_Dev
                    END
                """   
            )
        cursor.commit()
        print("Tabla compounds eliminado")
    except Exception as e:
        print(f"ERROR, al eliminar la tabla compounds {e}")

def create_table_compounds():
    try:
        with cursor:
            cursor.execute("""
                CREATE TABLE rptUnosof_API_GetCustomer_Compounds_Dev (
                com_id_product INT IDENTITY(1,1) PRIMARY KEY,
                com_nu_quantity INT,
                com_nm_variety NVARCHAR(100),
                com_nm_species NVARCHAR(100),
                com_id_elite_variety NVARCHAR(50),
                CONSTRAINT FK_compound_product FOREIGN KEY (com_id_product) REFERENCES rptUnosof_API_GetCustomer_Products_Dev(pro_id_product)
                )

            """)
            cursor.commit()
            print("Tabla compounds creada")
    except Exception as e:
        print(f"ERROR al crear la tabla Compounds")

def log_to_db(log_level, message, endpoint=None, status_code=None):
    with cursor:
        cursor.execute("""
            INSERT INTO Logs_Info (id_group, log_level, message, endpoint, status_code)
            VALUES (?, ?, ?, ?, ?)
        """, 5, log_level, message, endpoint, status_code)
        cursor.commit()

url_api_getCInvJSON_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 13 AND prm_descripcion = 'url_api_getC'"""

api_key_getCInvJSON_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 13 AND prm_descripcion = 'api_key_getC'"""

insert_query_invoices = """INSERT INTO rptUnosof_API_GetCustomer_Invoices_Dev VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

insert_query_boxes = """INSERT INTO rptUnosof_API_GetCustomer_Boxes_Dev VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

insert_query_products = """INSERT INTO rptUnosof_API_GetCustomer_Products_Dev VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

insert_query_compounds = """INSERT INTO rptUnosof_API_GetCustomer_Compounds_Dev VALUES (?,?,?,?)"""


def get_url_CInv():
    return get_value_from_db(url_api_getCInvJSON_query)

def get_api_key():
    return get_value_from_db(api_key_getCInvJSON_query)

def get_insert_invoices_query():
    return insert_query_invoices

def get_insert_boxes_query():
    return insert_query_boxes

def get_insert_product_query():
    return insert_query_products

def get_insert_compound_query():
    return insert_query_compounds