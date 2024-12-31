try:
    from datetime import datetime
    import unosof.get_customer_invoice_json.data_base as data_base
    from utils.mail import send_mail
    from utils.data_base import log_to_db
except Exception as e:
    print(f"ERROR, importacion librerias en manage_data, {e}")

# Función para validar y formatear las fechas
def validate_and_format_date(date_str):
    formats = [
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y",
        "%m/%d/%Y", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None

# data_base.delete_table_compounds()
# data_base.delete_table_products()
# data_base.delete_table_boxes()
# data_base.delete_table_invoices()
# data_base.create_table_invoices()
# data_base.create_table_boxes()
# data_base.create_table_products()
# data_base.create_table_compounds()

# Función principal de inserción de invoices
def insert_data(data):
    try:
        # Preparar la base de datos
        conn = data_base.cursor

        for i, invoice in enumerate(data["data"]["invoices"]):
            boxes = invoice.pop("boxes")

            # Validar y formatear las fechas en el nivel de invoice
            for field in ["dt_invoice", "dt_truck", "dt_purchaseorder", "dt_fly", "dt_posted"]:
                if invoice.get(field):
                    formatted_date = validate_and_format_date(str(invoice[field]))
                    invoice[field] = formatted_date if formatted_date else None

            invoices_list = [
                invoice.get("id_invoice"),
                invoice.get("tx_customer_ref"),
                int(invoice.get("mny_freight", 0)), 
                int(invoice.get("id_purchaseorder", 0)), 
                invoice.get("gu_purchaseorder"),
                invoice.get("tx_awb"),
                invoice.get("dt_invoice"),
                invoice.get("nm_cargo"),
                invoice.get("id_customer_elite_shipping"),
                invoice.get("ks_CustomerID"),
                invoice.get("nm_bill"),
                invoice.get("dt_truck"),
                int(invoice.get("id_customer", 0)),
                invoice.get("mny_sri_price"),
                invoice.get("nm_incoterm"),
                invoice.get("dt_purchaseorder"),
                invoice.get("nu_fulls"),
                int(invoice.get("nu_boxes", 0)),
                invoice.get("gu_invoice"),
                invoice.get("gu_bill_customer"),
                invoice.get("dt_fly"),
                invoice.get("nm_client_type"),
                invoice.get("nu_totalstemsPO"),
                invoice.get("nm_ship"),
                invoice.get("nm_truck"),
                invoice.get("tx_oe"),
                invoice.get("tx_hawb"),
                invoice.get("mny_flower"),
                invoice.get("ks_PO"),
                invoice.get("tx_company"),
                invoice.get("dt_posted"),
                invoice.get("id_customer_elite_billing"),
                invoice.get("nm_market"),
                invoice.get("id_customer_floricode"),
                invoice.get("id_customer_ship"),
                int(invoice.get("mny_total", 0)), 
            ]

            # Insertar invoice
            try:
                conn.execute(data_base.get_insert_invoices_query(), invoices_list)
            except Exception as e:
                print(f"Error al insertar invoice: {e}\nDatos: {invoices_list}")

            # Insertar las cajas asociadas al invoice
            for x, box in enumerate(boxes):
                products = box.pop("products")
                box_list = [
                    invoice.get("id_invoice"),
                    box.get("id_box"),
                    box.get("ks_BoxTipeID"),
                    box.get("tp_box"),
                    box.get("nu_width"),
                    box.get("tx_label"),
                    box.get("nu_box_weight"),
                    box.get("nu_length"),
                    box.get("mny_sri_final"),
                    box.get("ks_BoxCode"),
                    box.get("ks_LocationID"),
                    box.get("nm_box"),
                    box.get("id_box_elite"),
                    box.get("nu_height"),
                    box.get("tx_box_message"),
                ]

                try:
                    conn.execute(data_base.get_insert_boxes_query(), box_list)
                except Exception as e:
                    print(f"Error al insertar box: {e}\nDatos: {box_list}")

                # Insertar los productos asociados a cada caja
                for y, product in enumerate(products):
                    compounds = product.pop("compounds")

                    try:
                        product_list = [
                            box.get("id_box"),
                            product.get("gu_product"),
                            product.get("mny_rate_stem"),
                            product.get("mny_freight_unit"),
                            product.get("id_floricode"),
                            product.get("ks_ProductID"),
                            product.get("nu_length"),
                            product.get("nu_stems_bunch"),
                            product.get("id_elite_grade"),
                            product.get("nm_brand"),
                            product.get("nm_location"),
                            product.get("nu_weight"),
                            product.get("barcodes"),
                            product.get("mny_sri_final"),
                            product.get("nm_alternate"),
                            product.get("nm_variety"),
                            product.get("id_migros_variety"),
                            product.get("nu_bunches"),
                            product.get("id_elite_final_product"),
                            product.get("nm_species"),
                            product.get("nm_product"),
                        ]

                        conn.execute(data_base.get_insert_product_query(), product_list)
                    except Exception as e:
                        print(f"Error al insertar producto: {e}\nDatos: {product}")

                    for z, compound in enumerate(compounds):

                        try:
                            compound_list = [
                                compound.get("nu_quantity"),
                                compound.get("nm_variety"),
                                compound.get("nm_species"),
                                compound.get("id_elite_variety"),
                            ]
                            
                            conn.execute(data_base.get_insert_compound_query(), compound_list)
                        except Exception as e:
                            print(f"ERROR al insertar compound: {e}\nDatos: {compound_list}")
        
        conn.commit()
        print("Informacion guardada en la base de datos")
    except Exception as e:
        message = f"ERROR, en la funcion de guardado Get Customer Invoice, {e}"
        log_to_db(10, 'ERROR', message, endpoint='insert_data', status_code=500)
        send_mail(message)