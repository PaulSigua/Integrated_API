try:
    from datetime import datetime
    import unosof.get_venture_json_1.data_base as data_base
    from utils.mail import send_mail
    from utils.data_base import log_to_db
except Exception as e:
    print(f"ERROR, importacion librerias en manage_data, {e}")

# Función para validar y formatear las fechas
def validate_and_format_date(date_str):
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",  # ISO 8601 con zona horaria
        "%Y-%m-%dT%H:%M:%S",    # ISO 8601 sin zona horaria
        "%Y-%m-%d %H:%M:%S",    # Fecha y hora separadas por espacio
        "%Y-%m-%d",             # Solo fecha
        "%d-%m-%Y",             # Fecha en formato europeo con guiones
        "%d/%m/%Y",             # Fecha en formato europeo con barras
        "%m/%d/%Y",             # Fecha en formato estadounidense con barras
        "%d/%m/%Y %H:%M:%S",    # Fecha y hora europea
        "%m/%d/%Y %H:%M:%S"     # Fecha y hora estadounidense
    ]
    for fmt in formats:
        try:
            # Analizar la fecha
            parsed_date = datetime.strptime(date_str.strip(), fmt)  
            # Convertir a ISO 8601 estándar
            return parsed_date.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    # Si no coincide ningún formato, registrar error y devolver None
    print(f"ERROR al guardar la fecha: {date_str} no coincide con ningún formato")
    return None

# data_base.delete_table_productos()
# data_base.delete_table_cajas()
# data_base.delete_table_facturas()
# data_base.create_table_facturas()
# data_base.create_table_cajas()
# data_base.create_table_productos()
        
# Función principal de inserción de invoices
def insert_data(data):
    try:
        # Preparar la base de datos
        conn = data_base.cursor

        for i, invoice in enumerate(data["data"]["facturas"]):
            boxes = invoice.pop("cajas")

            # Validar y formatear las fechas en el nivel de invoice
            for field in ["fecha_f", "fecha_s", "fecha_v", "fecha_p", "fecha_sri", "fecha_au"]:
                if invoice.get(field):
                    formatted_date = validate_and_format_date(str(invoice[field]))
                    if formatted_date:
                        invoice[field] = formatted_date
                    else:
                        invoice[field] = None  # Asegurarse de no insertar datos inválidos

            invoices_list = [
                int(invoice.get("id")),
                invoice.get("gu_invoice"),
                invoice.get("gu_purchaseorder"),
                invoice.get("cliente"),
                invoice.get("empresa"),
                invoice.get("t_venta"),
                invoice.get("ciudad"),
                int(invoice.get("paisDestino")),
                invoice.get("Customer_Address"),
                invoice.get("Customer_Phone"),
                invoice.get("Port"),
                invoice.get("l_aerea"),
                invoice.get("destinatario"),
                invoice.get("paisCliente"),
                invoice.get("agencia"),
                int(invoice.get("sucursal")),
                invoice.get("fecha_f"),
                invoice.get("fecha_s"),
                invoice.get("fecha_v"),
                invoice.get("fecha_p"),
                invoice.get("fecha_sri"),
                invoice.get("dae"),
                invoice.get("guia_m"),
                invoice.get("guia_h"),
                invoice.get("species"),
                int(invoice.get("tallos")),
                float(invoice.get("precioUniario")),
                float(invoice.get("subtotal")),
                float(invoice.get("Iva")),
                float(invoice.get("total")),
                invoice.get("numero"),
                invoice.get("clave_a"),
                invoice.get("autorizacion"),
                invoice.get("fecha_au")
            ]

            # Insertar invoice
            try:
                conn.execute(data_base.get_insert_facturas(), invoices_list)
            except Exception as e:
                print(f"Error al insertar invoice: {e}\nDatos: {invoices_list}")

            # Insertar las cajas asociadas al invoice
            for x, box in enumerate(boxes):
                products = box.pop("productos")
                box_list = [
                    int(invoice.get("id")),
                    int(box.get("id")),
                    box.get("tipo"),
                    box.get("bodega"),
                    int(box.get("cantidad")),
                ]

                try:
                    conn.execute(data_base.get_insert_cajas(), box_list)
                except Exception as e:
                    print(f"Error al insertar box: {e}\nDatos: {box_list}")

                # Insertar los productos asociados a cada caja
                for y, product in enumerate(products):

                    try:
                        product_list = [
                            int(box.get("id")),
                            product.get("id"),
                            int(product.get("cantidad")),
                            int(product.get("cantidad_t")),
                            float(product.get("precio_t")),
                        ]

                        conn.execute(data_base.get_insert_productos(), product_list)
                    except Exception as e:
                        print(f"Error al insertar producto: {e}\nDatos: {product}")
        
        conn.commit()
        print("Informacion guardada en la base de datos")
    except Exception as e:
        message = f"ERROR, en la funcion de guardado Get Venture 1, {e}"
        log_to_db(12, 'ERROR', message, endpoint='insert_data', status_code=500)
        send_mail(message)