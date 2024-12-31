try:
    from datetime import datetime
    import unosof.get_roll_back.data_base as data_base
    from utils.mail import send_mail
    from utils.data_base import log_to_db
except Exception as e:
    print(f"ERROR, importacion librerias en manage_data, {e}")

# data_base.delete_table_RollBack()
# data_base.create_table_RollBack()

# Función principal de inserción de rolls
def insert_data(data):
    try:
        conn = data_base.cursor

        for i, roll in enumerate(data["data"]):

            rolls_list = [
                roll.get("tx_username"),
                roll.get("id_Purchaseorder"),
                roll.get("GU_PURCHASEORDER"),
                roll.get("TX_REASON"),
                roll.get("GU_USER"),
                roll.get("id_invoice"),
                roll.get("ID_DOMAIN"),
                roll.get("DT_CREATION"),
                roll.get("GU_INVOICE"),
            ]

            # Insertar roll
            try:
                conn.execute(data_base.get_insert_roll(), rolls_list)
            except Exception as e:
                print(f"Error al insertar roll: {e}\nDatos: {rolls_list}")
            
        conn.commit()
        print("Informacion guardada en la base de datos")
    except Exception as e:
        message = f"ERROR, en la funcion guardado Get Roll Back {e}"
        log_to_db(11, 'ERROR', message, endpoint='insert_data', status_code=500)
        send_mail(message)