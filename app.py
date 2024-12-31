try:
    from fastapi import FastAPI
    from unosof.get_customer_invoice_json.main import get_data
    from unosof.get_customer_invoice_json.manage_data import insert_data
    from datetime import datetime, timedelta
    import uvicorn
    
    from unosof.get_venture_json_1.main import get_data as get_data_venture_1
    from unosof.get_venture_json_1.manage_data import insert_data as insert_data_venture_1
    from unosof.get_venture_json_2.main import get_data as get_data_venture_2
    from unosof.get_venture_json_2.manage_data import insert_data as insert_data_venture_2
    from unosof.get_roll_back.main import get_data as get_data_roll
    from unosof.get_roll_back.manage_data import insert_data as insert_data_roll
    from utils.retry import retry
    from utils.data_base import cursor
    from utils.scheduler import start_scheduler, schedule_daily_task
except Exception as e:
    print(f"ERROR al importar las librerias en app.py, {e}")

app = FastAPI(
    debug=True,
    title="API conectada con la API integrada de Unosof",
    description="Esta API desarrollada con FastAPI permite obtener los datos de la API integrada de Unosof",
    version="1.0.0"
)

# Función para generar las fechas de inicio y fin para cada intervalo de 15 días
def generate_date_ranges(start_date: str, end_date: datetime):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    current_date = start_date_obj
    while current_date < end_date:
        next_date = current_date + timedelta(days=15)
        yield current_date.strftime('%Y-%m-%d'), min(next_date, end_date).strftime('%Y-%m-%d')
        current_date = next_date

@retry(max_retries=5, delay=2)
def perform_customer_invoice():
    try:
        start_date = '2019-01-01'
        end_date = datetime.today()
        all_data = []

        # Iterar sobre las fechas generadas en intervalos de 15 días
        for start, end in generate_date_ranges(start_date, end_date):
            data = get_data(start=start, end=end)  # Llama a la función de obtención de datos
            insert_data(data)  # Inserta los datos obtenidos en la base de datos
            all_data.extend(data)  # Almacena los datos obtenidos en una lista acumulativa
            print(f"Datos insertados para el inicio de fecha: {start} y fin de fecha: {end}")
        
        return {
            "Message": "Datos obtenidos y almacenados correctamente",
            "Total Records": len(all_data)
        }
        
    except Exception as e:
        print(f"ERROR, endpoint getCustomerInvoiceJSON: {e}")
    
@retry(max_retries=5, delay=2)
def perform_roll_back():
    try:
        start_date = '2019-01-01'
        end_date = datetime.today()
        all_data = []

        # Iterar sobre las fechas generadas en intervalos de 15 días
        for start, end in generate_date_ranges(start_date, end_date):
            data = get_data_roll(start=start, end=end)  # Llama a la función de obtención de datos
            insert_data_roll(data)  # Inserta los datos obtenidos en la base de datos
            all_data.extend(data)  # Almacena los datos obtenidos en una lista acumulativa
            print(f"Datos insertados para el inicio de fecha: {start} y fin de fecha: {end}")
        
        return {
            "Message": "Datos obtenidos y almacenados correctamente",
            "Total Records": len(all_data)
        }
        
    except Exception as e:
        print(f"ERROR, endpoint getVentureJSONM2: {e}")

@retry(max_retries=5, delay=2)
def perform_venture_1():
    try:
        start_date = '2019-01-01'
        end_date = datetime.today()
        all_data = []

        # Iterar sobre las fechas generadas en intervalos de 15 días
        for start, end in generate_date_ranges(start_date, end_date):
            data = get_data_venture_1(start=start, end=end)  # Llama a la función de obtención de datos
            insert_data_venture_1(data)  # Inserta los datos obtenidos en la base de datos
            all_data.extend(data)  # Almacena los datos obtenidos en una lista acumulativa
            print(f"Datos insertados para el inicio de fecha: {start} y fin de fecha: {end}")
        
        return {
            "Message": "Datos obtenidos y almacenados correctamente",
            "Total Records": len(all_data)
        }
    except Exception as e:
        print(f"ERROR, endpoint getVentureJSONM1: {e}")

@retry(max_retries=5, delay=2)
def perform_venture_2():
    try:
        start_date = '2019-01-01'
        end_date = datetime.today()
        all_data = []

        # Iterar sobre las fechas generadas en intervalos de 15 días
        for start, end in generate_date_ranges(start_date, end_date):
            data = get_data_venture_2(start=start, end=end)  # Llama a la función de obtención de datos
            insert_data_venture_2(data)  # Inserta los datos obtenidos en la base de datos
            all_data.extend(data)  # Almacena los datos obtenidos en una lista acumulativa
            print(f"Datos insertados para el inicio de fecha: {start} y fin de fecha: {end}")
        
        return {
            "Message": "Datos obtenidos y almacenados correctamente",
            "Total Records": len(all_data)
        }
    except Exception as e:
        print(f"ERROR, endpoint getVentureJSONM2: {e}")

def perform_all():
    try:
        perform_customer_invoice()
    except Exception as e:
        print(f"Error al ejecutar el scraping de ventas: {e}")

    try:
        perform_roll_back()
    except Exception as e:
        print(f"Error al ejecutar el scraping de subastas: {e}")

    try:
        perform_venture_1()
    except Exception as e:
        print(f"Error al ejecutar el scraping de boxes: {e}")

    try:
        perform_venture_2()
    except Exception as e:
        print(f"Error al ejecutar el scraping de clientes: {e}")

    finally:
        print("Tareas realizadas con exito ... ")
        cursor.close()

@app.get("/", description="Endpoint Raiz")
def default_endpoint():
    return { " Message " : " API ejecutandose ... " }

@app.get("/getCustomerInvoice", description="Endpoint para obtener la data")
def getCustomerInvoiceJSON():
    perform_customer_invoice()

@app.get("/getRB")
def getRB():
    perform_roll_back()

@app.get("/getVenture1")
def getVentureJSONM1():
    perform_venture_1()

@app.get("/getVenture2")
def getVentureJSONM2():
    perform_venture_2()

# Inicializar el scheduler al iniciar la aplicación
@app.on_event("startup")
def startup_event():
    scheduler = start_scheduler()
    schedule_daily_task(scheduler, perform_all, hour=1, minute=00, id='api_integrated')

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=9993, reload=True)