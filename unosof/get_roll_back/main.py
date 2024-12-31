try:
    import unosof.get_roll_back.data_base as data_base
    import requests
except Exception as e:
    print(f"ERROR al importar las librerias en main.py, {e}")

def generate_url(start, end):
    base_url = data_base.get_url_getRB()
    apikey = data_base.get_api_key_getRB()
    return base_url.format( api_key=apikey, date_start=start, date_end=end)

def get_data(start, end):
    try:
        url = generate_url(start, end)
        print("URL API obtenida: ", url)
        
        response = requests.get(url)
        data = response.json()
        
        # print(f"DATA OBTENIDA: , {data}")
        return data
    except Exception as e:
        print(f"ERROR al obtener los datos, {e}")
        return e
