try:
    import unosof.get_venture_json_2.data_base as data_base
    import requests
except Exception as e:
    print(f"ERROR al importar las librerias en main.py, {e}")

def generate_url(start, end):
    base_url = data_base.get_url_getVen2JSON()
    apikey = data_base.get_api_key_getCVen2JSON()
    return base_url.format(date_start=start, date_end=end, apy_key=apikey)

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

