import json
import requests
import pandas as pd

SERVICE_URL = 'http://localhost:5001'


def calc_clusters():
    df = pd.read_csv('graph_data.csv', sep=';')
    request_dct = {'data': df.to_dict()}

    try:
        responce = requests.get(SERVICE_URL, data=json.dumps(request_dct), headers={'Content-type': "application/json"},
                                verify=False)
        calc_result = json.loads(responce.content)

        result_df = pd.DataFrame.from_dict(calc_result['data'])

        writer = pd.ExcelWriter('./app_results/result.xlsx')
        result_df.to_excel(writer, sheet_name='res', index=False)
        writer.close()
    except Exception as err:
        print(err)


calc_clusters()
