import requests
import json

HOST = "http://127.0.0.1:6666"
URI_BLOB = "/blob/insert/hdfs"
URI_IMPALA = "/files/insert/hdfs"


# Parquetに変換してHDFSにput
def put_to_hdfs(csv_path):
    files = {'file': open(csv_path, 'rb')}
    payload = {'dest_path': '/Users/uma6/IdeaProjects/test_api'}

    params = {"params": json.dumps(payload)}
    r = requests.post(HOST + URI_BLOB, files=files, data=params)
    print("response: ", r.text)
    print("status: ", r.status_code)


def put_to_impala(csv_path):
    files = {'file': open(csv_path, 'rb')}
    payload = {'destination': {'dbname': 'test_db', 'tablename': 'test_table'},
               'to_format': {'file_type': 'csv', 'compression_type': 'snappy'},
               'schema': {'cols_type': {'col_a': 'INT', 'col_b': 'STRING', 'col_c': 'DOUBLE'}}
               }

    params = {"params": json.dumps(payload)}
    r = requests.post(HOST + URI_IMPALA, files=files, data=params)
    print("response: ", r.text)
    print("status: ", r.status_code)


if __name__ == '__main__':
    # put_to_hdfs("./test.csv")
    put_to_impala("./test.csv")
