import pandas as pd
import psycopg2
import os
import datetime
from flask import Flask, render_template, request, send_file

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        get_data()
        return "Data retrieval and file generation completed."
    return render_template('index.html')

def get_data():
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday1 = datetime.datetime.now()
    yesterday1 = "'" + yesterday1.strftime('%Y-%m-%d') + "'"
    yesterday = yesterday.strftime('%m')
    yesterday = str(yesterday)
    yesterday = "'" + yesterday + "'"

    def get_data_sub(store_num):
        if store_num == '34':
            conn = psycopg2.connect(host="10.34.1.220",
                                    database="CARREFOURS34_LIVE",
                                    user="readonly_c34",
                                    password="readonly_c34_password")
        elif store_num == '13':
            conn = psycopg2.connect(host="10.13.1.220",
                                    database="CARREFOURS13_LIVE",
                                    user="postgres",
                                    password="postgres")

        query = '''
        SELECT order_id, create_date, price_subtotal_incl, price_subtotal, product_id
        FROM public.pos_order_line
        WHERE EXTRACT(MONTH FROM CAST(create_date AS TIMESTAMP WITH TIME ZONE)) = ''' + yesterday + '''
        AND cast(create_date as date) < ''' + yesterday1
        print(query)

        query_product = '''
        SELECT id AS product_id, full_internal_code, Barcode
        FROM public.product_product'''

        cwd = os.getcwd()
        dir = os.path.join(cwd, 'Data_files1')

        df = pd.read_sql_query(query, conn)
        df_product = pd.read_sql_query(query_product, conn)

        df.to_excel(os.path.join(dir, 'main_data' + store_num + '.xlsx'), index=False)
        df_product.to_excel(os.path.join(dir, 'product_data' + store_num + '.xlsx'), index=False)

        df = pd.read_excel(os.path.join(dir, 'main_data' + store_num + '.xlsx'))
        df_product = pd.read_excel(os.path.join(dir, 'product_data' + store_num + '.xlsx'))
        df_structure = pd.read_excel(os.path.join(dir, 'structure.xlsx'), dtype=str)

        df['Store'] = 'S' + store_num
        df['order_id'] = df['Store'] + df['order_id'].astype(str)

        df = df.merge(df_product, on='product_id', how='left')
        df['Department Code'] = df['full_internal_code'].str[:2]
        df['Department Code'] = df['Department Code'].fillna('14')
        df = df.merge(df_structure, on='Department Code', how='left')

        df.to_excel(os.path.join(dir, 'data_frame' + store_num + '.xlsx'), index=False)

    get_data_sub('34')
    get_data_sub('13')

    cwd = os.getcwd()
    directory = os.path.join(cwd, 'Data_files1')

    try:
        os.remove(os.path.join(directory, 'data_frame.xlsx'))
    except:
        pass

    file_names = [f for f in os.listdir(directory) if f.startswith('data_frame')]

    combined_data = pd.DataFrame()

    for file_name in file_names:
        data = pd.read_excel(os.path.join(directory, file_name))
        combined_data = combined_data.append(data, ignore_index=True)

    combined_data.to_excel(os.path.join(directory, 'data_frame.xlsx'), index=False)

@app.route('/download', methods=['GET'])
def download():
    cwd = os.getcwd()
    directory = os.path.join(cwd, 'Data_files1')
    file_path = os.path.join(directory, 'data_frame.xlsx')
    return send_file(file_path, as_attachment=True, attachment_filename='data_frame.xlsx')

if __name__ == '__main__':
    app.run(debug=True)
