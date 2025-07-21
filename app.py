from flask import Flask, jsonify
import psycopg2
import pandas as pd

app = Flask(__name__)

@app.route('/datos')
def obtener_datos():
    conexion = psycopg2.connect(
        host="200.10.16.5",
        port=8069,
        database="bd_openfruit_dev",
        user="analiticos",
        password="1n1l-LTS-5fr0"
    )

    consulta = "SELECT * FROM analytics_v2 LIMIT 100;"
    df = pd.read_sql(consulta, conexion)

    # Convertir fechas a string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')

    return jsonify(df.to_dict(orient="records"))
