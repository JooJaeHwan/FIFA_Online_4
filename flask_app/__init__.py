from flask import Flask, render_template, request
import requests
from boto3 import client
import pandas as pd
import io

app = Flask(__name__)

def s3_conn():
    s3_client = client(service_name="s3",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    return s3_client

@app.route('/')

def index():
    positions = ["GK", "RWB", "RB", "RCB", "CB", "SW", "LCB", "LB", "LWB", "LDM", "CDM", "RDM", "LCM", "LM", "CM", 
                 "RM", "RCM", "LAM", "CAM", "RAM", "LW", "LF", "CF", "RF", "RW", "LS", "ST", "RS"]
    s3_client = s3_conn()
    dir_info = s3_client.list_objects(Bucket='fifaonline4', Prefix='data/position/SW')
    dates = [i['Key'].split('/')[-1].split('.')[0] for i in dir_info['Contents']]
    return render_template('index.html', positions = positions, dates=dates), 200

@app.route('/result', methods=["GET","POST"])
def result():
    position = request.form.get('position')
    date = request.form.get('date')
    s3_client = s3_conn()
    obj = s3_client.get_object(Bucket="fifaonline4", Key=f"data/position/{position}/{date}.parquet")
    players = pd.read_parquet(io.BytesIO(obj["Body"].read())).head(5)
    return render_template('result.html', position=position, date=date, players=players.itertuples()), 200

@app.route('/info')
def info():
    return render_template('info.html'), 200

if __name__ == '__main__':
    app.run('0.0.0.0', port=9999, debug=True)