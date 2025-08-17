from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def hello():
    return {
        'status': 'success',
        'message': 'YouTube ETL Service is running!',
        'service': 'youtube-etl',
        'version': '1.0'
    }

@app.route('/etl')
def run_etl():
    return {
        'status': 'success', 
        'message': 'ETL will run here',
        'data': 'Coming soon...'
    }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
