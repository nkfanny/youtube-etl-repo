from flask import Flask, jsonify
import os
from datetime import datetime, timedelta
import json

app = Flask(__name__)

@app.route('/')
def hello():
    return {
        'status': 'success',
        'message': 'YouTube ETL Service is running!',
        'service': 'youtube-etl',
        'version': '2.0',
        'endpoints': ['/etl', '/test']
    }

@app.route('/etl')
def run_etl():
    try:
        # Calculer les dates
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        return {
            'status': 'success',
            'message': 'ETL executed successfully',
            'period': f'{start_date} to {end_date}',
            'timestamp': datetime.now().isoformat(),
            'note': 'YouTube APIs will be configured next'
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/test')
def test():
    return {
        'status': 'success',
        'message': 'All systems operational',
        'timestamp': datetime.now().isoformat()
    }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
