from flask import Flask, jsonify
import os
import json
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread

app = Flask(__name__)

# Configuration
SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

@app.route('/')
def hello():
    return {
        'status': 'success',
        'message': 'YouTube ETL Service is running!',
        'service': 'youtube-etl',
        'version': '3.0',
        'endpoints': ['/etl', '/test', '/credentials-test']
    }

@app.route('/credentials-test')
def test_credentials():
    try:
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
        if not creds_json:
            return {'status': 'error', 'message': 'No credentials found'}
        
        creds_dict = json.loads(creds_json)
        return {
            'status': 'success',
            'message': 'Credentials loaded successfully',
            'project_id': creds_dict.get('project_id', 'Unknown')
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

@app.route('/etl')
def run_etl():
    try:
        # Test des credentials
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
        if not creds_json:
            return {'status': 'error', 'message': 'No credentials configured'}, 500
        
        # Calculer les dates
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        return {
            'status': 'success',
            'message': 'ETL ready to execute',
            'period': f'{start_date} to {end_date}',
            'timestamp': datetime.now().isoformat(),
            'next_step': 'YouTube API integration'
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
