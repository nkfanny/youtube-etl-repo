from flask import Flask, jsonify
import os
import json

app = Flask(__name__)

@app.route('/')
def hello():
    return {
        'status': 'success',
        'message': 'YouTube ETL Service is running!',
        'service': 'youtube-etl',
        'version': '5.1',
        'endpoints': ['/etl-debug', '/test-imports']
    }

@app.route('/test-imports')
def test_imports():
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        import gspread
        
        return {
            'status': 'success',
            'message': 'All imports successful',
            'imports': ['google.oauth2', 'googleapiclient', 'gspread']
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'error_type': type(e).__name__
        }

@app.route('/etl-debug')
def etl_debug():
    steps = []
    try:
        steps.append("1. Starting ETL debug...")
        
        # Étape 1: Imports
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        import gspread
        from datetime import datetime, timedelta
        steps.append("2. Imports successful")
        
        # Étape 2: Credentials
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
        if not creds_json:
            raise Exception('No credentials found')
        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=[
            'https://www.googleapis.com/auth/youtube.readonly',
            'https://www.googleapis.com/auth/yt-analytics.readonly',
            'https://www.googleapis.com/auth/spreadsheets'
        ])
        steps.append("3. Credentials loaded")
