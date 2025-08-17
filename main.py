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
        'version': '4.2'
    }

@app.route('/test-imports')
def test_imports():
    try:
        # Test des imports un par un
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

@app.route('/etl')
def run_etl():
    return {
        'status': 'success',
        'message': 'ETL will be implemented step by step'
    }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
