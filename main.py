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
        'version': '5.3',
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
        
        # Imports
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        import gspread
        from datetime import datetime, timedelta
        steps.append("2. Imports successful")
        
        # Credentials
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
        
        # YouTube Service
        youtube = build('youtube', 'v3', credentials=credentials)
        steps.append("4. YouTube service created")
        
        # Test Channel - deux méthodes
        response = youtube.channels().list(part='id,snippet', mine=True).execute()
        if response.get('items'):
            steps.append("5. Channel found with mine=True")
        else:
            steps.append("5a. No channel with mine=True, trying direct ID...")
            response = youtube.channels().list(part='id,snippet', id='UCS1m_ZhEAbQKfvIdAwoax2A').execute()
            if not response.get('items'):
                raise Exception('Channel not found with either method')
            steps.append("5b. Channel found with direct ID")
        
        channel = response['items'][0]
        channel_id = channel['id']
        channel_title = channel['snippet']['title']
        steps.append(f"6. Channel: {channel_title} (ID: {channel_id})")
        
        # Test Analytics
        analytics = build('youtubeAnalytics', 'v2', credentials=credentials)
        steps.append("7. Analytics service created")
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        analytics_response = analytics.reports().query(
            ids=f'channel=={channel_id}',
            startDate=start_date.strftime('%Y-%m-%d'),
            endDate=end_date.strftime('%Y-%m-%d'),
            metrics='views',
            maxResults=5
        ).execute()
        
        analytics_rows = analytics_response.get('rows', [])
        steps.append(f"8. Analytics data: {len(analytics_rows)} rows for {start_date} to {end_date}")
        
        return {
            'status': 'success',
            'message': 'ETL with Analytics success!',
            'steps': steps,
            'data': {
                'channel_title': channel_title,
                'channel_id': channel_id,
                'analytics_rows': len(analytics_rows),
                'period': f'{start_date} to {end_date}'
            }
        }
        
    except Exception as e:
        steps.append(f"ERROR: {str(e)}")
        return {
            'status': 'error',
            'message': str(e),
            'steps': steps,
            'error_type': type(e).__name__
        }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
