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
        'version': '5.0',
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
        
        # Étape 3: YouTube Service
        youtube = build('youtube', 'v3', credentials=credentials)
        steps.append("4. YouTube service created")
        
        # Étape 4: Test Channel
        response = youtube.channels().list(part='id,snippet', mine=True).execute()
        if not response.get('items'):
            raise Exception('No YouTube channel found')
        
        channel = response['items'][0]
        channel_id = channel['id']
        channel_title = channel['snippet']['title']
        steps.append(f"5. Channel found: {channel_title} (ID: {channel_id})")
        
        # Étape 5: Analytics Service
        analytics = build('youtubeAnalytics', 'v2', credentials=credentials)
        steps.append("6. Analytics service created")
        
        # Étape 6: Test Analytics
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
        steps.append(f"7. Analytics data: {len(analytics_rows)} rows for {start_date} to {end_date}")
        
        # Étape 7: Google Sheets
        gc = gspread.authorize(credentials)
        steps.append("8. Google Sheets authorized")
        
        # Test création spreadsheet
        spreadsheet_name = f"YouTube ETL Test {datetime.now().strftime('%Y%m%d_%H%M')}"
        spreadsheet = gc.create(spreadsheet_name)
        steps.append(f"9. Test spreadsheet created: {spreadsheet.url}")
        
        return {
            'status': 'success',
            'message': 'ETL debug completed successfully!',
            'steps': steps,
            'data': {
                'channel_title': channel_title,
                'channel_id': channel_id,
                'analytics_rows': len(analytics_rows),
                'spreadsheet_url': spreadsheet.url,
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
