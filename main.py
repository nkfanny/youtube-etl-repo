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

SPREADSHEET_NAME = "YouTube ETL Data"

def get_credentials():
    """Récupère les credentials depuis la variable d'environnement"""
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise Exception('No credentials found in environment')
    
    creds_dict = json.loads(creds_json)
    return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

def get_youtube_services():
    """Initialise les services YouTube"""
    credentials = get_credentials()
    youtube = build('youtube', 'v3', credentials=credentials)
    analytics = build('youtubeAnalytics', 'v2', credentials=credentials)
    return youtube, analytics

def get_channel_id(youtube):
    """Récupère l'ID de la chaîne"""
    response = youtube.channels().list(part='id', mine=True).execute()
    if not response['items']:
        raise Exception("Aucune chaîne YouTube trouvée")
    return response['items'][0]['id']

def get_analytics_data(analytics, channel_id, start_date, end_date):
    """Récupère les données YouTube Analytics"""
    try:
        response = analytics.reports().query(
            ids=f'channel=={channel_id}',
            startDate=start_date,
            endDate=end_date,
            metrics='views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost',
            dimensions='video,day',
            maxResults=200,
            sort='day'
        ).execute()
        
        return response.get('rows', [])
    except Exception as e:
        raise Exception(f'Erreur Analytics API: {str(e)}')

def get_video_metadata(youtube, video_ids):
    """Récupère les métadonnées des vidéos"""
    if not video_ids:
        return []
    
    metadata = []
    
    # Traiter par batches de 50
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        
        response = youtube.videos().list(
            part='snippet,contentDetails,statistics,status',
            id=','.join(batch)
        ).execute()
        
        for item in response['items']:
            duration_sec = parse_duration(item['contentDetails']['duration'])
            
            metadata.append({
                'videoId': item['id'],
                'title': item['snippet']['title'][:255],
                'publishedAt': item['snippet']['publishedAt'],
                'durationSec': duration_sec,
                'isShort': 1 if duration_sec <= 60 else 0,
                'status': item['status']['privacyStatus']
            })
    
    return metadata

def parse_duration(duration_iso):
    """Convertit PT#H#M#S en secondes"""
    import re
    if not duration_iso or duration_iso == 'PT0S':
        return 0
    
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?'
    match = re.match(pattern, duration_iso)
    
    if not match:
        return 0
    
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = float(match.group(3) or 0)
    
    return int(hours * 3600 + minutes * 60 + seconds)

def save_to_sheets(analytics_data, metadata):
    """Sauvegarde dans Google Sheets"""
    credentials = get_credentials()
    gc = gspread.authorize(credentials)
    
    # Créer ou ouvrir le spreadsheet
    try:
        spreadsheet = gc.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        spreadsheet = gc.create(SPREADSHEET_NAME)
    
    # Sauvegarder analytics
    try:
        worksheet = spreadsheet.worksheet("yt_video_daily")
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="yt_video_daily", rows=1000, cols=10)
    
    if analytics_data:
        headers = ['day', 'videoId', 'views', 'estimatedMinutesWatched', 'averageViewDuration', 
                  'averageViewPercentage', 'subscribersGained', 'subscribersLost']
        all_data = [headers] + analytics_data
        worksheet.update('A1', all_data)
    
    # Sauvegarder métadonnées
    if metadata:
        try:
            meta_worksheet = spreadsheet.worksheet("yt_video_meta")
            meta_worksheet.clear()
        except gspread.WorksheetNotFound:
            meta_worksheet = spreadsheet.add_worksheet(title="yt_video_meta", rows=1000, cols=10)
        
        meta_headers = ['videoId', 'title', 'publishedAt', 'durationSec', 'isShort', 'status']
        meta_data = [meta_headers]
        for item in metadata:
            meta_data.append([
                item['videoId'], item['title'], item['publishedAt'],
                item['durationSec'], item['isShort'], item['status']
            ])
        meta_worksheet.update('A1', meta_data)
    
    return spreadsheet.url

@app.route('/')
def hello():
    return {
        'status': 'success',
        'message': 'YouTube ETL Service is running!',
        'service': 'youtube-etl',
        'version': '4.0',
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
        # Initialiser les services
        youtube, analytics = get_youtube_services()
        
        # Obtenir l'ID de chaîne
        channel_id = get_channel_id(youtube)
        
        # Calculer la fenêtre de dates (7 derniers jours)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        # Récupérer les données analytics
        analytics_data = get_analytics_data(
            analytics, channel_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # Récupérer les métadonnées des vidéos
        video_ids = list(set(row[0] for row in analytics_data if row))
        metadata = get_video_metadata(youtube, video_ids)
        
        # Sauvegarder dans Google Sheets
        sheet_url = save_to_sheets(analytics_data, metadata)
        
        return {
            'status': 'success',
            'message': 'ETL executed successfully',
            'period': f'{start_date} to {end_date}',
            'analytics_rows': len(analytics_data),
            'metadata_count': len(metadata),
            'sheet_url': sheet_url,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            'status': 'error', 
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }, 500

@app.route('/test')
def test():
    return {
        'status': 'success',
        'message': 'All systems operational',
        'timestamp': datetime.now().isoformat()
    }
    @app.route('/debug')
def debug_youtube():
    try:
        # Test étape par étape
        youtube, analytics = get_youtube_services()
        
        # Test 1: Lister les chaînes
        response = youtube.channels().list(part='id,snippet', mine=True).execute()
        
        return {
            'status': 'success',
            'message': 'Debug info',
            'channels_found': len(response.get('items', [])),
            'channels': response.get('items', []),
            'full_response': response
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'error_type': type(e).__name__
        }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
