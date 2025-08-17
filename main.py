from flask import Flask, jsonify
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.oauth2.credentials import Credentials as UserCredentials
import gspread
from datetime import datetime, timedelta
import os
import json
import re

app = Flask(__name__)

# Configuration
SPREADSHEET_NAME = "YouTube_Intelligence"
SPREADSHEET_ID = "1bvob7xaoO5X-RHAhl34ZVcX2IH2qYufVy9aKaUdNXxU"
CHANNEL_ID = "UCS1m_ZhEAbQKfvIdAwoax2A"

# ==================== CLASSIFICATION CONTENT PILLARS ====================
CONTENT_PILLARS = {
    "Agilité": ["scrum", "kanban", "sprint", "agile", "agilité", "methode", "framework"],
    "Product Management": ["product", "roadmap", "feature", "backlog", "owner", "business", "analyst"],
    "Leadership": ["manager", "équipe", "leadership", "management", "chef", "team", "leader"],
    "Outils": ["outil", "logiciel", "app", "plateforme", "software", "tool"],
    "Formation": ["formation", "cours", "apprendre", "certification", "tutorial", "guide"]
}

# ==================== STRUCTURES DES SHEETS ====================
SHEET_HEADERS = {
    'Raw_Daily_Data': [
        'date', 'total_views', 'total_watch_time', 'subscribers_gained', 
        'subscribers_lost', 'total_comments', 'total_likes', 'avg_view_duration'
    ],
    'Video_Performance_Data': [
        'video_id', 'views_7d', 'watch_time_7d', 'avg_view_duration', 
        'avg_view_percentage', 'extraction_date'
    ],
    'Video_Master': [
        'video_id', 'title', 'publish_date', 'duration_seconds', 'category', 
        'thumbnail_url', 'tags', 'description_snippet', 
        'views_7d', 'watch_time_7d', 'avg_view_duration', 'retention_rate',
        'performance_tier', 'content_pillar', 'funnel_stage', 'last_updated'
    ],
    'Content_Taxonomy': [
        'video_id', 'primary_topic', 'secondary_topics', 'target_audience', 
        'content_format', 'hook_type', 'cta_presence', 'thumbnail_style',
        'publication_day', 'optimal_posting_time', 'strategic_intent'
    ],
    'Weekly_Aggregates': [
        'week_start_date', 'total_views', 'total_watch_time', 'avg_view_duration', 
        'new_subscribers_net', 'engagement_rate', 'top_traffic_source', 
        'top_performing_video', 'top_geo', 'mobile_vs_desktop_ratio', 
        'retention_benchmark', 'week_over_week_growth'
    ]
}

@app.route('/')
def hello():
    return jsonify({
        'status': 'success',
        'message': 'YouTube ETL Service - Phase 3 Architecture Hybride!',
        'service': 'youtube-etl',
        'version': '6.0-PHASE3',
        'authentication': 'YouTube OAuth + Sheets Service Account',
        'phase3_features': [
            'Video_Master enrichi',
            'Content_Taxonomy automatique',
            'Performance Tier scoring',
            'Content Pillars classification'
        ],
        'endpoints': ['/etl', '/etl-phase3', '/enrich-metadata', '/init-taxonomy', '/test', '/test-youtube', '/test-sheets']
    })

@app.route('/test')
def test_basic():
    """Test basique du service"""
    try:
        print("=== 🧪 TEST BASIQUE DU SERVICE PHASE 3 ===")
        print(f"🕐 {datetime.now().isoformat()}")
        
        # Variables d'environnement
        env_status = {}
        for var in ['YOUTUBE_TOKEN_JSON', 'GOOGLE_SA_JSON', 'PORT', 'K_SERVICE']:
            value = os.environ.get(var)
            env_status[var] = 'DÉFINIE' if value else 'MANQUANTE'
            print(f"   📋 {var}: {env_status[var]}")
        
        result = {
            'status': 'success',
            'message': 'Service Phase 3 opérationnel',
            'timestamp': datetime.now().isoformat(),
            'environment': env_status,
            'channel_id': CHANNEL_ID,
            'spreadsheet_id': SPREADSHEET_ID,
            'phase3_ready': {
                'youtube_data_api': env_status.get('YOUTUBE_TOKEN_JSON') == 'DÉFINIE',
                'youtube_analytics_api': env_status.get('YOUTUBE_TOKEN_JSON') == 'DÉFINIE',
                'sheets_api': env_status.get('GOOGLE_SA_JSON') == 'DÉFINIE',
                'content_classification': True,
                'performance_scoring': True
            }
        }
        
        print("✅ Test Phase 3 réussi")
        return jsonify(result)
        
    except Exception as e:
        print(f"❌ Erreur test basique: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/test-youtube')
def test_youtube():
    """Test spécifique YouTube Analytics + Data API"""
    try:
        print("=== 📺 TEST YOUTUBE APIS COMPLET ===")
        
        # Récupérer les credentials YouTube
        youtube_creds = get_youtube_credentials()
        if not youtube_creds:
            raise Exception("YouTube credentials non configurés")
        
        print("✅ Credentials YouTube récupérés")
        
        # Initialiser les services
        youtube_service, analytics_service = get_youtube_services(youtube_creds)
        print("✅ Services YouTube initialisés")
        
        # Test Analytics
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=3)
        
        print(f"🔍 Test Analytics période: {start_date} → {end_date}")
        
        analytics_response = analytics_service.reports().query(
            ids='channel==MINE',
            startDate=str(start_date),
            endDate=str(end_date),
            metrics='views,estimatedMinutesWatched',
            dimensions='day',
            sort='day'
        ).execute()
        
        analytics_rows = analytics_response.get('rows', [])
        print(f"✅ Analytics réponse: {len(analytics_rows)} lignes")
        
        # Test Data API - Métadonnées vidéos
        print(f"🔍 Test Data API - Métadonnées vidéos récentes...")
        
        data_response = youtube_service.search().list(
            part='id',
            channelId=CHANNEL_ID,
            type='video',
            order='date',
            maxResults=5
        ).execute()
        
        video_ids = [item['id']['videoId'] for item in data_response.get('items', [])]
        print(f"✅ Data API réponse: {len(video_ids)} vidéos trouvées")
        
        # Test métadonnées détaillées
        if video_ids:
            metadata_response = youtube_service.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(video_ids[:3])
            ).execute()
            
            metadata_items = metadata_response.get('items', [])
            print(f"✅ Métadonnées récupérées: {len(metadata_items)} vidéos")
        
        return jsonify({
            'status': 'success',
            'message': 'YouTube APIs (Analytics + Data) accessibles',
            'analytics_rows': len(analytics_rows),
            'data_api_videos': len(video_ids),
            'metadata_items': len(metadata_items) if video_ids else 0,
            'sample_video_ids': video_ids[:3],
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ Erreur YouTube: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'type': type(e).__name__,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/test-sheets')
def test_sheets():
    """Test spécifique Google Sheets"""
    try:
        print("=== 📊 TEST GOOGLE SHEETS ===")
        
        # Récupérer le client Sheets
        sheets_client = get_sheets_client()
        if not sheets_client:
            raise Exception("Sheets credentials non configurés")
        
        print("✅ Client Sheets récupéré")
        
        # Test d'accès au spreadsheet
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        print(f"✅ Spreadsheet ouvert: {spreadsheet.title}")
        
        # Lister les worksheets
        worksheets = spreadsheet.worksheets()
        worksheet_names = [ws.title for ws in worksheets]
        print(f"✅ Worksheets trouvés: {worksheet_names}")
        
        return jsonify({
            'status': 'success',
            'message': 'Google Sheets accessible',
            'spreadsheet_title': spreadsheet.title,
            'worksheets': worksheet_names,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ Erreur Sheets: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'type': type(e).__name__,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl')
def run_etl():
    """ETL YouTube Phase 1 & 2 (compatibilité)"""
    try:
        print("=== 🚀 DÉBUT ETL YOUTUBE PHASE 1 + 2 ===")
        start_time = datetime.now()
        
        # Initialisation
        youtube_creds = get_youtube_credentials()
        sheets_client = get_sheets_client()
        
        if not youtube_creds or not sheets_client:
            raise Exception("Credentials manquants")
        
        youtube_service, analytics_service = get_youtube_services(youtube_creds)
        
        # Calculer les dates
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        # Phase 1 & 2
        daily_rows = get_daily_channel_data(analytics_service, str(start_date), str(end_date))
        video_rows = get_video_performance_data(analytics_service, str(start_date), str(end_date))
        
        # Sauvegarde
        if daily_rows:
            save_daily_channel_data(sheets_client, daily_rows)
        
        if video_rows:
            save_video_performance_data(sheets_client, video_rows)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return jsonify({
            'status': 'success',
            'message': 'ETL Phase 1+2 exécuté avec succès',
            'execution_details': {
                'duration_seconds': round(execution_time, 2),
                'phase1_daily_rows': len(daily_rows),
                'phase2_video_rows': len(video_rows)
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ ERREUR ETL: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl-phase3')
def run_etl_phase3():
    """ETL YouTube PHASE 3 COMPLET - Architecture Hybride"""
    try:
        print("=== 🚀 DÉBUT ETL YOUTUBE PHASE 3 COMPLET ===")
        start_time = datetime.now()
        print(f"🕐 Début: {start_time.isoformat()}")
        
        # 1. Initialiser les credentials
        print("1️⃣ Initialisation des authentifications...")
        youtube_creds = get_youtube_credentials()
        sheets_client = get_sheets_client()
        
        if not youtube_creds:
            raise Exception("YouTube credentials manquants")
        if not sheets_client:
            raise Exception("Sheets credentials manquants")
        
        print("✅ Double authentification prête")
        
        # 2. Initialiser les services YouTube
        print("2️⃣ Initialisation services YouTube...")
        youtube_service, analytics_service = get_youtube_services(youtube_creds)
        print("✅ Services YouTube prêts")
        
        # 3. Calculer les dates
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        print(f"📅 Période ETL: {start_date} → {end_date}")
        
        # 4. PHASE 1 : Données quotidiennes par chaîne
        print("3️⃣ PHASE 1 : Récupération Daily Channel Data...")
        daily_rows = get_daily_channel_data(analytics_service, str(start_date), str(end_date))
        print(f"✅ Phase 1: {len(daily_rows)} lignes récupérées")
        
        # 5. PHASE 2 : Données par vidéo (7 jours)
        print("4️⃣ PHASE 2 : Récupération Video Performance Data...")
        video_rows = get_video_performance_data(analytics_service, str(start_date), str(end_date))
        print(f"✅ Phase 2: {len(video_rows)} lignes récupérées")
        
        # 6. PHASE 3 : Enrichissement métadonnées
        print("5️⃣ PHASE 3 : Enrichissement métadonnées...")
        video_ids = [row[0] for row in video_rows] if video_rows else []
        metadata_enriched = get_video_metadata(youtube_service, video_ids)
        print(f"✅ Phase 3: {len(metadata_enriched)} vidéos enrichies")
        
        # 7. Initialiser les sheets si nécessaire
        print("6️⃣ Initialisation des structures Sheets...")
        init_all_sheets(sheets_client)
        print("✅ Structures Sheets initialisées")
        
        # 8. Sauvegarder Phase 1
        print("7️⃣ Sauvegarde Daily Channel Data...")
        if daily_rows:
            save_daily_channel_data(sheets_client, daily_rows)
            print("✅ Phase 1 sauvegardée")
        
        # 9. Sauvegarder Phase 2
        print("8️⃣ Sauvegarde Video Performance Data...")
        if video_rows:
            save_video_performance_data(sheets_client, video_rows)
            print("✅ Phase 2 sauvegardée")
        
        # 10. Sauvegarder Phase 3 - Video Master
        print("9️⃣ Création Video Master enrichi...")
        if metadata_enriched:
            video_master_data = enrich_video_master(video_rows, metadata_enriched)
            save_video_master(sheets_client, video_master_data)
            print("✅ Video Master enrichi sauvegardé")
        
        # 11. Initialiser Content Taxonomy
        print("🔟 Initialisation Content Taxonomy...")
        if metadata_enriched:
            taxonomy_data = create_content_taxonomy(metadata_enriched)
            save_content_taxonomy(sheets_client, taxonomy_data)
            print("✅ Content Taxonomy initialisé")
        
        # 12. Résultat final
        execution_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ ETL PHASE 3 COMPLET TERMINÉ en {execution_time:.2f}s")
        
        return jsonify({
            'status': 'success',
            'message': 'ETL Phase 3 Complet exécuté avec succès',
            'execution_details': {
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': round(execution_time, 2),
                'period': {
                    'start': str(start_date),
                    'end': str(end_date)
                },
                'phase1_daily_rows': len(daily_rows),
                'phase2_video_rows': len(video_rows),
                'phase3_enriched_videos': len(metadata_enriched),
                'video_master_created': len(metadata_enriched) > 0,
                'content_taxonomy_initialized': len(metadata_enriched) > 0,
                'channel_id': CHANNEL_ID,
                'spreadsheet_id': SPREADSHEET_ID
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ ERREUR ETL PHASE 3: {e}")
        return jsonify({
            'status': 'error',
            'error_details': {
                'type': type(e).__name__,
                'message': str(e),
                'function': 'run_etl_phase3'
            },
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/enrich-metadata')
def enrich_metadata_only():
    """Enrichissement métadonnées seulement (pour optimiser quotas)"""
    try:
        print("=== 📋 ENRICHISSEMENT MÉTADONNÉES SEULEMENT ===")
        start_time = datetime.now()
        
        # Initialisation
        youtube_creds = get_youtube_credentials()
        sheets_client = get_sheets_client()
        
        if not youtube_creds or not sheets_client:
            raise Exception("Credentials manquants")
        
        youtube_service, analytics_service = get_youtube_services(youtube_creds)
        
        # Récupérer les dernières vidéos
        print("🔍 Récupération des vidéos récentes...")
        recent_videos = get_recent_video_ids(youtube_service, max_results=20)
        print(f"✅ {len(recent_videos)} vidéos récentes trouvées")
        
        # Enrichir métadonnées
        metadata_enriched = get_video_metadata(youtube_service, recent_videos)
        print(f"✅ {len(metadata_enriched)} vidéos enrichies")
        
        # Initialiser sheets si nécessaire
        init_all_sheets(sheets_client)
        
        # Sauvegarder Video Master
        if metadata_enriched:
            # Récupérer performance data (fenêtre 7j pour les vidéos récentes)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=6)
            video_rows = get_video_performance_data(analytics_service, str(start_date), str(end_date))
            
            video_master_data = enrich_video_master(video_rows, metadata_enriched)
            save_video_master(sheets_client, video_master_data)
            
            # Content Taxonomy
            taxonomy_data = create_content_taxonomy(metadata_enriched)
            save_content_taxonomy(sheets_client, taxonomy_data)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return jsonify({
            'status': 'success',
            'message': 'Enrichissement métadonnées terminé',
            'execution_details': {
                'duration_seconds': round(execution_time, 2),
                'videos_processed': len(metadata_enriched),
                'video_master_updated': len(metadata_enriched) > 0,
                'taxonomy_updated': len(metadata_enriched) > 0
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ ERREUR ENRICHISSEMENT: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/init-taxonomy')
def init_taxonomy_only():
    """Initialise seulement la structure Content Taxonomy"""
    try:
        print("=== 📋 INITIALISATION CONTENT TAXONOMY ===")
        
        sheets_client = get_sheets_client()
        if not sheets_client:
            raise Exception("Sheets credentials manquants")
        
        # Initialiser la structure
        init_content_taxonomy_sheet(sheets_client)
        
        return jsonify({
            'status': 'success',
            'message': 'Content Taxonomy initialisé',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ ERREUR INIT TAXONOMY: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# ==================== AUTHENTIFICATION ====================

def get_youtube_credentials():
    """Récupère les credentials YouTube OAuth (Brand Account)"""
    try:
        token_json = os.environ.get('YOUTUBE_TOKEN_JSON')
        if not token_json:
            print("⚠️ YOUTUBE_TOKEN_JSON non définie")
            return None
        
        token_data = json.loads(token_json)
        credentials = UserCredentials.from_authorized_user_info(
            token_data,
            scopes=[
                'https://www.googleapis.com/auth/youtube.readonly',
                'https://www.googleapis.com/auth/yt-analytics.readonly'
            ]
        )
        print("✅ Credentials YouTube chargés")
        return credentials
        
    except Exception as e:
        print(f"❌ Erreur credentials YouTube: {e}")
        return None

def get_sheets_client():
    """Récupère le client Google Sheets (Service Account)"""
    try:
        sa_json = os.environ.get('GOOGLE_SA_JSON')
        if not sa_json:
            print("❌ GOOGLE_SA_JSON non définie")
            return None
        
        sa_data = json.loads(sa_json)
        credentials = ServiceAccountCredentials.from_service_account_info(
            sa_data,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        
        client = gspread.authorize(credentials)
        return client
        
    except Exception as e:
        print(f"❌ Erreur Sheets client: {e}")
        return None

def get_youtube_services(credentials):
    """Initialise les services YouTube SANS CACHE"""
    youtube_service = build(
        'youtube', 'v3',
        credentials=credentials,
        cache_discovery=False
    )
    
    analytics_service = build(
        'youtubeAnalytics', 'v2',
        credentials=credentials,
        cache_discovery=False
    )
    
    return youtube_service, analytics_service

# ==================== RÉCUPÉRATION DONNÉES ====================

def get_daily_channel_data(analytics_service, start_date, end_date):
    """PHASE 1: Récupère les données quotidiennes par chaîne"""
    try:
        response = analytics_service.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='views,estimatedMinutesWatched,subscribersGained,subscribersLost,comments,likes,averageViewDuration',
            dimensions='day',
            sort='day'
        ).execute()
        
        return response.get('rows', [])
        
    except Exception as e:
        print(f"❌ Erreur Daily Channel Data: {e}")
        return []

def get_video_performance_data(analytics_service, start_date, end_date):
    """PHASE 2: Récupère les données par vidéo (fenêtre 7 jours)"""
    try:
        response = analytics_service.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage',
            dimensions='video',
            maxResults=50,
            sort='-views'
        ).execute()
        
        return response.get('rows', [])
        
    except Exception as e:
        print(f"❌ Erreur Video Performance Data: {e}")
        return []

def get_recent_video_ids(youtube_service, max_results=20):
    """Récupère les IDs des vidéos récentes de la chaîne"""
    try:
        search_response = youtube_service.search().list(
            part='id',
            channelId=CHANNEL_ID,
            type='video',
            order='date',
            maxResults=max_results
        ).execute()
        
        return [item['id']['videoId'] for item in search_response.get('items', [])]
        
    except Exception as e:
        print(f"❌ Erreur récupération video IDs: {e}")
        return []

def get_video_metadata(youtube_service, video_ids):
    """PHASE 3: Récupère les métadonnées des vidéos via YouTube Data API"""
    try:
        if not video_ids:
            return []
        
        print(f"📋 Récupération métadonnées pour {len(video_ids)} vidéos...")
        
        # Traiter par lots de 50 (limite API)
        all_metadata = []
        batch_size = 50
        
        for i in range(0, len(video_ids), batch_size):
            batch_ids = video_ids[i:i+batch_size]
            
            response = youtube_service.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(batch_ids)
            ).execute()
            
            for item in response.get('items', []):
                metadata = {
                    'video_id': item['id'],
                    'title': item['snippet'].get('title', ''),
                    'publish_date': item['snippet'].get('publishedAt', '')[:10],  # YYYY-MM-DD
                    'duration_seconds': parse_duration(item['contentDetails'].get('duration', 'PT0S')),
                    'category': item['snippet'].get('categoryId', ''),
                    'thumbnail_url': item['snippet'].get('thumbnails', {}).get('medium', {}).get('url', ''),
                    'tags': '|'.join(item['snippet'].get('tags', [])),
                    'description_snippet': item['snippet'].get('description', '')[:200] + '...' if item['snippet'].get('description', '') else '',
                    'view_count': int(item['statistics'].get('viewCount', 0)),
                    'like_count': int(item['statistics'].get('likeCount', 0)),
                    'comment_count': int(item['statistics'].get('commentCount', 0))
                }
                all_metadata.append(metadata)
        
        print(f"✅ Métadonnées récupérées: {len(all_metadata)} vidéos")
        return all_metadata
        
    except Exception as e:
        print(f"❌ Erreur métadonnées: {e}")
        return []

def parse_duration(duration_str):
    """Convertit la durée ISO 8601 (PT4M13S) en secondes"""
    try:
        # Regex pour parser PT4M13S ou PT1H2M3S
        import re
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        if not match:
            return 0
        
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        
        return hours * 3600 + minutes * 60 + seconds
        
    except:
        return 0

# ==================== ENRICHISSEMENT DONNÉES ====================

def enrich_video_master(video_performance_rows, metadata_list):
    """Fusionne performance + métadonnées pour créer Video Master"""
    try:
        print("🔄 Fusion performance + métadonnées...")
        
        # Créer dictionnaire performance par video_id
        performance_dict = {}
        for row in video_performance_rows:
            video_id = row[0]
            performance_dict[video_id] = {
                'views_7d': row[1] if len(row) > 1 else 0,
                'watch_time_7d': row[2] if len(row) > 2 else 0,
                'avg_view_duration': row[3] if len(row) > 3 else 0,
                'avg_view_percentage': row[4] if len(row) > 4 else 0
            }
        
        enriched_data = []
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        for metadata in metadata_list:
            video_id = metadata['video_id']
            performance = performance_dict.get(video_id, {
                'views_7d': 0,
                'watch_time_7d': 0,
                'avg_view_duration': 0,
                'avg_view_percentage': 0
            })
            
            # Calculer Performance Tier
            performance_tier = calculate_performance_tier(metadata, performance)
            
            # Classification Content Pillar
            content_pillar = classify_content_pillar(metadata['title'])
            
            # Calculer retention rate
            retention_rate = performance['avg_view_percentage']
            
            enriched_row = [
                video_id,
                metadata['title'],
                metadata['publish_date'],
                metadata['duration_seconds'],
                metadata['category'],
                metadata['thumbnail_url'],
                metadata['tags'],
                metadata['description_snippet'],
                performance['views_7d'],
                performance['watch_time_7d'],
                performance['avg_view_duration'],
                retention_rate,
                performance_tier,
                content_pillar,
                'À définir',  # funnel_stage (manuel)
                current_date   # last_updated
            ]
            
            enriched_data.append(enriched_row)
        
        print(f"✅ Video Master enrichi: {len(enriched_data)} lignes")
        return enriched_data
        
    except Exception as e:
        print(f"❌ Erreur enrichissement Video Master: {e}")
        return []

def calculate_performance_tier(metadata, performance):
    """Calcule le Performance Tier selon l'algorithme CMO"""
    try:
        # Récupérer les métriques de base
        views_7d = performance.get('views_7d', 0)
        watch_time_7d = performance.get('watch_time_7d', 0)
        retention_rate = performance.get('avg_view_percentage', 0)
        
        # Calculer les ratios (besoin de moyennes de la chaîne - pour l'instant utiliser des seuils fixes)
        # TODO: Calculer dynamiquement les moyennes de la chaîne
        channel_avg_views = 1000  # À calculer dynamiquement
        channel_avg_watch_time = 50  # À calculer dynamiquement
        channel_avg_retention = 45  # À calculer dynamiquement
        
        views_ratio = views_7d / max(channel_avg_views, 1)
        engagement_ratio = watch_time_7d / max(channel_avg_watch_time, 1)
        retention_ratio = retention_rate / max(channel_avg_retention, 1)
        
        # Formule CMO : score = (vues_ratio * 0.4) + (engagement_ratio * 0.3) + (retention_ratio * 0.3)
        score = (views_ratio * 0.4) + (engagement_ratio * 0.3) + (retention_ratio * 0.3)
        
        # Classification selon les seuils CMO
        if score >= 2.0:
            return "S-Tier"
        elif score >= 1.2:
            return "A-Tier"
        elif score >= 0.8:
            return "B-Tier"
        else:
            return "C-Tier"
            
    except Exception as e:
        print(f"⚠️ Erreur calcul Performance Tier: {e}")
        return "B-Tier"  # Valeur par défaut

def classify_content_pillar(title):
    """Classification automatique Content Pillar selon mots-clés CMO"""
    try:
        title_lower = title.lower()
        
        # Score par pillar
        pillar_scores = {}
        
        for pillar, keywords in CONTENT_PILLARS.items():
            score = 0
            for keyword in keywords:
                if keyword.lower() in title_lower:
                    score += 1
            pillar_scores[pillar] = score
        
        # Retourner le pillar avec le score le plus élevé
        if max(pillar_scores.values()) > 0:
            return max(pillar_scores, key=pillar_scores.get)
        else:
            return "Non classifié"
            
    except Exception as e:
        print(f"⚠️ Erreur classification Content Pillar: {e}")
        return "Non classifié"

def create_content_taxonomy(metadata_list):
    """Crée les données Content Taxonomy avec classification automatique"""
    try:
        print("🏷️ Création Content Taxonomy...")
        
        taxonomy_data = []
        
        for metadata in metadata_list:
            video_id = metadata['video_id']
            title = metadata['title']
            publish_date = metadata['publish_date']
            
            # Classification automatique
            primary_topic = classify_content_pillar(title)
            
            # Analyser les tags pour secondary_topics
            tags_list = metadata['tags'].split('|') if metadata['tags'] else []
            secondary_topics = '|'.join(tags_list[:3])  # Prendre les 3 premiers tags
            
            # Analyser la date de publication pour le jour
            try:
                pub_datetime = datetime.strptime(publish_date, '%Y-%m-%d')
                publication_day = pub_datetime.strftime('%A')  # Lundi, Mardi, etc.
            except:
                publication_day = 'Inconnu'
            
            # Détection automatique de certains éléments
            hook_type = detect_hook_type(title)
            cta_presence = detect_cta_presence(metadata['description_snippet'])
            
            taxonomy_row = [
                video_id,
                primary_topic,
                secondary_topics,
                'À définir',  # target_audience (manuel)
                detect_content_format(title),
                hook_type,
                cta_presence,
                'À analyser',  # thumbnail_style (manuel)
                publication_day,
                'À optimiser',  # optimal_posting_time (analyse future)
                'À définir'   # strategic_intent (manuel)
            ]
            
            taxonomy_data.append(taxonomy_row)
        
        print(f"✅ Content Taxonomy créé: {len(taxonomy_data)} lignes")
        return taxonomy_data
        
    except Exception as e:
        print(f"❌ Erreur création Content Taxonomy: {e}")
        return []

def detect_hook_type(title):
    """Détection automatique du type d'accroche"""
    title_lower = title.lower()
    
    if any(word in title_lower for word in ['comment', 'how to', 'guide', 'tutorial']):
        return 'How-to'
    elif any(word in title_lower for word in ['?', 'pourquoi', 'why', 'what', 'quoi']):
        return 'Question'
    elif any(word in title_lower for word in ['top', 'meilleur', 'best', 'différent']):
        return 'Liste/Comparaison'
    elif any(word in title_lower for word in ['secret', 'astuce', 'tip', 'hack']):
        return 'Secret/Astuce'
    else:
        return 'Informatif'

def detect_cta_presence(description):
    """Détection automatique de Call-to-Action"""
    if not description:
        return 'Non détecté'
    
    description_lower = description.lower()
    cta_indicators = [
        'abonne', 'subscribe', 'like', 'partage', 'share', 'commentaire', 'comment',
        'clique', 'click', 'inscris', 'rejoins', 'follow', 'télécharge', 'download'
    ]
    
    if any(indicator in description_lower for indicator in cta_indicators):
        return 'Présent'
    else:
        return 'Absent'

def detect_content_format(title):
    """Détection automatique du format de contenu"""
    title_lower = title.lower()
    
    if any(word in title_lower for word in ['vs', 'versus', 'comparaison']):
        return 'Comparaison'
    elif any(word in title_lower for word in ['tutorial', 'guide', 'pas à pas']):
        return 'Tutorial'
    elif any(word in title_lower for word in ['analyse', 'review', 'test']):
        return 'Analyse'
    elif any(word in title_lower for word in ['bases', 'introduction', 'débutant']):
        return 'Introduction'
    else:
        return 'Éducatif'

# ==================== INITIALISATION SHEETS ====================

def init_all_sheets(sheets_client):
    """Initialise toutes les structures de sheets Phase 3"""
    try:
        print("📊 Initialisation des structures Sheets...")
        
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        
        # Vérifier/créer chaque sheet nécessaire
        for sheet_name, headers in SHEET_HEADERS.items():
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"✅ Sheet '{sheet_name}' existe déjà")
                
                # Vérifier si les headers sont corrects
                existing_headers = worksheet.row_values(1)
                if existing_headers != headers:
                    print(f"🔄 Mise à jour headers pour '{sheet_name}'")
                    worksheet.clear()
                    worksheet.append_row(headers)
                
            except gspread.WorksheetNotFound:
                print(f"📋 Création du sheet '{sheet_name}'...")
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=1000,
                    cols=len(headers)
                )
                worksheet.append_row(headers)
                print(f"✅ Sheet '{sheet_name}' créé avec {len(headers)} colonnes")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur initialisation sheets: {e}")
        return False

def init_content_taxonomy_sheet(sheets_client):
    """Initialise spécifiquement le sheet Content Taxonomy"""
    try:
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        
        try:
            worksheet = spreadsheet.worksheet('Content_Taxonomy')
            print("✅ Content_Taxonomy existe déjà")
        except gspread.WorksheetNotFound:
            print("📋 Création Content_Taxonomy...")
            headers = SHEET_HEADERS['Content_Taxonomy']
            worksheet = spreadsheet.add_worksheet(
                title='Content_Taxonomy',
                rows=1000,
                cols=len(headers)
            )
            worksheet.append_row(headers)
            print(f"✅ Content_Taxonomy créé avec {len(headers)} colonnes")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur init Content_Taxonomy: {e}")
        return False

# ==================== SAUVEGARDE ====================

def save_daily_channel_data(sheets_client, daily_rows):
    """Sauvegarde Phase 1 dans Raw_Daily_Data"""
    try:
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet('Raw_Daily_Data')
        
        # Convertir les données selon les headers
        converted_rows = []
        for row in daily_rows:
            converted_row = [
                row[0],  # date
                row[1] if len(row) > 1 else 0,  # total_views
                row[2] if len(row) > 2 else 0,  # total_watch_time
                row[3] if len(row) > 3 else 0,  # subscribers_gained
                row[4] if len(row) > 4 else 0,  # subscribers_lost
                row[5] if len(row) > 5 else 0,  # total_comments
                row[6] if len(row) > 6 else 0,  # total_likes
                row[7] if len(row) > 7 else 0   # avg_view_duration
            ]
            converted_rows.append(converted_row)
        
        if converted_rows:
            worksheet.append_rows(converted_rows)
            print(f"✅ {len(converted_rows)} lignes ajoutées à Raw_Daily_Data")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde Raw_Daily_Data: {e}")
        return False

def save_video_performance_data(sheets_client, video_rows):
    """Sauvegarde Phase 2 dans Video_Performance_Data"""
    try:
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet('Video_Performance_Data')
        
        extraction_date = datetime.now().strftime('%Y-%m-%d')
        converted_rows = []
        
        for row in video_rows:
            converted_row = [
                row[0],  # video_id
                row[1] if len(row) > 1 else 0,  # views_7d
                row[2] if len(row) > 2 else 0,  # watch_time_7d
                row[3] if len(row) > 3 else 0,  # avg_view_duration
                row[4] if len(row) > 4 else 0,  # avg_view_percentage
                extraction_date  # extraction_date
            ]
            converted_rows.append(converted_row)
        
        if converted_rows:
            worksheet.append_rows(converted_rows)
            print(f"✅ {len(converted_rows)} lignes ajoutées à Video_Performance_Data")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde Video_Performance_Data: {e}")
        return False

def save_video_master(sheets_client, video_master_data):
    """Sauvegarde Video Master enrichi"""
    try:
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet('Video_Master')
        
        if video_master_data:
            # Effacer les données existantes (garder headers)
            worksheet.clear()
            worksheet.append_row(SHEET_HEADERS['Video_Master'])
            
            # Ajouter les nouvelles données
            worksheet.append_rows(video_master_data)
            print(f"✅ {len(video_master_data)} lignes ajoutées à Video_Master")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde Video_Master: {e}")
        return False

def save_content_taxonomy(sheets_client, taxonomy_data):
    """Sauvegarde Content Taxonomy"""
    try:
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet('Content_Taxonomy')
        
        if taxonomy_data:
            # Effacer les données existantes (garder headers)
            worksheet.clear()
            worksheet.append_row(SHEET_HEADERS['Content_Taxonomy'])
            
            # Ajouter les nouvelles données
            worksheet.append_rows(taxonomy_data)
            print(f"✅ {len(taxonomy_data)} lignes ajoutées à Content_Taxonomy")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde Content_Taxonomy: {e}")
        return False

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print("=" * 70)
    print("🚀 YOUTUBE ETL SERVICE - PHASE 3 ARCHITECTURE HYBRIDE V6.0")
    print("=" * 70)
    print(f"🌐 Port: {port}")
    print(f"📺 Channel: {CHANNEL_ID}")
    print(f"📊 Spreadsheet: {SPREADSHEET_ID}")
    print(f"🔐 Auth YouTube: OAuth User Token (Data + Analytics)")
    print(f"🔐 Auth Sheets: Service Account")
    print("=" * 70)
    print("🎯 PHASE 3 FEATURES:")
    print("   📋 Video_Master avec métadonnées enrichies")
    print("   🏷️ Content_Taxonomy avec classification automatique")
    print("   🏆 Performance Tier scoring automatique")
    print("   🎨 Content Pillars auto-détection")
    print("   📊 Architecture hybride optimisée")
    print("=" * 70)
    print("📋 Endpoints:")
    print("   GET  /              - Accueil Phase 3")
    print("   GET  /test          - Test basique")
    print("   GET  /test-youtube  - Test YouTube APIs")
    print("   GET  /test-sheets   - Test Sheets")
    print("   GET  /etl           - ETL Phase 1+2 (compatibilité)")
    print("   GET  /etl-phase3    - ETL COMPLET Phase 3")
    print("   GET  /enrich-metadata - Métadonnées seulement")
    print("   GET  /init-taxonomy - Init Content Taxonomy")
    print("=" * 70)
    print("🎯 SUCCESS METRICS PHASE 3:")
    print("   ✅ Video_Master alimenté automatiquement")
    print("   ✅ Performance Tier cohérent")
    print("   ✅ Content pillars ≥85% précision")
    print("   ✅ Quotas API <50% utilisation")
    print("   ✅ Time to insight: 2h → 15min")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=port, debug=True)
