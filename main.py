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
SPREADSHEET_NAME = "YouTube ETL Data"
SPREADSHEET_ID = "1bvob7xaoO5X-RHAhl34ZVcX2IH2qYufVy9aKaUdNXxU"
CHANNEL_ID = "UCS1m_ZhEAbQKfvIdAwoax2A"

@app.route('/')
def hello():
    return jsonify({
        'status': 'success',
        'message': 'YouTube ETL Service with Dual Auth!',
        'service': 'youtube-etl',
        'version': '3.0',
        'authentication': 'YouTube OAuth + Sheets Service Account',
        'endpoints': ['/etl', '/test', '/test-youtube', '/test-sheets']
    })

@app.route('/test')
def test_basic():
    """Test basique du service"""
    try:
        print("=== 🧪 TEST BASIQUE DU SERVICE ===")
        print(f"🕐 {datetime.now().isoformat()}")
        
        # Variables d'environnement
        env_status = {}
        for var in ['YOUTUBE_TOKEN_JSON', 'GOOGLE_SA_JSON', 'PORT', 'K_SERVICE']:
            value = os.environ.get(var)
            env_status[var] = 'DÉFINIE' if value else 'MANQUANTE'
            print(f"   📋 {var}: {env_status[var]}")
        
        result = {
            'status': 'success',
            'message': 'Service opérationnel',
            'timestamp': datetime.now().isoformat(),
            'environment': env_status,
            'channel_id': CHANNEL_ID,
            'spreadsheet_id': SPREADSHEET_ID,
            'auth_ready': {
                'youtube': env_status.get('YOUTUBE_TOKEN_JSON') == 'DÉFINIE',
                'sheets': env_status.get('GOOGLE_SA_JSON') == 'DÉFINIE'
            }
        }
        
        print("✅ Test basique réussi")
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
    """Test spécifique YouTube Analytics"""
    try:
        print("=== 📺 TEST YOUTUBE ANALYTICS ===")
        
        # Récupérer les credentials YouTube
        youtube_creds = get_youtube_credentials()
        if not youtube_creds:
            raise Exception("YouTube credentials non configurés")
        
        print("✅ Credentials YouTube récupérés")
        
        # Initialiser les services
        youtube_service, analytics_service = get_youtube_services(youtube_creds)
        print("✅ Services YouTube initialisés")
        
        # Test Analytics simple
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=3)
        
        print(f"🔍 Test Analytics période: {start_date} → {end_date}")
        
        response = analytics_service.reports().query(
            ids='channel==MINE',
            startDate=str(start_date),
            endDate=str(end_date),
            metrics='views,estimatedMinutesWatched',
            dimensions='day',
            sort='day'
        ).execute()
        
        rows = response.get('rows', [])
        print(f"✅ Analytics réponse: {len(rows)} lignes")
        
        return jsonify({
            'status': 'success',
            'message': 'YouTube Analytics accessible',
            'data_rows': len(rows),
            'sample_data': rows[:2] if rows else [],
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
    """ETL YouTube complet avec double authentification"""
    try:
        print("=== 🚀 DÉBUT ETL YOUTUBE COMPLET ===")
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
        
        # 4. Récupérer données Analytics
        print("3️⃣ Récupération Analytics...")
        analytics_rows = get_analytics_data_daily(analytics_service, str(start_date), str(end_date))
        print(f"✅ Analytics: {len(analytics_rows)} lignes récupérées")
        
        # 5. Sauvegarder dans Sheets
        print("4️⃣ Sauvegarde dans Sheets...")
        if analytics_rows:
            save_analytics_to_sheets(sheets_client, analytics_rows)
            print("✅ Données sauvegardées dans Sheets")
        else:
            print("⚠️ Aucune donnée à sauvegarder")
        
        # 6. Résultat final
        execution_time = (datetime.now() - start_time).total_seconds()
        print(f"✅ ETL TERMINÉ en {execution_time:.2f}s")
        
        return jsonify({
            'status': 'success',
            'message': 'ETL exécuté avec succès',
            'execution_details': {
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': round(execution_time, 2),
                'period': {
                    'start': str(start_date),
                    'end': str(end_date)
                },
                'analytics_rows': len(analytics_rows),
                'channel_id': CHANNEL_ID,
                'spreadsheet_id': SPREADSHEET_ID
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ ERREUR ETL: {e}")
        return jsonify({
            'status': 'error',
            'error_details': {
                'type': type(e).__name__,
                'message': str(e),
                'function': 'run_etl'
            },
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
    """Récupère le client Google Sheets (Service Account) - SUPER DEBUG"""
    try:
        print("=== 🔍 SUPER DEBUG SERVICE ACCOUNT ===")
        
        # 1. Vérifier la variable d'environnement
        sa_json = os.environ.get('GOOGLE_SA_JSON')
        print(f"📋 Variable exists: {sa_json is not None}")
        
        if not sa_json:
            print("❌ GOOGLE_SA_JSON est None ou vide")
            # Lister toutes les variables d'environnement disponibles
            print("🔍 Variables disponibles:")
            for key in sorted(os.environ.keys()):
                if 'GOOGLE' in key or 'JSON' in key or 'SA' in key:
                    print(f"   {key}: {len(os.environ[key]) if os.environ[key] else 'VIDE'}")
            return None
        
        # 2. Analyser le contenu
        print(f"📏 Longueur JSON: {len(sa_json)} caractères")
        print(f"🔤 Premiers caractères: '{sa_json[:50]}'")
        print(f"🔤 Derniers caractères: '{sa_json[-50:]}'")
        
        # 3. Vérifier si c'est du JSON valide
        try:
            sa_data = json.loads(sa_json)
            print("✅ JSON parse réussi")
        except json.JSONDecodeError as je:
            print(f"❌ JSON invalide à position {je.pos}")
            print(f"   Contexte: '{sa_json[max(0, je.pos-20):je.pos+20]}'")
            return None
        
        # 4. Analyser le contenu du JSON
        print(f"🔍 Clés JSON: {list(sa_data.keys())}")
        print(f"🔍 Type: {sa_data.get('type', 'MANQUANT')}")
        print(f"🔍 Project ID: {sa_data.get('project_id', 'MANQUANT')}")
        print(f"🔍 Client email: {sa_data.get('client_email', 'MANQUANT')}")
        
        # 5. Vérifier les champs obligatoires
        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        missing_fields = [field for field in required_fields if field not in sa_data]
        
        if missing_fields:
            print(f"❌ Champs manquants: {missing_fields}")
            return None
        
        print("✅ Tous les champs obligatoires présents")
        
        # 6. Créer les credentials
        print("🔧 Création des credentials...")
        credentials = ServiceAccountCredentials.from_service_account_info(
            sa_data,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        print("✅ Credentials créés")
        
        # 7. Autoriser gspread
        print("🔧 Autorisation gspread...")
        client = gspread.authorize(credentials)
        print("✅ Client gspread autorisé")
        
        return client
        
    except Exception as e:
        print(f"❌ ERREUR INATTENDUE: {type(e).__name__}")
        print(f"   Message: {str(e)}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return None

def get_youtube_services(credentials):
    """Initialise les services YouTube SANS CACHE"""
    print("🔧 Construction services YouTube...")
    
    # SOLUTION du bug cache multi-utilisateurs
    youtube_service = build(
        'youtube', 'v3',
        credentials=credentials,
        cache_discovery=False  # CRUCIAL !
    )
    
    analytics_service = build(
        'youtubeAnalytics', 'v2',
        credentials=credentials,
        cache_discovery=False  # CRUCIAL !
    )
    
    print("✅ Services YouTube construits (cache désactivé)")
    return youtube_service, analytics_service

# ==================== RÉCUPÉRATION DONNÉES ====================

def get_analytics_data_daily(analytics_service, start_date, end_date):
    """Récupère les données Analytics par jour (agrégat chaîne)"""
    try:
        print(f"📊 Requête Analytics: {start_date} → {end_date}")
        
        response = analytics_service.reports().query(
            ids='channel==MINE',
            startDate=start_date,
            endDate=end_date,
            metrics='views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost',
            dimensions='day',
            sort='day'
        ).execute()
        
        rows = response.get('rows', [])
        print(f"✅ Analytics: {len(rows)} lignes récupérées")
        
        return rows
        
    except Exception as e:
        print(f"❌ Erreur Analytics: {e}")
        return []

# ==================== SAUVEGARDE ====================

def save_analytics_to_sheets(sheets_client, analytics_rows):
    """Sauvegarde les données Analytics dans Google Sheets"""
    try:
        print(f"💾 Sauvegarde {len(analytics_rows)} lignes...")
        
        # Ouvrir le spreadsheet
        spreadsheet = sheets_client.open_by_key(SPREADSHEET_ID)
        
        # Vérifier/créer la feuille yt_video_daily
        try:
            worksheet = spreadsheet.worksheet('yt_video_daily')
            print("✅ Feuille yt_video_daily trouvée")
        except gspread.WorksheetNotFound:
            print("📋 Création feuille yt_video_daily...")
            worksheet = spreadsheet.add_worksheet(
                title='yt_video_daily',
                rows=1000,
                cols=12
            )
            # Ajouter les en-têtes
            headers = [
                'day', 'videoId', 'views', 'estimatedMinutesWatched',
                'averageViewDuration', 'averageViewPercentage',
                'subscribersGained', 'subscribersLost', 'endScreenClicks',
                'cardClicks', 'impressions', 'clickThroughRate'
            ]
            worksheet.append_row(headers)
            print("✅ Feuille créée avec en-têtes")
        
        # Convertir les données Analytics au format requis
        # Format: day, videoId(vide), views, estimatedMinutesWatched, averageViewDuration, averageViewPercentage, subscribersGained, subscribersLost, endScreenClicks(vide), cardClicks(vide), impressions(vide), clickThroughRate(vide)
        converted_rows = []
        for row in analytics_rows:
            converted_row = [
                row[0],  # day
                '',      # videoId (vide car agrégat par jour)
                row[1],  # views
                row[2],  # estimatedMinutesWatched
                row[3],  # averageViewDuration
                row[4],  # averageViewPercentage
                row[5],  # subscribersGained
                row[6],  # subscribersLost
                '',      # endScreenClicks (non disponible dans l'API)
                '',      # cardClicks (non disponible dans l'API)
                '',      # impressions (non disponible dans l'API)
                ''       # clickThroughRate (non disponible dans l'API)
            ]
            converted_rows.append(converted_row)
        
        # Ajouter les données
        if converted_rows:
            worksheet.append_rows(converted_rows)
            print(f"✅ {len(converted_rows)} lignes ajoutées à yt_video_daily")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde: {e}")
        return False

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print("=" * 60)
    print("🚀 YOUTUBE ETL SERVICE - DOUBLE AUTHENTIFICATION")
    print("=" * 60)
    print(f"🌐 Port: {port}")
    print(f"📺 Channel: {CHANNEL_ID}")
    print(f"📊 Spreadsheet: {SPREADSHEET_ID}")
    print(f"🔐 Auth YouTube: OAuth User Token")
    print(f"🔐 Auth Sheets: Service Account")
    print("=" * 60)
    print("📋 Endpoints:")
    print("   GET  /           - Accueil")
    print("   GET  /test       - Test basique")
    print("   GET  /test-youtube - Test YouTube seul")
    print("   GET  /test-sheets  - Test Sheets seul")
    print("   GET  /etl        - ETL complet")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=port, debug=True)
