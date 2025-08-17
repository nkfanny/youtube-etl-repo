from flask import Flask, jsonify
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import gspread
from datetime import datetime, timedelta
import os
import json
import re

app = Flask(__name__)

# Configuration
SPREADSHEET_NAME = "YouTube ETL Data"
SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Votre ID de chaîne (à remplacer)
CHANNEL_ID = "UCS1m_ZhEAbQKfvIdAwoax2A"  # REMPLACEZ PAR VOTRE VRAI ID

@app.route('/')
def hello():
    return jsonify({
        'status': 'success',
        'message': 'YouTube ETL Service is running!',
        'service': 'youtube-etl',
        'version': '2.0',
        'endpoints': ['/etl', '/test']
    })

@app.route('/test')
def test_apis():
    """Test des APIs YouTube sans cache"""
    try:
        print("=== DÉBUT TEST DES APIS YOUTUBE ===")
        print(f"🕐 Timestamp: {datetime.now().isoformat()}")
        print(f"🔧 Version Flask: {Flask.__version__}")
        print(f"📊 Port configuré: {os.environ.get('PORT', 8080)}")
        print(f"🌍 Timezone: {datetime.now().astimezone().tzinfo}")
        
        # Test variables d'environnement
        print("📋 Variables d'environnement disponibles:")
        env_vars = ['PORT', 'GOOGLE_APPLICATION_CREDENTIALS', 'K_SERVICE', 'K_REVISION']
        for var in env_vars:
            value = os.environ.get(var, 'NON_DÉFINIE')
            print(f"   {var}: {value}")
        
        # Test basique sans credentials (pour commencer)
        result = {
            'status': 'success',
            'message': 'Service de test fonctionnel',
            'timestamp': datetime.now().isoformat(),
            'environment': {
                'port': os.environ.get('PORT', 8080),
                'service': os.environ.get('K_SERVICE', 'local'),
                'revision': os.environ.get('K_REVISION', 'dev')
            },
            'note': 'Credentials à configurer'
        }
        
        print("✅ Test réussi - Service opérationnel")
        print("=== FIN TEST ===")
        return jsonify(result)
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ ERREUR DURANT LE TEST:")
        print(f"   Type: {type(e).__name__}")
        print(f"   Message: {error_msg}")
        print(f"   Timestamp: {datetime.now().isoformat()}")
        
        return jsonify({
            'status': 'error',
            'error_type': type(e).__name__,
            'message': error_msg,
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/etl')
def run_etl():
    """ETL YouTube complet - sera activé une fois les credentials configurés"""
    try:
        print("=== 🚀 DÉBUT ETL YOUTUBE ===")
        print(f"🕐 Début exécution: {datetime.now().isoformat()}")
        
        # Calculer la fenêtre de dates (7 derniers jours)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        print(f"📅 Période ETL calculée:")
        print(f"   📅 Date début: {start_date}")
        print(f"   📅 Date fin: {end_date}")
        print(f"   📊 Nombre de jours: 7")
        
        # Debug Channel ID
        print(f"📺 Configuration chaîne:")
        print(f"   🆔 Channel ID: {CHANNEL_ID}")
        print(f"   📋 Spreadsheet: {SPREADSHEET_NAME}")
        
        # Simuler les étapes ETL avec logs détaillés
        print("📋 Étapes ETL à exécuter:")
        print("   1️⃣ Initialisation des services YouTube")
        print("   2️⃣ Récupération données Analytics")
        print("   3️⃣ Récupération métadonnées vidéos")
        print("   4️⃣ Sauvegarde Google Sheets")
        print("   5️⃣ Logging de l'exécution")
        
        # Pour l'instant, simuler l'ETL
        print("⚠️ MODE SIMULATION - Credentials non configurés")
        
        result = {
            'status': 'success',
            'message': 'ETL simulé avec succès',
            'execution_details': {
                'start_time': datetime.now().isoformat(),
                'period': {
                    'start': str(start_date),
                    'end': str(end_date),
                    'days': 7
                },
                'channel_id': CHANNEL_ID,
                'spreadsheet': SPREADSHEET_NAME,
                'analytics_rows': 0,
                'metadata_rows': 0,
                'mode': 'simulation'
            },
            'next_steps': [
                'Configurer Service Account',
                'Ajouter credentials JSON',
                'Tester APIs réelles',
                'Activer mode production'
            ],
            'timestamp': datetime.now().isoformat()
        }
        
        print("✅ ETL SIMULÉ TERMINÉ AVEC SUCCÈS")
        print(f"🕐 Fin exécution: {datetime.now().isoformat()}")
        print("=== 🏁 FIN ETL ===")
        
        return jsonify(result)
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ ERREUR CRITIQUE DURANT L'ETL:")
        print(f"   🔥 Type d'erreur: {type(e).__name__}")
        print(f"   💬 Message: {error_msg}")
        print(f"   🕐 Timestamp: {datetime.now().isoformat()}")
        print(f"   📍 Localisation: Fonction run_etl()")
        
        # TODO: En production, envoyer une alerte
        
        return jsonify({
            'status': 'error',
            'error_details': {
                'type': type(e).__name__,
                'message': error_msg,
                'function': 'run_etl',
                'timestamp': datetime.now().isoformat()
            },
            'troubleshooting': [
                'Vérifier les credentials',
                'Vérifier les APIs activées',
                'Consulter les logs détaillés'
            ]
        }), 500

def get_fresh_credentials():
    """Récupère des credentials frais SANS CACHE"""
    # TODO: Implémenter avec Service Account
    # Pour l'instant, retourne None
    return None

def get_youtube_services():
    """Initialise les services YouTube SANS CACHE (solution du bug GitHub)"""
    print("🔧 Initialisation des services YouTube...")
    print("🚫 Mode SANS CACHE activé (correction bug Analytics)")
    
    credentials = get_fresh_credentials()
    
    if not credentials:
        print("❌ ERREUR: Credentials non configurés")
        raise Exception("Credentials non configurés")
    
    print("✅ Credentials récupérés")
    
    try:
        # IMPORTANT: cache_discovery=False pour éviter le bug de cache
        print("🔨 Construction service YouTube Data API v3...")
        youtube_service = build(
            'youtube', 'v3', 
            credentials=credentials, 
            cache_discovery=False  # SOLUTION DU BUG !
        )
        print("✅ YouTube Data API service initialisé")
        
        print("🔨 Construction service YouTube Analytics API v2...")
        analytics_service = build(
            'youtubeAnalytics', 'v2', 
            credentials=credentials,
            cache_discovery=False  # SOLUTION DU BUG !
        )
        print("✅ YouTube Analytics API service initialisé")
        
        print("🎉 Tous les services YouTube sont prêts")
        return youtube_service, analytics_service
        
    except Exception as e:
        print(f"❌ ERREUR lors de l'initialisation des services:")
        print(f"   Type: {type(e).__name__}")
        print(f"   Message: {str(e)}")
        raise

def get_analytics_data(analytics_service, start_date, end_date):
    """Récupère les données YouTube Analytics SANS CACHE"""
    try:
        print(f"📊 Début récupération Analytics...")
        print(f"   📅 Période: {start_date} → {end_date}")
        print(f"   📺 Channel ID: {CHANNEL_ID}")
        
        # Nettoyer explicitement le cache (équivalent PHP)
        # En Python, on utilise cache_discovery=False dans build()
        print("🧹 Cache désactivé (correction bug multi-utilisateurs)")
        
        print("🔍 Construction requête Analytics...")
        metrics = 'views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost'
        print(f"   📈 Métriques: {metrics}")
        print(f"   📊 Dimensions: video,day")
        print(f"   🔢 Tri: day")
        
        print("🚀 Exécution requête YouTube Analytics API...")
        response = analytics_service.reports().query(
            ids=f'channel=={CHANNEL_ID}',
            startDate=start_date,
            endDate=end_date,
            metrics=metrics,
            dimensions='video,day',
            sort='day'
        ).execute()
        
        rows = response.get('rows', [])
        columns = response.get('columnHeaders', [])
        
        print(f"✅ Récupération Analytics terminée:")
        print(f"   📊 Lignes récupérées: {len(rows)}")
        print(f"   📋 Colonnes: {len(columns)}")
        
        if len(rows) > 0:
            print(f"   📝 Exemple première ligne: {rows[0][:3]}...")
        else:
            print("   ⚠️ Aucune donnée pour cette période")
        
        return rows
        
    except Exception as e:
        print(f"❌ ERREUR YouTube Analytics API:")
        print(f"   🔥 Type: {type(e).__name__}")
        print(f"   💬 Message: {str(e)}")
        print(f"   📍 Fonction: get_analytics_data")
        
        # Log des détails de la requête pour debug
        print(f"   🔍 Détails requête:")
        print(f"      Channel: {CHANNEL_ID}")
        print(f"      Dates: {start_date} → {end_date}")
        print(f"      Métriques: {metrics}")
        
        return []

def get_video_metadata(youtube_service, video_ids):
    """Récupère les métadonnées des vidéos"""
    if not video_ids:
        return []
    
    metadata = []
    
    # Traiter par batches de 50
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        
        try:
            response = youtube_service.videos().list(
                part='snippet,contentDetails,statistics,status',
                id=','.join(batch)
            ).execute()
            
            for item in response['items']:
                duration_sec = parse_duration(item['contentDetails']['duration'])
                
                metadata.append([
                    item['id'],
                    item['snippet']['title'][:255],
                    item['snippet']['publishedAt'],
                    duration_sec,
                    1 if duration_sec <= 60 else 0,
                    item['status']['privacyStatus']
                ])
                
        except Exception as e:
            print(f"Erreur métadonnées batch: {e}")
            continue
    
    return metadata

def parse_duration(duration_iso):
    """Convertit PT#H#M#S en secondes"""
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

def save_to_sheets(data, sheet_name):
    """Sauvegarde dans Google Sheets"""
    print(f"💾 Début sauvegarde Google Sheets...")
    print(f"   📊 Données: {len(data)} lignes")
    print(f"   📋 Feuille: {sheet_name}")
    print(f"   📄 Spreadsheet: {SPREADSHEET_NAME}")
    
    # TODO: Implémenter gspread
    print("⚠️ Sauvegarde simulée - gspread à implémenter")
    print(f"✅ Simulation sauvegarde terminée")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print("=" * 50)
    print("🚀 DÉMARRAGE YOUTUBE ETL SERVICE")
    print("=" * 50)
    print(f"🌐 Port: {port}")
    print(f"🔧 Mode debug: True")
    print(f"📺 Channel ID: {CHANNEL_ID}")
    print(f"📄 Spreadsheet: {SPREADSHEET_NAME}")
    print(f"🕐 Démarrage: {datetime.now().isoformat()}")
    print("=" * 50)
    print("📋 Endpoints disponibles:")
    print("   GET  /      - Page d'accueil")
    print("   GET  /test  - Test des APIs")
    print("   GET  /etl   - ETL YouTube complet")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=port, debug=True)
