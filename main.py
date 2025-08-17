# YouTube Analytics - Import Historique Agitips
# Utilise la même config que ton ancien code qui marchait

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import time
import logging

# Configuration logging bb
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration (même que ton ancien code)
CHANNEL_ID = "UCS1m_ZhEAbQKfvIdAwoax2A"  # Agitips
GENESIS_DATE = "2020-11-16"  # Première vidéo

class YouTubeHistoricalImporter:
    def __init__(self):
        self.channel_id = CHANNEL_ID
        self.genesis_date = GENESIS_DATE
        self.youtube_data = None
        self.youtube_analytics = None
        
        # Même authentification que ton ancien code
        self.authenticate()
        
    def authenticate(self):
        """Authentification avec Service Account (comme ton ancien code)"""
        try:
            # Récupère les credentials depuis les variables d'environnement
            # ou depuis le fichier credentials.json
            
            if os.path.exists('credentials.json'):
                credentials = ServiceAccountCredentials.from_service_account_file(
                    'credentials.json',
                    scopes=[
                        'https://www.googleapis.com/auth/youtube.readonly',
                        'https://www.googleapis.com/auth/yt-analytics.readonly'
                    ]
                )
            else:
                # Utilise les variables d'environnement (pour déploiement)
                credentials_info = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))
                credentials = ServiceAccountCredentials.from_service_account_info(
                    credentials_info,
                    scopes=[
                        'https://www.googleapis.com/auth/youtube.readonly',
                        'https://www.googleapis.com/auth/yt-analytics.readonly'
                    ]
                )
            
            self.youtube_data = build('youtube', 'v3', credentials=credentials)
            self.youtube_analytics = build('youtubeAnalytics', 'v2', credentials=credentials)
            
            logger.info("✅ Authentification réussie")
            
        except Exception as e:
            logger.error(f"❌ Erreur authentification: {e}")
            raise
            
    def get_all_videos_since_genesis(self):
        """Récupère toutes les vidéos depuis novembre 2020"""
        logger.info(f"🔍 Récupération vidéos depuis {self.genesis_date}")
        
        videos = []
        
        try:
            # 1. Récupère les uploads de la chaîne
            channel_response = self.youtube_data.channels().list(
                part='contentDetails',
                id=self.channel_id
            ).execute()
            
            uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # 2. Liste toutes les vidéos de la playlist uploads
            next_page_token = None
            
            while True:
                playlist_response = self.youtube_data.playlistItems().list(
                    part='snippet',
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                
                video_ids = []
                for item in playlist_response['items']:
                    publish_date = item['snippet']['publishedAt'][:10]
                    
                    # Filtre par date (depuis genesis)
                    if publish_date >= self.genesis_date:
                        video_ids.append(item['snippet']['resourceId']['videoId'])
                
                # 3. Récupère les détails des vidéos
                if video_ids:
                    videos_response = self.youtube_data.videos().list(
                        part='snippet,statistics,contentDetails',
                        id=','.join(video_ids)
                    ).execute()
                    
                    for video in videos_response['items']:
                        videos.append({
                            'video_id': video['id'],
                            'title': video['snippet']['title'],
                            'publish_date': video['snippet']['publishedAt'][:10],
                            'duration': self._parse_duration(video['contentDetails']['duration']),
                            'description': video['snippet']['description'][:200],
                            'thumbnail_url': video['snippet']['thumbnails']['high']['url'],
                            'views': int(video['statistics'].get('viewCount', 0)),
                            'likes': int(video['statistics'].get('likeCount', 0)),
                            'comments': int(video['statistics'].get('commentCount', 0))
                        })
                
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    break
                    
                time.sleep(0.5)  # Rate limiting
                
            logger.info(f"✅ {len(videos)} vidéos récupérées")
            return videos
            
        except Exception as e:
            logger.error(f"❌ Erreur récupération vidéos: {e}")
            return []
            
    def get_analytics_data(self, video_ids_batch):
        """Récupère les analytics pour un batch de vidéos"""
        try:
            # Performance lifetime
            response = self.youtube_analytics.reports().query(
                ids=f'channel=={self.channel_id}',
                dimensions='video',
                metrics='views,estimatedMinutesWatched,likes,comments,shares,subscribersGained,averageViewDuration',
                filters=f'video=={",".join(video_ids_batch)}',
                startDate=self.genesis_date,
                endDate=datetime.now().strftime('%Y-%m-%d')
            ).execute()
            
            analytics_data = {}
            for row in response.get('rows', []):
                analytics_data[row[0]] = {
                    'analytics_views': row[1],
                    'watch_time_minutes': row[2],
                    'analytics_likes': row[3],
                    'analytics_comments': row[4],
                    'shares': row[5],
                    'subscribers_gained': row[6],
                    'avg_view_duration': row[7]
                }
                
            return analytics_data
            
        except Exception as e:
            logger.error(f"❌ Erreur analytics: {e}")
            return {}
            
    def create_excel_report(self, videos_data, analytics_data):
        """Génère le rapport Excel"""
        logger.info("📊 Génération rapport Excel")
        
        # Combine les données
        for video in videos_data:
            video_id = video['video_id']
            if video_id in analytics_data:
                video.update(analytics_data[video_id])
                
        # Convertit en DataFrame
        df = pd.DataFrame(videos_data)
        
        # Ajoute des colonnes calculées
        df['engagement_rate'] = ((df['likes'] + df['comments']) / df['views'] * 100).round(2)
        df['content_pillar'] = df['title'].apply(self._classify_content)
        df['performance_tier'] = pd.qcut(df['views'], q=4, labels=['D', 'C', 'B', 'A'])
        
        # Sauvegarde Excel
        filename = f"Agitips_Historical_Analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet principal
            df.to_excel(writer, sheet_name='Historical_Data', index=False)
            
            # Sheet résumé
            summary = self._create_summary(df)
            summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet par pilier de contenu
            pillar_analysis = df.groupby('content_pillar').agg({
                'views': ['count', 'mean', 'sum'],
                'engagement_rate': 'mean',
                'avg_view_duration': 'mean'
            }).round(2)
            pillar_analysis.to_excel(writer, sheet_name='Content_Pillars')
            
        logger.info(f"✅ Rapport généré: {filename}")
        return filename
        
    def _classify_content(self, title):
        """Classification simple par titre"""
        title_lower = title.lower()
        
        if any(word in title_lower for word in ['tip', 'astuce', 'conseil']):
            return 'Tips'
        elif any(word in title_lower for word in ['interview', 'rencontre']):
            return 'Interview'  
        elif any(word in title_lower for word in ['formation', 'cours', 'apprendre']):
            return 'Education'
        else:
            return 'General'
            
    def _create_summary(self, df):
        """Crée un résumé exécutif"""
        total_videos = len(df)
        total_views = df['views'].sum()
        avg_views = df['views'].mean()
        best_video = df.loc[df['views'].idxmax()]
        
        summary_data = [
            {'Metric': 'Total Videos', 'Value': total_videos},
            {'Metric': 'Total Views', 'Value': f"{total_views:,}"},
            {'Metric': 'Average Views per Video', 'Value': f"{avg_views:,.0f}"},
            {'Metric': 'Best Performing Video', 'Value': best_video['title']},
            {'Metric': 'Best Video Views', 'Value': f"{best_video['views']:,}"},
            {'Metric': 'Genesis Date', 'Value': self.genesis_date},
            {'Metric': 'Analysis Date', 'Value': datetime.now().strftime('%Y-%m-%d')}
        ]
        
        return pd.DataFrame(summary_data)
        
    def _parse_duration(self, duration_str):
        """Parse durée YouTube (PT1M30S -> 90)"""
        import re
        pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
        match = pattern.match(duration_str)
        
        if match:
            hours = int(match.group(1) or 0)
            minutes = int(match.group(2) or 0) 
            seconds = int(match.group(3) or 0)
            return hours * 3600 + minutes * 60 + seconds
        return 0
        
    def run_full_analysis(self):
        """Lance l'analyse complète"""
        logger.info("🚀 DÉBUT ANALYSE HISTORIQUE AGITIPS")
        start_time = time.time()
        
        try:
            # 1. Récupère toutes les vidéos
            videos = self.get_all_videos_since_genesis()
            
            if not videos:
                logger.error("❌ Aucune vidéo trouvée")
                return None
                
            # 2. Récupère analytics par batch
            video_ids = [v['video_id'] for v in videos]
            all_analytics = {}
            
            batch_size = 10
            for i in range(0, len(video_ids), batch_size):
                batch = video_ids[i:i+batch_size]
                analytics = self.get_analytics_data(batch)
                all_analytics.update(analytics)
                time.sleep(1)  # Rate limiting
                
                logger.info(f"✅ Batch {i//batch_size + 1}/{(len(video_ids)-1)//batch_size + 1}")
                
            # 3. Génère rapport Excel
            filename = self.create_excel_report(videos, all_analytics)
            
            duration = time.time() - start_time
            logger.info(f"✅ ANALYSE TERMINÉE en {duration:.1f}s")
            
            return filename
            
        except Exception as e:
            logger.error(f"💥 ERREUR: {e}")
            raise

# Script principal
if __name__ == "__main__":
    print("🚀 YouTube Historical Analysis - Agitips")
    print("=" * 50)
    
    try:
        # Lance l'analyse
        importer = YouTubeHistoricalImporter()
        result_file = importer.run_full_analysis()
        
        if result_file:
            print(f"\n🎉 SUCCESS!")
            print(f"📁 Fichier généré: {result_file}")
            print(f"📊 Données depuis: {GENESIS_DATE}")
            print(f"📺 Chaîne: Agitips ({CHANNEL_ID})")
        else:
            print("\n❌ Échec de l'analyse")
            
    except Exception as e:
        print(f"\n💥 ERREUR: {e}")
        print("Vérifiez vos credentials et permissions")
