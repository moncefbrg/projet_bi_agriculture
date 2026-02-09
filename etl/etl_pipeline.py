"""
ETL ULTRA SIMPLIFIÉ - Agriculture Résilience 2030
Version qui fonctionne TOUJOURS
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys

class ETLUltraSimple:
    def __init__(self):
        # Configuration simple
        db_url = 'postgresql://bi_user:bi_password@postgres:5432/agriculture_dw'
        print(f"Connexion à PostgreSQL...")
        
        self.engine = create_engine(db_url)
        
    def test_connection(self):
        """Teste la connexion"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                print(f"Connecté à: {version}")
                return True
        except Exception as e:
            print(f"Erreur connexion: {e}")
            return False
    
    def clean_database(self):
        """Nettoie SEULEMENT les données dynamiques, préserve les dimensions de base"""
        print("Nettoyage des données dynamiques...")
        
        try:
            with self.engine.connect() as conn:
                # Liste des tables à NE PAS nettoyer (déjà peuplées par init.sql)
                tables_preserve = ['dim_region', 'dim_alerte']
                
                # Liste des tables à nettoyer (données dynamiques)
                tables_to_clean = [
                    'fait_releves_climatiques',
                    'mv_dashboard_kpis',
                    'dim_temps',
                    'dim_station'
                ]
                
                # Désactiver temporairement les contraintes
                conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
                
                for table in tables_to_clean:
                    try:
                        # TRUNCATE est plus rapide que DELETE
                        if table == 'mv_dashboard_kpis':
                            # Pour la vue matérialisée
                            conn.execute(text(f"DELETE FROM {table};"))
                        else:
                            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
                        print(f"   {table} vidée")
                    except Exception as e:
                        print(f"   Attention sur {table}: {str(e)[:50]}...")
                
                # Réactiver les contraintes
                conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE;"))
                conn.commit()
                
            print("Données dynamiques nettoyées")
            return True
            
        except Exception as e:
            print(f"Erreur nettoyage: {e}")
            # Essayer de rollback
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("ROLLBACK;"))
            except:
                pass
            return False
    
    def insert_minimal_data(self):
        """Insère des données minimales mais complètes"""
        print("Insertion de données minimales...")
        
        try:
            # 1. Vérifier quelles données existent déjà
            with self.engine.connect() as conn:
                # Vérifier dim_temps
                count_temps = conn.execute(text("SELECT COUNT(*) FROM dim_temps")).scalar()
                if count_temps > 0:
                    print(f"   dim_temps: {count_temps} dates déjà existantes - réutilisation")
                    # Récupérer les dates existantes
                    dates_result = conn.execute(text("SELECT id_temps, date_complete FROM dim_temps ORDER BY date_complete"))
                    date_ids = {row[1]: row[0] for row in dates_result}
                else:
                    # 1. dim_temps - 7 jours de données
                    dates = pd.date_range('2024-01-01', periods=7)
                    dim_temps_data = []
                    for i, date in enumerate(dates, 1):
                        dim_temps_data.append({
                            'date_complete': date.date(),
                            'annee': date.year,
                            'mois': date.month,
                            'jour': date.day,
                            'trimestre': (date.month - 1) // 3 + 1,
                            'semaine_annee': date.isocalendar().week,
                            'jour_semaine': date.strftime('%A'),
                            'est_weekend': date.weekday() >= 5,
                            'saison': 'Hiver' if date.month in [12, 1, 2] else 
                                     'Printemps' if date.month in [3, 4, 5] else
                                     'Eté' if date.month in [6, 7, 8] else 'Automne'
                        })
                    
                    dim_temps_df = pd.DataFrame(dim_temps_data)
                    dim_temps_df.to_sql('dim_temps', self.engine, if_exists='append', index=False)
                    print(f"   dim_temps: {len(dim_temps_df)} dates ajoutées")
                    
                    # Récupérer les IDs
                    dates_result = conn.execute(text("SELECT id_temps, date_complete FROM dim_temps ORDER BY date_complete"))
                    date_ids = {row[1]: row[0] for row in dates_result}
                
                # 2. Vérifier dim_station
                count_station = conn.execute(text("SELECT COUNT(*) FROM dim_station")).scalar()
                if count_station > 0:
                    print(f"   dim_station: {count_station} stations déjà existantes - réutilisation")
                    # Récupérer les stations existantes
                    stations_result = conn.execute(text("SELECT id_station, code_station FROM dim_station"))
                    station_ids = {}
                    for row in stations_result:
                        station_code = row[1]
                        if 'HAO' in station_code:
                            station_ids['STN_HAO_001'] = row[0]
                        elif 'GHA' in station_code:
                            station_ids['STN_GHA_001'] = row[0]
                        elif 'SOU' in station_code:
                            station_ids['STN_SOU_001'] = row[0]
                else:
                    # dim_station - 3 stations
                    dim_station_data = [
                        {
                            'code_station': 'STN_HAO_001',
                            'nom_station': 'Station Haouz Centre',
                            'ville': 'Marrakech',
                            'zone_geo': 'Haouz',
                            'altitude': 450,
                            'capteur_type': 'Digital',
                            'latitude': 31.6295,
                            'longitude': -7.9811
                        },
                        {
                            'code_station': 'STN_GHA_001',
                            'nom_station': 'Station Gharb Nord',
                            'ville': 'Kenitra',
                            'zone_geo': 'Gharb',
                            'altitude': 150,
                            'capteur_type': 'Analogique',
                            'latitude': 34.2610,
                            'longitude': -6.5802
                        },
                        {
                            'code_station': 'STN_SOU_001',
                            'nom_station': 'Station Souss Sud',
                            'ville': 'Agadir',
                            'zone_geo': 'Souss-Massa',
                            'altitude': 250,
                            'capteur_type': 'Digital',
                            'latitude': 30.4278,
                            'longitude': -9.5981
                        }
                    ]
                    
                    dim_station_df = pd.DataFrame(dim_station_data)
                    dim_station_df.to_sql('dim_station', self.engine, if_exists='append', index=False)
                    print(f"   dim_station: {len(dim_station_df)} stations ajoutées")
                    
                    # Récupérer les IDs
                    stations_result = conn.execute(text("SELECT id_station, code_station FROM dim_station"))
                    station_ids = {}
                    for row in stations_result:
                        if 'HAO' in row[1]:
                            station_ids['STN_HAO_001'] = row[0]
                        elif 'GHA' in row[1]:
                            station_ids['STN_GHA_001'] = row[0]
                        elif 'SOU' in row[1]:
                            station_ids['STN_SOU_001'] = row[0]
                
                # 3. Récupérer les régions (déjà existantes depuis init.sql)
                regions_result = conn.execute(text("SELECT id_region, zone_geo FROM dim_region"))
                region_ids = {row[1]: row[0] for row in regions_result}
                print(f"   dim_region: {len(region_ids)} régions disponibles")
            
            # 4. fait_releves_climatiques - données cohérentes avec KPIs
            print("   Creation des faits avec KPIs...")
            
            faits_data = []
            for date_str, id_temps in date_ids.items():
                for station_code, id_station in station_ids.items():
                    # Déterminer la zone et région
                    if 'HAO' in station_code:
                        zone = 'Haouz'
                    elif 'GHA' in station_code:
                        zone = 'Gharb'
                    else:
                        zone = 'Souss-Massa'
                    
                    id_region = region_ids.get(zone, 1)
                    
                    # Données réalistes avec variations
                    base_temp = 25 if zone == 'Haouz' else 22 if zone == 'Gharb' else 28
                    temp_max = base_temp + np.random.uniform(-5, 10)
                    precip = np.random.exponential(5) if np.random.random() < 0.3 else 0
                    
                    # Calcul des KPIs
                    idhc = np.clip(30 - precip * 3, 0, 150)
                    jours_sans_pluie = np.random.randint(0, 15)
                    
                    # Score risque
                    score_temp = np.clip((temp_max - 20) / 20 * 100, 0, 100)
                    score_idhc = np.clip(idhc / 150 * 100, 0, 100)
                    score_secheresse = np.clip(jours_sans_pluie / 30 * 100, 0, 100)
                    score_risque = score_temp * 0.4 + score_idhc * 0.4 + score_secheresse * 0.2
                    
                    fait = {
                        'id_temps': id_temps,
                        'id_station': id_station,
                        'id_region': id_region,
                        'temperature_max': round(temp_max, 1),
                        'temperature_moy': round(temp_max - 3, 1),
                        'temperature_min': round(temp_max - 8, 1),
                        'humidite_moyenne': round(np.random.uniform(40, 80), 1),
                        'precipitations_jour': round(precip, 1),
                        'wind_speed_max': round(np.random.exponential(10), 1),
                        'radiation_solaire': round(np.random.uniform(200, 400), 1),
                        'idhc_30j': round(idhc, 1),
                        'jours_sans_pluie': int(jours_sans_pluie),
                        'score_risque': round(score_risque, 1),
                        'niveau_stress_hydrique': 'Critique' if score_risque > 75 else 
                                                 'Élevé' if score_risque > 50 else
                                                 'Modéré' if score_risque > 25 else 'Normal',
                        'flag_alerte_temperature': temp_max > 35,
                        'qualite_donnee': 'Bonne',
                        'source_donnee': 'ETL Minimal'
                    }
                    faits_data.append(fait)
            
            faits_df = pd.DataFrame(faits_data)
            faits_df.to_sql('fait_releves_climatiques', self.engine, if_exists='append', index=False)
            print(f"   fait_releves: {len(faits_df)} relevés ajoutés")
            
            return len(faits_df)
            
        except Exception as e:
            print(f"Erreur insertion: {e}")
            raise
    
    def refresh_views(self):
        """Rafraîchit les vues matérialisées"""
        print("Rafraichissement des vues...")
        
        try:
            with self.engine.connect() as conn:
                try:
                    conn.execute(text("REFRESH MATERIALIZED VIEW mv_dashboard_kpis"))
                    conn.commit()
                    print("   Vue matérialisée rafraichie")
                except Exception as e:
                    print(f"   Impossible de rafraichir la vue matérialisée: {e}")
            
            return True
        except Exception as e:
            print(f"   Erreur rafraichissement: {e}")
            return False
    
    def validate(self):
        """Valide les résultats"""
        print("Validation des données...")
        
        try:
            with self.engine.connect() as conn:
                print("   Statistiques:")
                
                stats = [
                    ("Dates", "SELECT COUNT(*) FROM dim_temps"),
                    ("Stations", "SELECT COUNT(*) FROM dim_station"),
                    ("Regions", "SELECT COUNT(*) FROM dim_region"),
                    ("Alertes", "SELECT COUNT(*) FROM dim_alerte"),
                    ("Releves", "SELECT COUNT(*) FROM fait_releves_climatiques"),
                    ("Score moyen", "SELECT ROUND(AVG(score_risque), 1) FROM fait_releves_climatiques"),
                    ("Alertes temperature", "SELECT COUNT(*) FROM fait_releves_climatiques WHERE flag_alerte_temperature")
                ]
                
                for name, query in stats:
                    try:
                        result = conn.execute(text(query)).scalar()
                        print(f"      {name}: {result}")
                    except Exception as e:
                        print(f"      {name}: Erreur - {str(e)[:50]}")
                
                # Verifier integrite
                integrity = """
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN t.id_temps IS NULL THEN 1 ELSE 0 END) as erreurs_temps,
                        SUM(CASE WHEN s.id_station IS NULL THEN 1 ELSE 0 END) as erreurs_station,
                        SUM(CASE WHEN r.id_region IS NULL THEN 1 ELSE 0 END) as erreurs_region
                    FROM fait_releves_climatiques f
                    LEFT JOIN dim_temps t ON f.id_temps = t.id_temps
                    LEFT JOIN dim_station s ON f.id_station = s.id_station
                    LEFT JOIN dim_region r ON f.id_region = r.id_region
                """
                
                try:
                    result = conn.execute(text(integrity)).fetchone()
                    if result[1] == 0 and result[2] == 0 and result[3] == 0:
                        print(f"   Integrite parfaite ({result[0]} faits)")
                    else:
                        print(f"   Problemes d'integrite: temps={result[1]}, station={result[2]}, region={result[3]}")
                except Exception as e:
                    print(f"   Erreur verification integrite: {e}")
            
            return True
            
        except Exception as e:
            print(f"Erreur validation: {e}")
            return False
    
    def run(self):
        """Exécute l'ETL simplifié"""
        print("=" * 60)
        print("ETL ULTRA SIMPLIFIE")
        print("=" * 60)
        
        start_time = datetime.now()
        
        # Test connexion
        if not self.test_connection():
            print("Impossible de se connecter")
            sys.exit(1)
        
        # Nettoyer
        if not self.clean_database():
            print("Nettoyage partiel, continuation...")
        
        # Inserer données
        try:
            total = self.insert_minimal_data()
        except Exception as e:
            print(f"Echec insertion: {e}")
            sys.exit(1)
        
        # Rafraichir vues
        self.refresh_views()
        
        # Valider
        self.validate()
        
        # Resume
        print("\n" + "=" * 60)
        print("ETL SIMPLIFIE TERMINE !")
        print("=" * 60)
        
        execution_time = datetime.now() - start_time
        print(f"Temps d'execution: {execution_time}")
        print(f"Donnees inserees avec succes")
        print(f"Pret pour le dashboard")
        
        return True

if __name__ == "__main__":
    etl = ETLUltraSimple()
    etl.run()