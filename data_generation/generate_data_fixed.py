"""
GÉNÉRATION DE DONNÉES COMPATIBLE DW
Génère des données directement structurées pour le schéma en étoile
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

class DataGeneratorDW:
    def __init__(self, seed=42):
        np.random.seed(seed)
        
    def generate_dim_temps(self):
        """Génère la dimension temps (365 jours 2024)"""
        dates = pd.date_range('2024-01-01', '2024-12-31')
        
        def get_saison(mois):
            if mois in [12, 1, 2]: return 'Hiver'
            elif mois in [3, 4, 5]: return 'Printemps'
            elif mois in [6, 7, 8]: return 'Eté'
            else: return 'Automne'
        
        dim_temps = pd.DataFrame({
            'id_temps': range(1, len(dates) + 1),
            'date_complete': dates,
            'annee': dates.year,
            'mois': dates.month,
            'jour': dates.day,
            'trimestre': dates.quarter,
            'semaine_annee': dates.isocalendar().week,
            'jour_semaine': dates.day_name(),
            'est_weekend': dates.dayofweek.isin([5, 6]),
            'saison': dates.month.map(get_saison)
        })
        return dim_temps
    
    def generate_dim_station(self):
        """Génère la dimension station (15 stations)"""
        zones = [
            {'zone': 'Haouz', 'ville': 'Marrakech', 'alt_min': 300, 'alt_max': 500},
            {'zone': 'Gharb', 'ville': 'Kenitra', 'alt_min': 50, 'alt_max': 200},
            {'zone': 'Souss-Massa', 'ville': 'Agadir', 'alt_min': 100, 'alt_max': 400},
            {'zone': 'Oriental', 'ville': 'Oujda', 'alt_min': 400, 'alt_max': 600},
            {'zone': 'Tadla', 'ville': 'Béni Mellal', 'alt_min': 200, 'alt_max': 400}
        ]
        
        stations = []
        station_id = 1
        
        for zone_info in zones:
            for i in range(1, 4):  # 3 stations par zone
                stations.append({
                    'id_station': station_id,
                    'code_station': f"STN_{zone_info['zone'][:3].upper()}_{i:03d}",
                    'nom_station': f"Station {zone_info['zone']} {i}",
                    'ville': zone_info['ville'],
                    'zone_geo': zone_info['zone'],
                    'altitude': np.random.randint(zone_info['alt_min'], zone_info['alt_max']),
                    'capteur_type': np.random.choice(['Digital', 'Analogique']),
                    'latitude': round(31.0 + np.random.uniform(-2, 2), 4),
                    'longitude': round(-8.0 + np.random.uniform(-2, 2), 4)
                })
                station_id += 1
        
        return pd.DataFrame(stations)
    
    def generate_dim_region(self):
        """Génère la dimension région"""
        regions = [
            {'zone_geo': 'Haouz', 'region_admin': 'Marrakech-Safi', 'superficie_ha': 1200000},
            {'zone_geo': 'Gharb', 'region_admin': 'Rabat-Salé-Kénitra', 'superficie_ha': 800000},
            {'zone_geo': 'Souss-Massa', 'region_admin': 'Souss-Massa', 'superficie_ha': 700000},
            {'zone_geo': 'Oriental', 'region_admin': 'Oriental', 'superficie_ha': 900000},
            {'zone_geo': 'Tadla', 'region_admin': 'Béni Mellal-Khénifra', 'superficie_ha': 600000}
        ]
        
        dim_region = pd.DataFrame(regions)
        dim_region['id_region'] = range(1, len(dim_region) + 1)
        return dim_region
    
    def generate_fait_releves(self, dim_temps, dim_station):
        """Génère la table de faits avec KPIs calculés"""
        faits = []
        
        for _, temp_row in dim_temps.iterrows():
            date = temp_row['date_complete']
            mois = temp_row['mois']
            saison = temp_row['saison']
            
            for _, station in dim_station.iterrows():
                zone = station['zone_geo']
                
                # Simulation données météo cohérentes
                if saison == 'Eté':
                    temp_base = 35 if zone == 'Oriental' else 32
                    prob_pluie = 0.05
                elif saison == 'Hiver':
                    temp_base = 8 if zone == 'Oriental' else 12
                    prob_pluie = 0.3 if zone == 'Gharb' else 0.15
                else:
                    temp_base = 25
                    prob_pluie = 0.2
                
                # Température avec variation journalière
                temp_max = temp_base + np.random.normal(0, 3)
                temp_max = max(-5, min(45, temp_max))
                
                # Précipitations
                precip = np.random.exponential(8) if np.random.random() < prob_pluie else 0
                
                # CALCUL DES KPIs
                # KPI 1: IDHC (simulé sur 30 jours)
                idhc_base = 40 if zone in ['Haouz', 'Souss-Massa'] else 30
                idhc_30j = idhc_base + np.random.normal(0, 15)
                idhc_30j = max(0, idhc_30j)
                
                # KPI 3: Jours sans pluie
                jours_sans_pluie = np.random.poisson(7)
                
                # KPI 2 & 5: Score risque composite
                score_temp = min(100, max(0, (temp_max - 20) * 5))
                score_idhc = min(100, idhc_30j)
                score_secheresse = min(100, jours_sans_pluie * 5)
                score_risque = score_temp * 0.4 + score_idhc * 0.4 + score_secheresse * 0.2
                
                fait = {
                    'id_temps': temp_row['id_temps'],
                    'id_station': station['id_station'],
                    'id_region': {'Haouz':1, 'Gharb':2, 'Souss-Massa':3, 
                                  'Oriental':4, 'Tadla':5}[zone],
                    'temperature_max': round(temp_max, 1),
                    'temperature_moy': round(temp_max - 5, 1),
                    'temperature_min': round(temp_max - 10, 1),
                    'humidite_moyenne': round(50 + np.random.normal(0, 15), 1),
                    'precipitations_jour': round(precip, 1),
                    'wind_speed_max': round(np.random.exponential(10), 1),
                    'radiation_solaire': round(np.random.uniform(100, 500), 1),
                    # KPIs
                    'idhc_30j': round(idhc_30j, 1),
                    'jours_sans_pluie': int(jours_sans_pluie),
                    'score_risque': round(score_risque, 1),
                    'niveau_stress_hydrique': 'Critique' if score_risque > 75 else 
                                             'Élevé' if score_risque > 50 else
                                             'Modéré' if score_risque > 25 else 'Normal',
                    'flag_alerte_temperature': temp_max > 40,
                    'qualite_donnee': np.random.choice(['Excellente', 'Bonne', 'Moyenne'], 
                                                      p=[0.7, 0.25, 0.05]),
                    'source_donnee': 'Simulation DW'
                }
                faits.append(fait)
        
        return pd.DataFrame(faits)
    
    def run(self):
        print("Génération des données pour le DW...")
        
        dim_temps = self.generate_dim_temps()
        dim_station = self.generate_dim_station()
        dim_region = self.generate_dim_region()
        fait_releves = self.generate_fait_releves(dim_temps, dim_station)
        
        # Sauvegarde
        dim_temps.to_csv('data_generation/dim_temps.csv', index=False)
        dim_station.to_csv('data_generation/dim_station.csv', index=False)
        dim_region.to_csv('data_generation/dim_region.csv', index=False)
        fait_releves.to_csv('data_generation/fait_releves.csv', index=False)
        
        # Génération fichiers sources pour compatibilité
        self.generate_source_files(dim_station, fait_releves)
        
        print(f"✅ dim_temps: {len(dim_temps)} dates")
        print(f"✅ dim_station: {len(dim_station)} stations")
        print(f"✅ dim_region: {len(dim_region)} régions")
        print(f"✅ fait_releves: {len(fait_releves):,} enregistrements")
        
        return dim_temps, dim_station, dim_region, fait_releves
    
    def generate_source_files(self, dim_station, fait_releves):
        """Génère les fichiers sources dans l'ancien format pour compatibilité"""
        # stations_master.xlsx
        stations_master = dim_station.copy()
        stations_master['ID_Station'] = stations_master['id_station']
        stations_master['Nom_Station'] = stations_master['nom_station']
        stations_master['Ville'] = stations_master['ville']
        stations_master['Zone_Geo'] = stations_master['zone_geo']
        stations_master['Altitude'] = stations_master['altitude']
        stations_master['Capteur_Type'] = stations_master['capteur_type']
        
        stations_master[['ID_Station', 'Nom_Station', 'Ville', 'Altitude', 
                         'Zone_Geo', 'Capteur_Type']].to_excel(
            'data_generation/stations_master.xlsx', index=False)
        
        # relevés_meteo.csv (échantillon)
        sample_faits = fait_releves.sample(min(10000, len(fait_releves)))
        releves = []
        for _, row in sample_faits.iterrows():
            releves.append({
                'timestamp': f"2024-{np.random.randint(1,13):02d}-{np.random.randint(1,28):02d} "
                           f"{np.random.randint(0,24):02d}:00:00",
                'st_id': row['id_station'],
                'temp_c': row['temperature_max'],
                'hum_pct': row['humidite_moyenne'],
                'wind_speed': row['wind_speed_max']
            })
        
        pd.DataFrame(releves).to_csv('data_generation/relevés_meteo.csv', index=False)
        
        # notifications.json
        notifications = []
        for _, station in dim_station.iterrows():
            for day in range(1, 31):
                precip = np.random.exponential(5) if np.random.random() < 0.2 else 0
                severity = 'Rouge' if precip > 50 else 'Orange' if precip > 30 else 'Jaune' if precip > 15 else 'RAS'
                
                notifications.append({
                    'date': f"2024-06-{day:02d}",
                    'station_code': station['id_station'],
                    'precip_mm': round(precip, 1),
                    'type_precip': 'Pluie' if precip > 0 else 'Aucune',
                    'severity_index': severity,
                    'alert_msg': f"Alerte {severity}: {precip}mm de pluie"
                })
        
        with open('data_generation/notifications.json', 'w', encoding='utf-8') as f:
            json.dump(notifications, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    generator = DataGeneratorDW(seed=42)
    generator.run()