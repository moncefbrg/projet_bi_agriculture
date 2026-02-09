-- database/init.sql
-- PostgreSQL 16 - Schéma DW Agriculture Résilience 2030
-- Optimisé pour les 5 KPIs définis

-- 1. Création des extensions utiles (PG16)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- 2. DIMENSION TEMPS (optimisée pour les requêtes temporelles)
CREATE TABLE dim_temps (
    id_temps SERIAL PRIMARY KEY,  -- CHANGÉ ICI
    date_complete DATE NOT NULL UNIQUE,
    annee SMALLINT NOT NULL CHECK (annee BETWEEN 2000 AND 2100),
    mois SMALLINT NOT NULL CHECK (mois BETWEEN 1 AND 12),
    jour SMALLINT NOT NULL CHECK (jour BETWEEN 1 AND 31),
    trimestre SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semaine_annee SMALLINT NOT NULL CHECK (semaine_annee BETWEEN 1 AND 53),
    jour_semaine VARCHAR(10) NOT NULL,
    est_weekend BOOLEAN NOT NULL,
    saison VARCHAR(10) NOT NULL CHECK (saison IN ('Hiver', 'Printemps', 'Eté', 'Automne')),
    
    -- Index pour performances
    CONSTRAINT unique_date UNIQUE (date_complete)
);

CREATE INDEX idx_temps_date ON dim_temps(date_complete);
CREATE INDEX idx_temps_annee_mois ON dim_temps(annee, mois);
CREATE INDEX idx_temps_saison ON dim_temps(saison);

-- 3. DIMENSION STATION
CREATE TABLE dim_station (
    id_station SERIAL PRIMARY KEY,  -- CHANGÉ ICI
    code_station VARCHAR(20) NOT NULL UNIQUE,
    nom_station VARCHAR(100) NOT NULL,
    ville VARCHAR(50) NOT NULL,
    zone_geo VARCHAR(50) NOT NULL,
    altitude INTEGER CHECK (altitude BETWEEN -100 AND 5000),
    capteur_type VARCHAR(20) NOT NULL CHECK (capteur_type IN ('Digital', 'Analogique')),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    date_installation DATE,
    actif BOOLEAN DEFAULT TRUE,
    
    -- Contraintes géographiques Maroc
    CHECK (latitude BETWEEN 21.0 AND 36.0),
    CHECK (longitude BETWEEN -17.0 AND -1.0)
);

CREATE INDEX idx_station_zone ON dim_station(zone_geo);
CREATE INDEX idx_station_ville ON dim_station(ville);

-- 4. DIMENSION ALERTE
CREATE TABLE dim_alerte (
    id_alerte SERIAL PRIMARY KEY,  -- CHANGÉ ICI
    type_precip VARCHAR(20) NOT NULL CHECK (type_precip IN ('Pluie', 'Grêle', 'Neige', 'Aucune')),
    severity_index VARCHAR(10) NOT NULL CHECK (severity_index IN ('RAS', 'Jaune', 'Orange', 'Rouge')),
    niveau_urgence SMALLINT NOT NULL CHECK (niveau_urgence BETWEEN 0 AND 3), -- 0=RAS, 3=Rouge
    code_couleur VARCHAR(10) NOT NULL CHECK (code_couleur IN ('GREEN', 'YELLOW', 'ORANGE', 'RED')),
    description TEXT
);

-- 5. DIMENSION REGION
CREATE TABLE dim_region (
    id_region SERIAL PRIMARY KEY,  -- CHANGÉ ICI
    zone_geo VARCHAR(50) NOT NULL UNIQUE,
    region_admin VARCHAR(100) NOT NULL,
    superficie_ha DECIMAL(12,2) CHECK (superficie_ha > 0),
    population INTEGER,
    principal_culture VARCHAR(50),
    risque_secheresse VARCHAR(20) CHECK (risque_secheresse IN ('Faible', 'Moyen', 'Élevé'))
);

-- 6. TABLE DE FAITS avec les 5 KPIs intégrés
CREATE TABLE fait_releves_climatiques (
    id_releve BIGSERIAL PRIMARY KEY,  -- CHANGÉ ICI
    id_temps INTEGER NOT NULL REFERENCES dim_temps(id_temps),
    id_station INTEGER NOT NULL REFERENCES dim_station(id_station),
    id_alerte INTEGER REFERENCES dim_alerte(id_alerte),
    id_region INTEGER NOT NULL REFERENCES dim_region(id_region),
    
    -- Mesures brutes (niveau station/jour)
    temperature_max DECIMAL(5,2) CHECK (temperature_max BETWEEN -50 AND 60),
    temperature_min DECIMAL(5,2) CHECK (temperature_min BETWEEN -50 AND 60),
    temperature_moy DECIMAL(5,2) CHECK (temperature_moy BETWEEN -50 AND 60),
    humidite_moyenne DECIMAL(5,2) CHECK (humidite_moyenne BETWEEN 0 AND 100),
    precipitations_jour DECIMAL(6,2) CHECK (precipitations_jour >= 0),
    wind_speed_max DECIMAL(6,2) CHECK (wind_speed_max >= 0),
    radiation_solaire DECIMAL(7,2) CHECK (radiation_solaire >= 0),
    
    -- KPI 1: Indice Déficit Hydrique Cumulé (30 jours)
    idhc_30j DECIMAL(8,2) CHECK (idhc_30j >= 0),
    
    -- KPI 2: Température maximale journalière (déjà dans temperature_max)
    -- Ajout d'un flag pour alerte température
    flag_alerte_temperature BOOLEAN DEFAULT FALSE,
    
    -- KPI 3: Jours consécutifs sans pluie
    jours_sans_pluie INTEGER CHECK (jours_sans_pluie >= 0),
    
    -- KPI 5: Score risque composite (0-100)
    score_risque DECIMAL(5,2) CHECK (score_risque BETWEEN 0 AND 100),
    niveau_stress_hydrique VARCHAR(20) CHECK (niveau_stress_hydrique IN ('Normal', 'Modéré', 'Élevé', 'Critique')),
    
    -- Métadonnées
    qualite_donnee VARCHAR(20) CHECK (qualite_donnee IN ('Excellente', 'Bonne', 'Moyenne', 'Faible')),
    source_donnee VARCHAR(30) NOT NULL,
    date_chargement TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    date_maj TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes de cohérence
    CHECK (temperature_max >= temperature_min),
    CHECK (temperature_moy BETWEEN temperature_min AND temperature_max)
);

-- 7. INDEX pour performances optimales (PG16)
CREATE INDEX idx_fait_temps ON fait_releves_climatiques(id_temps);
CREATE INDEX idx_fait_station ON fait_releves_climatiques(id_station);
CREATE INDEX idx_fait_region ON fait_releves_climatiques(id_region);
CREATE INDEX idx_fait_date_temperature ON fait_releves_climatiques(id_temps, temperature_max DESC);
CREATE INDEX idx_fait_idhc ON fait_releves_climatiques(idhc_30j DESC);
CREATE INDEX idx_fait_score_risque ON fait_releves_climatiques(score_risque DESC);

-- 8. VUE MATERIALISÉE pour le dashboard (PG16) - VERSION CORRIGÉE
CREATE MATERIALIZED VIEW mv_dashboard_kpis
TABLESPACE pg_default
AS
SELECT 
    t.date_complete,
    t.annee,
    t.mois,
    t.saison,
    s.nom_station,
    s.ville,
    s.zone_geo,
    r.region_admin,
    f.temperature_max,
    f.temperature_moy,
    f.humidite_moyenne,
    f.precipitations_jour,
    f.idhc_30j,
    
    -- KPI 1: Catégorie IDHC
    CASE 
        WHEN f.idhc_30j < 50 THEN 'Normal'
        WHEN f.idhc_30j BETWEEN 50 AND 100 THEN 'Modéré'
        ELSE 'Critique'
    END as categorie_idhc,
    
    -- KPI 2: Alerte température
    CASE 
        WHEN f.temperature_max > 45 THEN 'Extrême'
        WHEN f.temperature_max > 40 THEN 'Alerte'
        ELSE 'Normal'
    END as statut_temperature,
    
    -- KPI 3: Sévérité sécheresse
    CASE 
        WHEN f.jours_sans_pluie > 20 THEN 'Sécheresse sévère'
        WHEN f.jours_sans_pluie > 10 THEN 'Sécheresse modérée'
        ELSE 'Normal'
    END as statut_secheresse,
    
    -- KPI 4: Info alerte
    a.severity_index,
    a.code_couleur,
    
    -- KPI 5: Score risque
    f.score_risque,
    CASE 
        WHEN f.score_risque < 25 THEN 'Faible'
        WHEN f.score_risque < 50 THEN 'Modéré'
        WHEN f.score_risque < 75 THEN 'Élevé'
        ELSE 'Critique'
    END as categorie_risque,
    
    f.niveau_stress_hydrique,
    f.qualite_donnee
    
FROM fait_releves_climatiques f
JOIN dim_temps t ON f.id_temps = t.id_temps
JOIN dim_station s ON f.id_station = s.id_station
JOIN dim_region r ON f.id_region = r.id_region
LEFT JOIN dim_alerte a ON f.id_alerte = a.id_alerte
WHERE 1=1;  -- Inclut toutes les données (corrigé pour Docker)

CREATE UNIQUE INDEX idx_mv_dashboard ON mv_dashboard_kpis(date_complete, nom_station);

-- 9. VUE pour les alertes urgentes
CREATE VIEW v_alertes_urgentes AS
SELECT 
    s.nom_station,
    s.ville,
    s.zone_geo,
    t.date_complete,
    f.temperature_max,
    f.idhc_30j,
    f.jours_sans_pluie,
    f.score_risque,
    a.severity_index,
    a.code_couleur,
    CASE 
        WHEN f.temperature_max > 40 OR f.idhc_30j > 100 OR f.score_risque > 75 THEN 'URGENT'
        ELSE 'SURVEILLANCE'
    END as priorite
FROM fait_releves_climatiques f
JOIN dim_temps t ON f.id_temps = t.id_temps
JOIN dim_station s ON f.id_station = s.id_station
LEFT JOIN dim_alerte a ON f.id_alerte = a.id_alerte
WHERE 1=1  -- Corrigé pour inclure toutes les données
ORDER BY priorite DESC, f.score_risque DESC;

-- 10. Insertion des données de référence
INSERT INTO dim_region (zone_geo, region_admin, superficie_ha, risque_secheresse) VALUES
('Haouz', 'Marrakech-Safi', 1200000, 'Élevé'),
('Gharb', 'Rabat-Salé-Kénitra', 800000, 'Moyen'),
('Souss-Massa', 'Souss-Massa', 700000, 'Élevé'),
('Oriental', 'Oriental', 900000, 'Moyen'),
('Tadla', 'Béni Mellal-Khénifra', 600000, 'Moyen');

INSERT INTO dim_alerte (type_precip, severity_index, niveau_urgence, code_couleur) VALUES
('Aucune', 'RAS', 0, 'GREEN'),
('Pluie', 'Jaune', 1, 'YELLOW'),
('Grêle', 'Orange', 2, 'ORANGE'),
('Pluie', 'Rouge', 3, 'RED'),
('Neige', 'Orange', 2, 'ORANGE');

-- 11. Fonction pour rafraîchir la vue matérialisée (PG16)
CREATE OR REPLACE FUNCTION refresh_dashboard_mv()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dashboard_kpis;
    RAISE NOTICE 'Vue matérialisée rafraîchie: %', CURRENT_TIMESTAMP;
END;
$$;

-- 12. Configuration des statistiques (PG16) - VERSION CORRIGÉE
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET track_io_timing = on;

-- Message de confirmation
DO $$ 
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ DATA WAREHOUSE AGRICULTURE INITIALISÉ';
    RAISE NOTICE '   PostgreSQL 16.11 (alpine)';
    RAISE NOTICE '   Base: agriculture_dw';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables créées:';
    RAISE NOTICE '   - dim_temps (optimisée temporelle)';
    RAISE NOTICE '   - dim_station (stations géoréférencées)';
    RAISE NOTICE '   - dim_alerte (5 niveaux d''alerte)';
    RAISE NOTICE '   - dim_region (5 régions agricoles)';
    RAISE NOTICE '   - fait_releves_climatiques (avec 5 KPIs)';
    RAISE NOTICE 'Vues créées:';
    RAISE NOTICE '   - mv_dashboard_kpis (matérialisée)';
    RAISE NOTICE '   - v_alertes_urgentes';
    RAISE NOTICE 'Fonction: refresh_dashboard_mv()';
    RAISE NOTICE '========================================';
END $$;