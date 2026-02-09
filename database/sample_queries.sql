-- REQUÊTES D'ANALYSE POUR LES 5 KPIs

-- 1. KPI IDHC par zone géographique
SELECT 
    s.zone_geo,
    ROUND(AVG(f.idhc_30j), 1) as idhc_moyen,
    COUNT(CASE WHEN f.idhc_30j < 50 THEN 1 END) as "Normal",
    COUNT(CASE WHEN f.idhc_30j BETWEEN 50 AND 100 THEN 1 END) as "Modéré",
    COUNT(CASE WHEN f.idhc_30j > 100 THEN 1 END) as "Critique",
    COUNT(*) as total_stations
FROM fait_releves_climatiques f
JOIN dim_station s ON f.id_station = s.id_station
JOIN dim_temps t ON f.id_temps = t.id_temps
WHERE t.date_complete >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.zone_geo
ORDER BY idhc_moyen DESC;

-- 2. KPI Températures extrêmes
SELECT 
    s.nom_station,
    s.ville,
    s.zone_geo,
    MAX(f.temperature_max) as record_chaleur,
    COUNT(CASE WHEN f.temperature_max > 40 THEN 1 END) as jours_alerte,
    COUNT(CASE WHEN f.temperature_max > 45 THEN 1 END) as jours_extreme,
    ROUND(AVG(f.temperature_max), 1) as moyenne
FROM fait_releves_climatiques f
JOIN dim_station s ON f.id_station = s.id_station
GROUP BY s.nom_station, s.ville, s.zone_geo
HAVING MAX(f.temperature_max) > 40
ORDER BY record_chaleur DESC;

-- 3. KPI Sécheresse (jours sans pluie)
SELECT 
    s.zone_geo,
    ROUND(AVG(f.jours_sans_pluie), 1) as jours_secs_moyens,
    COUNT(CASE WHEN f.jours_sans_pluie > 10 THEN 1 END) as vigilance,
    COUNT(CASE WHEN f.jours_sans_pluie > 20 THEN 1 END) as alerte_secheresse,
    MAX(f.jours_sans_pluie) as record_secheresse
FROM fait_releves_climatiques f
JOIN dim_station s ON f.id_station = s.id_station
GROUP BY s.zone_geo
ORDER BY jours_secs_moyens DESC;

-- 4. Vue Dashboard complète
SELECT * FROM mv_dashboard_kpis
WHERE date_complete >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY zone_geo, date_complete DESC;

-- 5. Alertes urgentes pour intervention
SELECT * FROM v_alertes_urgentes
WHERE priorite = 'URGENT'
ORDER BY score_risque DESC;