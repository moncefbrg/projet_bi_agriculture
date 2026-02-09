# 🌾 PROJET BI - AGRICULTURE RÉSILIENCE 2030

## 📋 Description
Preuve de Concept (PoC) d'une plateforme décisionnelle pour le Ministère de l'Agriculture et du Développement Durable du Maroc.

## 🎯 KPIs Prioritaires
1. **IDHC** - Indice Déficit Hydrique Cumulé (30 jours)
2. **Température max** journalière (>40°C alerte)
3. **Jours consécutifs** sans pluie
4. **Fréquence alertes** météo
5. **Score risque** station composite

## 🚀 Installation Rapide

### Prérequis
- Docker Desktop
- 4GB RAM minimum
- 2GB espace disque

### 1. Démarrer l'environnement
# 1.1. 
cd database
# 1.2. Démarrez
docker-compose up -d

# 1.3. Attendez que PostgreSQL soit prêt
sleep 10

# 1.4. Exécutez l'ETL
docker exec python_etl python /etl/etl_pipeline.py

# 1.5. Vérifiez
docker exec agriculture_dw psql -U bi_user -d agriculture_dw -c "
SELECT COUNT(*) FROM mv_dashboard_kpis;
SELECT date_complete, nom_station, temperature_max, score_risque, categorie_risque 
FROM mv_dashboard_kpis 
ORDER BY date_complete DESC, nom_station 
LIMIT 10;
"