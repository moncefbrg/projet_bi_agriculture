#!/bin/bash
echo "🔍 Vérification PostgreSQL 16 - Agriculture Résilience 2030"
echo "=========================================================="

echo "1. Version PostgreSQL:"
docker exec agriculture_dw psql -U bi_user -d agriculture_dw -c "SELECT version();" | head -3

echo ""
echo "2. Base de données:"
docker exec agriculture_dw psql -U bi_user -d agriculture_dw -c "\l agriculture_dw"

echo ""
echo "3. Tables créées:"
docker exec agriculture_dw psql -U bi_user -d agriculture_dw -c "\dt"

echo ""
echo "4. Vérification des KPIs dans le DW:"
docker exec agriculture_dw psql -U bi_user -d agriculture_dw -c "
SELECT 
    'KPI 1 - IDHC' as indicateur,
    COUNT(*) as stations,
    ROUND(AVG(idhc_30j), 1) as moyenne,
    COUNT(CASE WHEN idhc_30j > 100 THEN 1 END) as critiques
FROM fait_releves_climatiques
UNION ALL
SELECT 
    'KPI 2 - Température' as indicateur,
    COUNT(*) as stations,
    ROUND(AVG(temperature_max), 1) as moyenne,
    COUNT(CASE WHEN temperature_max > 40 THEN 1 END) as alertes
FROM fait_releves_climatiques
UNION ALL
SELECT 
    'KPI 5 - Score Risque' as indicateur,
    COUNT(*) as stations,
    ROUND(AVG(score_risque), 1) as moyenne,
    COUNT(CASE WHEN score_risque > 75 THEN 1 END) as critiques
FROM fait_releves_climatiques;
"

echo ""
echo "✅ PostgreSQL 16 opérationnel avec le schéma Agriculture!"