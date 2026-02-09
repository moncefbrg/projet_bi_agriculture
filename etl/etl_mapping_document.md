# DOCUMENTATION ETL - Agriculture Résilience 2030

## 1. Architecture du Pipeline
Sources → ETL Python → PostgreSQL 16 → Dashboard
↓ ↓ ↓ ↓
CSV/JSON Pandas DW Star Visualisation
Excel SQLAlchemy Schema


## 2. Mapping Source → Cible

| Source | Table Cible | Transformation | Validation |
|--------|-------------|----------------|------------|
| `dim_temps.csv` | `dim_temps` | Aucune, déjà formaté | Dates uniques |
| `dim_station.csv` | `dim_station` | Standardisation codes | Clés uniques |
| `dim_region.csv` | `dim_region` | Aucune | Référentiel complet |
| `fait_releves.csv` | `fait_releves_climatiques` | Vérification KPIs, nettoyage valeurs aberrantes | Intégrité référentielle |

## 3. Règles de Transformation

### KPI 1 - IDHC
- Source: Calculé dans génération
- Transformation: `idhc_30j = Σ(ET0 - précipitations) sur 30j`
- Validation: `0 ≤ idhc_30j ≤ 500`

### KPI 2 - Température
- Source: `temperature_max`
- Transformation: Détection seuils (>40°C alerte, >45°C extrême)
- Validation: `-50 ≤ temp ≤ 60`

### KPI 5 - Score Risque
- Formule: `0.4*idhc_norm + 0.3*temp_norm + 0.3*alertes_norm`
- Normalisation: 0-100
- Validation: `0 ≤ score ≤ 100`

## 4. Qualité des Données
- Valeurs aberrantes: remplacement par moyenne mobile
- Données manquantes: imputation par zone/saison
- Incohérences: log dans table d'audit

## 5. Orchestration
1. Chargement dimensions (ordre: temps → région → station)
2. Chargement faits (par lots de 5000)
3. Validation intégrité
4. Rafraîchissement vues matérialisées
5. Logging et monitoring