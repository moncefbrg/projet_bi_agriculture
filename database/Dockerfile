# Utilise l'image slim (Debian) : compromis parfait entre taille et compatibilité
FROM python:3.9-slim

# Installation des dépendances système
# libpq-dev est nécessaire pour communiquer avec PostgreSQL
# gcc est nécessaire pour compiler certains outils si besoin
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Définit le répertoire de travail
WORKDIR /etl

# Copie le fichier requirements.txt
# (Le contexte reste la racine du projet via le docker-compose)
COPY etl/requirements.txt ./

# Installe les dépendances Python
# Grâce à l'image slim, pip va télécharger les versions pré-compilées (plus rapide)
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copie le contenu du dossier etl
COPY etl/ .

# Garde le conteneur actif
CMD ["sleep", "infinity"]