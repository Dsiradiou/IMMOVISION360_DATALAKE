"""
================================================================================
  ImmoVision 360 - Phase 2 : Load
  Script : 06_load.py
  Auteur : Siradiou
  Mission : Charger le fichier Silver transformé (transformed_elysee.csv)
            dans une table PostgreSQL structurée et typée.
            Table cible : elysee_listings_silver
            Base de données : immovision_db (configurée dans .env)
================================================================================
"""

# ==============================================================================
# PARTIE 0 — IMPORTS
# ==============================================================================

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv


# ==============================================================================
# PARTIE 1 — CONFIGURATION
# ==============================================================================

# Chargement sécurisé des credentials depuis .env
# Le fichier .env doit contenir :
#   DB_USER=postgres
#   DB_PASSWORD=ton_mot_de_passe
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_NAME=immovision_db
load_dotenv()

DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "immovision_db")

# --- Chemins ---
INPUT_CSV = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\transformed_elysee.csv"

# --- Table cible ---
TABLE_NAME = "elysee_listings_silver"


# ==============================================================================
# PARTIE 2 — CONNEXION À POSTGRESQL
#
# On utilise SQLAlchemy comme pont entre Pandas et PostgreSQL.
# La chaîne de connexion suit le format :
#   postgresql://user:password@host:port/database
#
# POURQUOI SQLAlchemy et pas psycopg2 directement ?
# Pandas.to_sql() nécessite un objet "engine" SQLAlchemy.
# SQLAlchemy gère aussi automatiquement le pool de connexions
# et la gestion des transactions.
# ==============================================================================

def creer_engine(user, password, host, port, dbname):
    """
    Crée et teste la connexion au serveur PostgreSQL.
    Lève une erreur claire si la connexion échoue.
    """
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    print(f"[→] Connexion à PostgreSQL...")
    print(f"    Host : {host}:{port}")
    print(f"    Base : {dbname}")
    print(f"    User : {user}")

    try:
        engine = create_engine(url)
        # Test de connexion réel avec une requête simple
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"[✓] Connexion établie avec succès\n")
        return engine

    except OperationalError as e:
        print(f"\n[✗] Échec de connexion à PostgreSQL :")
        print(f"    {e}")
        print(f"\n  Vérifiez que :")
        print(f"    1. PostgreSQL tourne bien (pgAdmin4 ouvert)")
        print(f"    2. La base '{dbname}' existe")
        print(f"    3. Les credentials dans .env sont corrects")
        raise


# ==============================================================================
# PARTIE 3 — CHARGEMENT DU CSV SILVER
# ==============================================================================

def charger_csv(input_path: str) -> pd.DataFrame:
    """
    Charge transformed_elysee.csv et affiche un aperçu.
    """
    print(f"[→] Chargement du fichier Silver : {input_path}")

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"[✗] Fichier introuvable : {input_path}\n"
            f"    Vérifiez que 05_transform.py a bien été exécuté."
        )

    df = pd.read_csv(input_path, low_memory=False)
    print(f"[✓] {len(df)} lignes chargées — {len(df.columns)} colonnes")
    return df


# ==============================================================================
# PARTIE 4 — TYPAGE EXPLICITE DES COLONNES
#
# Avant l'injection, on force les types Python/Pandas corrects.
# Sans cette étape, SQLAlchemy peut inférer des types incorrects
# (ex: une colonne booléenne stockée comme TEXT dans PostgreSQL).
#
# MAPPING DES TYPES :
#   Pandas bool   → PostgreSQL BOOLEAN
#   Pandas int64  → PostgreSQL BIGINT
#   Pandas float64→ PostgreSQL DOUBLE PRECISION
#   Pandas object → PostgreSQL TEXT
# ==============================================================================

def typer_colonnes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique le typage explicite des colonnes pour garantir
    des types PostgreSQL corrects après l'injection.
    """
    df = df.copy()

    # --- Colonnes booléennes ---
    cols_bool = [
        "host_is_superhost", "host_has_profile_pic",
        "host_identity_verified", "instant_bookable"
    ]
    for col in cols_bool:
        if col in df.columns:
            # Gérer les cas où la colonne est stockée comme string "True"/"False"
            df[col] = df[col].map(
                {True: True, False: False, "True": True, "False": False,
                 "true": True, "false": False}
            ).astype("boolean")  # type nullable de pandas (gère les NaN)

    # --- Colonnes entières ---
    cols_int = [
        "id", "host_id", "accommodates", "availability_365", "availability_90",
        "host_listings_count", "calculated_host_listings_count",
        "calculated_host_listings_count_entire_homes",
        "number_of_reviews", "number_of_reviews_ltm", "minimum_nights",
        "host_response_time_score",
        "standardization_score", "neighborhood_impact"
    ]
    for col in cols_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            # Int64 (majuscule) = entier nullable pandas (supporte les NaN)

    # --- Colonnes flottantes ---
    cols_float = [
        "price", "latitude", "longitude",
        "host_response_rate", "host_acceptance_rate",
        "review_scores_rating", "review_scores_cleanliness",
        "review_scores_checkin", "review_scores_communication",
        "review_scores_location", "review_scores_value",
        "reviews_per_month",
        "host_anciennete_jours", "jours_depuis_premier_avis",
        "jours_depuis_dernier_avis"
    ]
    for col in cols_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print("[✓] Typage des colonnes appliqué")
    return df


# ==============================================================================
# PARTIE 5 — INJECTION DANS POSTGRESQL (Le Load)
#
# pandas.to_sql() avec if_exists="replace" :
#   → Si la table n'existe pas : elle est créée automatiquement
#   → Si la table existe déjà : elle est supprimée et recréée
#
# C'est le principe d'IDEMPOTENCE :
# On peut relancer ce script autant de fois qu'on veut, le résultat
# sera toujours le même — la table contient les données les plus récentes.
#
# En production avancée, on utiliserait un UPSERT pour une mise à jour
# incrémentale. Pour ce projet, le replace est le bon compromis.
# ==============================================================================

def injecter(df: pd.DataFrame, engine, table_name: str) -> None:
    """
    Injecte le DataFrame dans PostgreSQL via SQLAlchemy.
    Affiche la progression par chunks pour les grands volumes.
    """
    print(f"[→] Injection dans la table '{table_name}'...")
    print(f"    Lignes à injecter : {len(df)}")
    print(f"    Colonnes          : {len(df.columns)}")

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",    # Recrée la table à chaque exécution
            index=False,            # On n'injecte pas l'index Pandas
            chunksize=500,          # Injection par lots de 500 lignes (évite les timeouts)
            method="multi",         # INSERT multi-lignes (plus rapide)
        )
        print(f"[✓] Injection terminée avec succès")

    except Exception as e:
        print(f"[✗] Erreur lors de l'injection : {e}")
        raise


# ==============================================================================
# PARTIE 6 — VÉRIFICATION POST-INJECTION
#
# Après le chargement, on interroge directement PostgreSQL pour vérifier
# que les données sont bien en place. C'est la preuve concrète que le
# Data Warehouse est opérationnel — c'est aussi ce qu'on capture en
# screenshot pour le README.md final.
# ==============================================================================

def verifier(engine, table_name: str, nb_lignes_attendu: int) -> None:
    """
    Interroge PostgreSQL pour confirmer que la table est bien peuplée.
    Affiche les statistiques clés directement depuis la base.
    """
    print(f"\n[→] Vérification post-injection dans PostgreSQL...")

    with engine.connect() as conn:

        # Comptage des lignes
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        nb_lignes = result.scalar()

        # Prix moyen
        result_prix = conn.execute(
            text(f"SELECT ROUND(AVG(price)::numeric, 2) FROM {table_name} WHERE price > 0")
        )
        prix_moyen = result_prix.scalar()

        # Distribution standardization_score
        result_std = conn.execute(text(f"""
            SELECT standardization_score, COUNT(*) as nb
            FROM {table_name}
            GROUP BY standardization_score
            ORDER BY standardization_score DESC
        """))
        dist_std = result_std.fetchall()

        # Distribution neighborhood_impact
        result_ni = conn.execute(text(f"""
            SELECT neighborhood_impact, COUNT(*) as nb
            FROM {table_name}
            GROUP BY neighborhood_impact
            ORDER BY neighborhood_impact DESC
        """))
        dist_ni = result_ni.fetchall()

        # Top 5 hôtes multi-propriétaires
        result_multi = conn.execute(text(f"""
            SELECT host_name, calculated_host_listings_count
            FROM {table_name}
            ORDER BY calculated_host_listings_count DESC
            LIMIT 5
        """))
        top_multi = result_multi.fetchall()

    print("\n" + "=" * 65)
    print("  RAPPORT LOAD — 06_load.py")
    print("=" * 65)
    print(f"  Table PostgreSQL        : {table_name}")
    print(f"  Base de données         : {DB_NAME}")
    print(f"  Lignes attendues        : {nb_lignes_attendu}")
    print(f"  Lignes dans PostgreSQL  : {nb_lignes}")
    statut = "✓ COHÉRENT" if nb_lignes == nb_lignes_attendu else "⚠ ÉCART DÉTECTÉ"
    print(f"  Cohérence               : {statut}")
    print(f"  Prix moyen (€/nuit)     : {prix_moyen} €")

    print(f"\n  📷 Distribution standardization_score :")
    labels_std = {1: "Industrialisé", 0: "Personnel", -1: "Non analysé"}
    for score, nb in dist_std:
        print(f"    {labels_std.get(score, '?')} ({score}) : {nb}")

    print(f"\n  📝 Distribution neighborhood_impact :")
    labels_ni = {1: "Hôtélisé", 0: "Voisinage", -1: "Neutre"}
    for score, nb in dist_ni:
        print(f"    {labels_ni.get(score, '?')} ({score}) : {nb}")

    print(f"\n  🏠 Top 5 hôtes multi-annonces :")
    for nom, nb in top_multi:
        print(f"    {nom:<30} : {nb} annonces")

    print("=" * 65)
    print("  [✓] Data Warehouse opérationnel.")
    print("  [→] Capturez ce rapport pour votre README.md (screenshot pgAdmin)")
    print("=" * 65 + "\n")


# ==============================================================================
# PARTIE 7 — POINT D'ENTRÉE
# ==============================================================================

if __name__ == "__main__":

    # Étape 1 : Créer la connexion PostgreSQL
    engine = creer_engine(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    # Étape 2 : Charger le CSV Silver
    df = charger_csv(INPUT_CSV)

    # Étape 3 : Typer les colonnes correctement
    df = typer_colonnes(df)

    # Étape 4 : Injecter dans PostgreSQL
    injecter(df, engine, TABLE_NAME)

    # Étape 5 : Vérification post-injection
    verifier(engine, TABLE_NAME, len(df))