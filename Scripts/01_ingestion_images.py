"""
================================================================================
  ImmoVision 360 - Phase 1 : Ingestion des Images
  Script : 01_ingestion_images.py
  Auteur : Siradiou
  Mission : Télécharger les photos des appartements du quartier Élysée
            depuis listings.csv et les stocker dans le Data Lake local.
================================================================================
"""

# ==============================================================================
# PARTIE 0 — IMPORTS
# Les bibliothèques dont on a besoin pour faire fonctionner le script.
# ==============================================================================

import os           # Pour créer des dossiers, vérifier si un fichier existe
import time         # Pour faire une pause entre chaque téléchargement (rate limiting)
import requests     # Pour envoyer des requêtes HTTP et télécharger les images
import pandas as pd # Pour lire et filtrer le fichier CSV
from PIL import Image  # Pour redimensionner les images à 320x320 px
from io import BytesIO # Pour manipuler les octets d'une image en mémoire


# ==============================================================================
# PARTIE 1 — CONFIGURATION
# Tous les paramètres du script sont regroupés ici.
# Si tu veux changer un chemin ou un filtre, tu n'as qu'un seul endroit à modifier.
# ==============================================================================

# --- Chemins du projet ---
# Chemin vers le fichier listings.csv sur ta machine
CSV_PATH = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\listings.csv\listings.csv"

# Dossier de destination où les images seront sauvegardées
OUTPUT_DIR = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\images"

# --- Paramètres de filtrage ---
# Valeur exacte du quartier dans la colonne 'neighbourhood_cleansed'
QUARTIER_CIBLE = "Élysée"

# --- Paramètres d'image ---
# Taille cible pour toutes les images téléchargées (en pixels)
IMAGE_SIZE = (320, 320)

# --- Paramètres réseau (éthique & robustesse) ---
# Pause en secondes entre chaque téléchargement (respecter le serveur)
PAUSE_ENTRE_REQUETES = 0.5

# Délai maximum d'attente pour une réponse du serveur (en secondes)
TIMEOUT = 10

# User-Agent : on se présente comme un navigateur pour éviter les blocages basiques
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


# ==============================================================================
# PARTIE 2 — PRÉPARATION DU DATA LAKE
# On crée le dossier de destination s'il n'existe pas encore.
# exist_ok=True évite une erreur si le dossier existe déjà.
# ==============================================================================

def creer_dossier_sortie(chemin: str) -> None:
    """Crée le dossier de destination si nécessaire."""
    os.makedirs(chemin, exist_ok=True)
    print(f"[✓] Dossier de sortie prêt : {chemin}")


# ==============================================================================
# PARTIE 3 — LECTURE ET FILTRAGE DU CSV
# On ouvre listings.csv et on garde uniquement les annonces de l'Élysée.
# Le notebook 00_data a confirmé : la colonne est 'neighbourhood_cleansed'
# et la valeur exacte est 'Élysée' (avec accent). 2625 annonces attendues.
# ==============================================================================

def charger_annonces_elysee(csv_path: str, quartier: str) -> pd.DataFrame:
    """
    Lit le CSV et retourne uniquement les lignes correspondant au quartier cible.
    On ne garde que les colonnes utiles pour économiser de la mémoire.
    """
    print(f"\n[→] Lecture du fichier : {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)

    # Filtrage géographique : on ne garde que l'Élysée
    df_elysee = df[df["neighbourhood_cleansed"] == quartier].copy()

    # On ne conserve que les deux colonnes vitales pour ce script
    df_elysee = df_elysee[["id", "picture_url"]].dropna(subset=["picture_url"])

    print(f"[✓] Annonces trouvées dans '{quartier}' : {len(df_elysee)}")
    return df_elysee


# ==============================================================================
# PARTIE 4 — TÉLÉCHARGEMENT D'UNE IMAGE (unité de base)
# Cette fonction s'occupe d'une seule image à la fois.
# Elle est appelée en boucle dans la partie 5.
# ==============================================================================

def telecharger_image(listing_id: int, url: str, output_dir: str) -> str:
    """
    Télécharge une image depuis une URL, la redimensionne à 320x320,
    et la sauvegarde sous le nom <ID>.jpg dans output_dir.

    Retourne :
        "ok"      → succès
        "skip"    → fichier déjà présent (idempotence)
        "erreur"  → échec du téléchargement
    """

    # --- IDEMPOTENCE ---
    # Si le fichier existe déjà → on ne retélécharge pas.
    # Cela permet de relancer le script après une coupure sans tout recommencer.
    chemin_fichier = os.path.join(output_dir, f"{listing_id}.jpg")
    if os.path.exists(chemin_fichier):
        return "skip"

    try:
        # --- REQUÊTE HTTP ---
        # On envoie une requête GET avec notre User-Agent et un timeout.
        reponse = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        # Si le serveur répond avec une erreur (404, 403, 500...), on lève une exception
        reponse.raise_for_status()

        # --- TRAITEMENT DE L'IMAGE ---
        # On lit les octets reçus comme une image PIL (Pillow)
        image = Image.open(BytesIO(reponse.content))

        # Conversion en RGB pour éviter les problèmes avec PNG transparents ou images en niveaux de gris
        image = image.convert("RGB")

        # Redimensionnement à 320x320 pixels (réduit le poids, standardise pour la Phase 2 IA)
        image = image.resize(IMAGE_SIZE, Image.LANCZOS)

        # Sauvegarde dans le dossier images avec le nom = ID de l'annonce
        image.save(chemin_fichier, "JPEG", quality=85)

        return "ok"

    except requests.exceptions.Timeout:
        print(f"  [!] Timeout pour ID {listing_id}")
        return "erreur"

    except requests.exceptions.HTTPError as e:
        print(f"  [!] Erreur HTTP {e.response.status_code} pour ID {listing_id}")
        return "erreur"

    except requests.exceptions.ConnectionError:
        print(f"  [!] Connexion impossible pour ID {listing_id}")
        return "erreur"

    except Exception as e:
        # Filet de sécurité : toute autre erreur imprévue est capturée ici
        print(f"  [!] Erreur inattendue pour ID {listing_id} : {e}")
        return "erreur"


# ==============================================================================
# PARTIE 5 — BOUCLE D'INGESTION PRINCIPALE
# On parcourt toutes les annonces de l'Élysée et on télécharge chaque image.
# On comptabilise les succès, skips et erreurs pour le rapport final.
# ==============================================================================

def lancer_ingestion(df: pd.DataFrame, output_dir: str) -> dict:
    """
    Parcourt le DataFrame ligne par ligne et télécharge chaque image.
    Affiche une progression toutes les 100 images.
    Retourne un dictionnaire de statistiques.
    """
    total = len(df)
    compteurs = {"ok": 0, "skip": 0, "erreur": 0, "ids_erreur": []}

    print(f"\n[→] Début de l'ingestion — {total} images à traiter...\n")

    for i, (_, row) in enumerate(df.iterrows()):
        listing_id = int(row["id"])
        url = row["picture_url"]

        # Appel de la fonction de téléchargement unitaire
        statut = telecharger_image(listing_id, url, output_dir)
        compteurs[statut] += 1

        # On mémorise les IDs en erreur pour le rapport
        if statut == "erreur":
            compteurs["ids_erreur"].append(listing_id)

        # Affichage de progression toutes les 100 images
        if (i + 1) % 100 == 0 or (i + 1) == total:
            print(
                f"  [{i+1}/{total}] "
                f"✓ {compteurs['ok']} téléchargées | "
                f"↷ {compteurs['skip']} existantes | "
                f"✗ {compteurs['erreur']} erreurs"
            )

        # Pause entre chaque requête — règle de courtoisie serveur
        if statut != "skip":
            time.sleep(PAUSE_ENTRE_REQUETES)

    return compteurs


# ==============================================================================
# PARTIE 6 — RAPPORT FINAL
# On affiche un bilan lisible à la fin de l'exécution.
# Ce rapport sera copié dans le README.md comme preuve de livraison.
# ==============================================================================

def afficher_rapport(compteurs: dict, total: int) -> None:
    """Affiche un bilan complet de l'ingestion dans le terminal."""
    reussis = compteurs["ok"] + compteurs["skip"]
    taux = (reussis / total * 100) if total > 0 else 0

    print("\n" + "=" * 60)
    print("  RAPPORT D'INGESTION — ImmoVision 360 / Élysée")
    print("=" * 60)
    print(f"  Total annonces dans le CSV     : {total}")
    print(f"  Images téléchargées (nouvelles): {compteurs['ok']}")
    print(f"  Images déjà présentes (skip)   : {compteurs['skip']}")
    print(f"  Erreurs de téléchargement      : {compteurs['erreur']}")
    print(f"  Taux de complétion             : {taux:.1f}%")

    if compteurs["ids_erreur"]:
        print(f"\n  Premiers IDs en erreur :")
        for eid in compteurs["ids_erreur"][:5]:
            print(f"    → {eid}")

    print("=" * 60)
    print("  [✓] Ingestion terminée. Données prêtes pour le Script 3 (Sanity Check).")
    print("=" * 60 + "\n")


# ==============================================================================
# PARTIE 7 — POINT D'ENTRÉE DU SCRIPT
# Ce bloc s'exécute quand on lance : python 01_ingestion_images.py
# Il orchestre l'appel de toutes les fonctions dans le bon ordre.
# ==============================================================================

if __name__ == "__main__":

    # Étape 1 : Préparer le dossier de sortie
    creer_dossier_sortie(OUTPUT_DIR)

    # Étape 2 : Charger et filtrer le CSV sur l'Élysée
    df_elysee = charger_annonces_elysee(CSV_PATH, QUARTIER_CIBLE)

    # Étape 3 : Lancer la boucle de téléchargement
    stats = lancer_ingestion(df_elysee, OUTPUT_DIR)

    # Étape 4 : Afficher le rapport final
    afficher_rapport(stats, len(df_elysee))