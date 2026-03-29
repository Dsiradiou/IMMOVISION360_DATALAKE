"""
================================================================================
  ImmoVision 360 - Phase 1 : Ingestion des Textes
  Script : 02_ingestion_textes.py
  Auteur : Siradiou
  Mission : Transformer reviews.csv en un corpus textuel structuré par annonce.
            Un fichier <ID>.txt par appartement de l'Élysée, contenant tous
            les commentaires fusionnés — prêt pour la Phase 2 (NLP/Sentiment).
================================================================================
"""

# ==============================================================================
# PARTIE 0 — IMPORTS
# Beaucoup moins de dépendances que le script 1 : pas de réseau, pas d'images.
# Tout se fait en lecture/écriture de fichiers locaux.
# ==============================================================================

import os           # Création de dossiers, vérification d'existence de fichiers
import re           # Expressions régulières pour nettoyer les balises HTML
import pandas as pd # Lecture et manipulation des CSV


# ==============================================================================
# PARTIE 1 — CONFIGURATION
# Tous les paramètres centralisés. Même logique que le script 1.
# ==============================================================================

# --- Chemins du projet ---
CSV_LISTINGS = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\listings.csv\listings.csv"

CSV_REVIEWS = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\reviews.csv\reviews.csv"

OUTPUT_DIR = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\texts"

# --- Paramètres de filtrage ---
QUARTIER_CIBLE = "Élysée"  # Valeur exacte confirmée par le notebook 00_data

# --- Mode d'écriture ---
# False → on skip les fichiers déjà existants (idempotence, mode normal)
# True  → on réécrit tous les fichiers même s'ils existent déjà
OVERWRITE = False


# ==============================================================================
# PARTIE 2 — PRÉPARATION DU DOSSIER DE SORTIE
# Même logique que le script 1.
# ==============================================================================

def creer_dossier_sortie(chemin: str) -> None:
    """Crée le dossier /texts/ si nécessaire."""
    os.makedirs(chemin, exist_ok=True)
    print(f"[✓] Dossier de sortie prêt : {chemin}")


# ==============================================================================
# PARTIE 3 — CHARGEMENT DES SOURCES
# On charge les deux CSV séparément.
#
# POURQUOI DEUX CSV ?
# - listings.csv → contient le quartier (neighbourhood_cleansed)
#                  On s'en sert uniquement pour savoir quels IDs sont dans l'Élysée.
# - reviews.csv  → contient les commentaires (listing_id + comments)
#                  C'est la vraie matière première de ce script.
#
# On fait ensuite une jointure entre les deux pour ne garder que les
# commentaires des annonces de l'Élysée.
# ==============================================================================

def charger_ids_elysee(csv_listings: str, quartier: str) -> set:
    """
    Lit listings.csv et retourne l'ensemble des IDs d'annonces dans l'Élysée.
    On utilise un set (ensemble) pour des recherches ultra-rapides plus tard.
    """
    print(f"\n[→] Lecture de listings.csv pour extraire les IDs Élysée...")
    df = pd.read_csv(csv_listings, low_memory=False)
    df_elysee = df[df["neighbourhood_cleansed"] == quartier]
    ids = set(df_elysee["id"].astype(int).tolist())
    print(f"[✓] {len(ids)} IDs trouvés dans '{quartier}'")
    return ids


def charger_reviews(csv_reviews: str) -> pd.DataFrame:
    """
    Lit reviews.csv en ne gardant que les colonnes utiles :
    - listing_id : pour faire la jointure avec les IDs Élysée
    - comments   : le texte brut du commentaire
    """
    print(f"[→] Lecture de reviews.csv...")
    df = pd.read_csv(
        csv_reviews,
        low_memory=False,
        usecols=["listing_id", "comments"]  # On ignore les autres colonnes (date, reviewer_name, etc.)
    )
    print(f"[✓] {len(df)} commentaires chargés au total (toute la ville de Paris)")
    return df


# ==============================================================================
# PARTIE 4 — NETTOYAGE D'UN COMMENTAIRE
# Avant d'écrire les textes, on nettoie chaque commentaire individuellement.
#
# POURQUOI CE NETTOYAGE ?
# Les commentaires Airbnb contiennent parfois des balises HTML comme <br/> ou
# &amp; qui sont des artefacts du scraping. Si on les laisse, les modèles NLP
# de la Phase 2 vont "voir" ces balises comme des mots, ce qui pollue l'analyse.
#
# RÈGLE D'OR : nettoyage LÉGER uniquement. On ne touche pas au contenu,
# on ne traduit pas, on ne corrige pas l'orthographe. Le texte brut doit rester
# aussi proche que possible de l'original pour que le NLP puisse travailler
# sur la vraie langue des voyageurs (français, anglais, russe, etc.)
# ==============================================================================

def nettoyer_commentaire(texte: str) -> str:
    """
    Applique un nettoyage minimal sur un commentaire brut :
    1. Supprime les balises HTML (<br/>, <br>, <p>, etc.)
    2. Supprime les entités HTML (&amp; → &, &nbsp; → espace, etc.)
    3. Élimine les espaces multiples et les sauts de ligne excessifs
    4. Retire les espaces en début et fin de chaîne
    """
    if not isinstance(texte, str):
        return ""

    # Étape 1 : Supprimer toutes les balises HTML avec une regex
    # <br/>, <br>, <p>, </p>, <strong>, etc. → remplacés par un espace
    texte = re.sub(r"<[^>]+>", " ", texte)

    # Étape 2 : Décoder les entités HTML courantes
    texte = texte.replace("&amp;", "&")
    texte = texte.replace("&nbsp;", " ")
    texte = texte.replace("&lt;", "<")
    texte = texte.replace("&gt;", ">")
    texte = texte.replace("&quot;", '"')
    texte = texte.replace("&#39;", "'")

    # Étape 3 : Normaliser les espaces (remplacer les doubles espaces ou \t par un seul)
    texte = re.sub(r"[ \t]+", " ", texte)

    # Étape 4 : Normaliser les sauts de ligne (maximum 2 consécutifs)
    texte = re.sub(r"\n{3,}", "\n\n", texte)

    # Étape 5 : Retirer les espaces en bords de chaîne
    return texte.strip()


# ==============================================================================
# PARTIE 5 — ÉCRITURE D'UN FICHIER TEXTE (unité de base)
# Cette fonction gère UN seul listing_id à la fois.
# Elle reçoit la liste de ses commentaires déjà nettoyés et les écrit dans
# un fichier <ID>.txt avec un en-tête lisible.
#
# FORMAT DU FICHIER :
# ==========================================
# Commentaires pour l'annonce 785412
# Nombre de commentaires : 47
# Quartier : Élysée
# ==========================================
#
# [1] Premier commentaire nettoyé...
#
# [2] Deuxième commentaire nettoyé...
# ==============================================================================

def ecrire_fichier_texte(
    listing_id: int,
    commentaires: list,
    output_dir: str,
    overwrite: bool
) -> str:
    """
    Écrit le fichier <listing_id>.txt dans output_dir.

    Retourne :
        "ok"     → fichier créé avec succès
        "skip"   → fichier déjà existant et overwrite=False
        "vide"   → aucun commentaire à écrire pour cet ID
        "erreur" → exception lors de l'écriture
    """
    chemin_fichier = os.path.join(output_dir, f"{listing_id}.txt")

    # --- IDEMPOTENCE ---
    # Si le fichier existe et qu'on n'est pas en mode overwrite → on skip
    if os.path.exists(chemin_fichier) and not overwrite:
        return "skip"

    # --- VÉRIFICATION : au moins un commentaire non vide ---
    commentaires_valides = [c for c in commentaires if c.strip()]
    if not commentaires_valides:
        return "vide"

    try:
        with open(chemin_fichier, "w", encoding="utf-8") as f:

            # En-tête du fichier
            f.write("=" * 50 + "\n")
            f.write(f"Commentaires pour l'annonce {listing_id}\n")
            f.write(f"Nombre de commentaires : {len(commentaires_valides)}\n")
            f.write(f"Quartier : {QUARTIER_CIBLE}\n")
            f.write("=" * 50 + "\n\n")

            # Corps : un commentaire numéroté par bloc
            for i, commentaire in enumerate(commentaires_valides, start=1):
                f.write(f"[{i}] {commentaire}\n\n")

        return "ok"

    except Exception as e:
        print(f"  [!] Erreur écriture ID {listing_id} : {e}")
        return "erreur"


# ==============================================================================
# PARTIE 6 — BOUCLE D'INGESTION PRINCIPALE
# On regroupe les commentaires par listing_id (groupby), puis on appelle
# la fonction d'écriture pour chaque annonce de l'Élysée.
#
# POURQUOI groupby ?
# reviews.csv a UNE LIGNE PAR COMMENTAIRE. Un appartement avec 50 avis
# a donc 50 lignes dans le CSV. On doit les fusionner en un seul fichier.
# C'est exactement ce que fait pandas groupby + agg.
# ==============================================================================

def lancer_ingestion_textes(
    df_reviews: pd.DataFrame,
    ids_elysee: set,
    output_dir: str,
    overwrite: bool
) -> dict:
    """
    Filtre les reviews sur les IDs Élysée, regroupe par annonce,
    et écrit un fichier .txt par annonce.
    """

    # Étape 1 : Filtrer uniquement les commentaires des annonces de l'Élysée
    print(f"\n[→] Filtrage des commentaires sur les {len(ids_elysee)} IDs Élysée...")
    df_elysee = df_reviews[df_reviews["listing_id"].isin(ids_elysee)].copy()
    print(f"[✓] {len(df_elysee)} commentaires concernent l'Élysée")

    # Étape 2 : Nettoyer tous les commentaires
    print("[→] Nettoyage des commentaires...")
    df_elysee["comments"] = df_elysee["comments"].apply(nettoyer_commentaire)

    # Étape 3 : Regrouper tous les commentaires d'un même listing_id
    # groupby crée un groupe par ID, agg("list") rassemble les commentaires en liste
    print("[→] Regroupement par annonce (groupby)...")
    groupes = df_elysee.groupby("listing_id")["comments"].agg(list)
    # groupes est une Series : index = listing_id, valeur = liste de commentaires

    total = len(groupes)
    compteurs = {"ok": 0, "skip": 0, "vide": 0, "erreur": 0}

    print(f"[→] Écriture des fichiers texte — {total} annonces à traiter...\n")

    # Étape 4 : Écrire un fichier par annonce
    for i, (listing_id, commentaires) in enumerate(groupes.items()):
        statut = ecrire_fichier_texte(
            int(listing_id),
            commentaires,
            output_dir,
            overwrite
        )
        compteurs[statut] += 1

        # Progression toutes les 200 annonces
        if (i + 1) % 200 == 0 or (i + 1) == total:
            print(
                f"  [{i+1}/{total}] "
                f"✓ {compteurs['ok']} écrits | "
                f"↷ {compteurs['skip']} existants | "
                f"∅ {compteurs['vide']} vides | "
                f"✗ {compteurs['erreur']} erreurs"
            )

    return compteurs, total


# ==============================================================================
# PARTIE 7 — RAPPORT FINAL
# Même logique que le script 1 : un bilan clair à copier dans le README.md
# ==============================================================================

def afficher_rapport(compteurs: dict, total: int) -> None:
    """Affiche le bilan de l'ingestion texte dans le terminal."""
    reussis = compteurs["ok"] + compteurs["skip"]
    taux = (reussis / total * 100) if total > 0 else 0

    print("\n" + "=" * 60)
    print("  RAPPORT D'INGESTION TEXTES — ImmoVision 360 / Élysée")
    print("=" * 60)
    print(f"  Annonces avec commentaires dans l'Élysée : {total}")
    print(f"  Fichiers .txt créés (nouveaux)           : {compteurs['ok']}")
    print(f"  Fichiers déjà présents (skip)            : {compteurs['skip']}")
    print(f"  Annonces sans commentaires (vides)       : {compteurs['vide']}")
    print(f"  Erreurs d'écriture                       : {compteurs['erreur']}")
    print(f"  Taux de complétion                       : {taux:.1f}%")
    print("=" * 60)
    print("  [✓] Ingestion textes terminée.")
    print("      Les fichiers .txt sont prêts pour le Script 3 (Sanity Check)")
    print("      et la Phase 2 (NLP / Analyse de sentiment).")
    print("=" * 60 + "\n")


# ==============================================================================
# PARTIE 8 — POINT D'ENTRÉE
# Même structure que le script 1 : on orchestre tout ici dans l'ordre logique.
# ==============================================================================

if __name__ == "__main__":

    # Étape 1 : Préparer le dossier de sortie
    creer_dossier_sortie(OUTPUT_DIR)

    # Étape 2 : Charger les IDs de l'Élysée depuis listings.csv
    ids_elysee = charger_ids_elysee(CSV_LISTINGS, QUARTIER_CIBLE)

    # Étape 3 : Charger tous les commentaires depuis reviews.csv
    df_reviews = charger_reviews(CSV_REVIEWS)

    # Étape 4 : Lancer l'ingestion (filtrage + nettoyage + écriture)
    stats, total = lancer_ingestion_textes(df_reviews, ids_elysee, OUTPUT_DIR, OVERWRITE)

    # Étape 5 : Afficher le rapport final
    afficher_rapport(stats, total)