"""
================================================================================
  ImmoVision 360 - Phase 2 : Transform
  Script : 05_transform.py
  Auteur : Siradiou
  Mission : Nettoyer filtered_elysee.csv (règles du README_DATAPROFIILING.md)
            et l'enrichir avec 2 nouvelles colonnes générées par Gemini :
            - standardization_score  (analyse des images .jpg)
            - neighborhood_impact    (analyse des textes .txt)
            Modèle : gemini-2.5-flash-preview-04-17
            Résultat : data/processed/transformed_elysee.csv
================================================================================
"""

# ==============================================================================
# PARTIE 0 — IMPORTS
# ==============================================================================

import os
import time
import pandas as pd
import google.generativeai as genai
import PIL.Image
from dotenv import load_dotenv


# ==============================================================================
# PARTIE 1 — CONFIGURATION
# ==============================================================================

# Chargement sécurisé des variables depuis .env
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --- Chemins ---
INPUT_CSV = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\filtered_elysee.csv"

DIR_IMAGES = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\images"

DIR_TEXTES = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\texts"

OUTPUT_CSV = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\transformed_elysee.csv"

# --- Paramètres Gemini ---
# Gemini 2.5 Flash : meilleur raisonnement, même quota gratuit
MODEL_NAME = "gemini-2.5-flash-preview-04-17"

# Gemini 2.5 Flash : 10 req/min en gratuit → pause de 6.5s par sécurité
PAUSE_API = 6.5  # secondes

# Nombre max de caractères du texte envoyé à Gemini
MAX_TEXT_CHARS = 3000

# Nombre de tentatives en cas d'erreur API avant d'abandonner
MAX_RETRIES = 3


# ==============================================================================
# PARTIE 2 — INITIALISATION DE GEMINI
# ==============================================================================

def init_gemini(api_key: str):
    """
    Configure et retourne le modèle Gemini 2.5 Flash.
    Lève une erreur claire si la clé API est absente du .env.
    """
    if not api_key:
        raise ValueError(
            "[✗] GEMINI_API_KEY introuvable dans le fichier .env\n"
            "    Vérifiez que .env contient : GEMINI_API_KEY=AIzaSy..."
        )
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(MODEL_NAME)
    print(f"[✓] Modèle Gemini initialisé : {MODEL_NAME}")
    return model


# ==============================================================================
# PARTIE 3 — NETTOYAGE DU DATAFRAME
# Application des règles définies dans README_DATAPROFIILING.md
#
# RÈGLES APPLIQUÉES :
# 1. Drop des lignes avec price = 0 ou NaN
# 2. Drop minimum_nights = 365 (hors scope)
# 3. Imputation reviews_per_month → 0 (logique métier)
# 4. Imputation médiane pour les scores de notes
# 5. Imputation médiane pour host_response_rate et host_acceptance_rate
# 6. Imputation 'unknown' pour host_response_time
# 7. Imputation False pour host_is_superhost
# 8. Encodage ordinal de host_response_time
# 9. Conversion des dates en ancienneté (jours)
# ==============================================================================

def nettoyer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique toutes les règles de nettoyage définies dans le profiling.
    Retourne un DataFrame propre, prêt pour l'enrichissement IA.
    """
    df = df.copy()
    nb_initial = len(df)
    print(f"\n[→] Nettoyage — {nb_initial} lignes en entrée")

    # ── Règle 1 : Drop price = 0 ou NaN ──────────────────────────────────────
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[df["price"].notna() & (df["price"] > 0)]
    print(f"  [✓] Après drop price nul/NaN    : {len(df)} lignes ({nb_initial - len(df)} supprimées)")

    # ── Règle 2 : Drop minimum_nights = 365 ──────────────────────────────────
    avant = len(df)
    df = df[df["minimum_nights"] < 365]
    print(f"  [✓] Après drop min_nights=365   : {len(df)} lignes ({avant - len(df)} supprimées)")

    # ── Règle 3 : Imputation reviews_per_month → 0 ───────────────────────────
    df["reviews_per_month"] = df["reviews_per_month"].fillna(0)

    # ── Règle 4 : Imputation médiane pour les scores de notes ─────────────────
    cols_notes = [
        "review_scores_rating", "review_scores_cleanliness",
        "review_scores_checkin", "review_scores_communication",
        "review_scores_location", "review_scores_value"
    ]
    for col in cols_notes:
        if col in df.columns:
            mediane = df[col].median()
            df[col] = df[col].fillna(mediane)

    # ── Règle 5 : Imputation médiane pour les taux hôte ───────────────────────
    for col in ["host_response_rate", "host_acceptance_rate"]:
        if col in df.columns:
            mediane = df[col].median()
            df[col] = df[col].fillna(mediane)

    # ── Règle 6 : Imputation 'unknown' pour host_response_time ───────────────
    if "host_response_time" in df.columns:
        df["host_response_time"] = df["host_response_time"].fillna("unknown")

    # ── Règle 7 : Imputation False pour host_is_superhost ────────────────────
    if "host_is_superhost" in df.columns:
        df["host_is_superhost"] = df["host_is_superhost"].fillna(False)

    # ── Règle 8 : Encodage ordinal de host_response_time ─────────────────────
    # Plus le score est élevé, plus l'hôte répond vite → signal Hypothèse B
    mapping_response = {
        "within an hour"     : 4,
        "within a few hours" : 3,
        "within a day"       : 2,
        "a few days or more" : 1,
        "unknown"            : 0,
    }
    if "host_response_time" in df.columns:
        df["host_response_time_score"] = (
            df["host_response_time"].map(mapping_response).fillna(0).astype(int)
        )

    # ── Règle 9 : Conversion des dates en ancienneté (jours) ─────────────────
    aujourd_hui = pd.Timestamp("today").normalize()

    if "host_since" in df.columns:
        df["host_since"] = pd.to_datetime(df["host_since"], errors="coerce")
        df["host_anciennete_jours"] = (aujourd_hui - df["host_since"]).dt.days

    if "first_review" in df.columns:
        df["first_review"] = pd.to_datetime(df["first_review"], errors="coerce")
        df["jours_depuis_premier_avis"] = (aujourd_hui - df["first_review"]).dt.days

    if "last_review" in df.columns:
        df["last_review"] = pd.to_datetime(df["last_review"], errors="coerce")
        df["jours_depuis_dernier_avis"] = (aujourd_hui - df["last_review"]).dt.days

    print(f"  [✓] Imputations et encodages appliqués")
    print(f"[✓] Nettoyage terminé — {len(df)} lignes propres\n")
    return df


# ==============================================================================
# PARTIE 4 — FEATURE 1 : STANDARDIZATION_SCORE (Image → Gemini 2.5 Flash Vision)
#
# Gemini 2.5 Flash reçoit l'image .jpg de chaque annonce et la classe :
#   "industrialise" → déco minimaliste, style catalogue, murs blancs, IKEA → 1
#   "personnel"     → objets de vie, livres, déco hétéroclite, chaleureux  → 0
#   "autre"         → pas l'intérieur d'un logement                        → -1
#
# IDEMPOTENCE : les lignes déjà traitées (score ≠ None) sont skippées.
# ==============================================================================

PROMPT_IMAGE = """
Analyse cette photo d'appartement Airbnb et classe-la strictement dans l'une de ces catégories :

- "industrialise" : décoration minimaliste style catalogue (IKEA, murs blancs, absence d'objets personnels, look hôtelier standardisé)
- "personnel" : appartement habité avec des objets de vie (livres, photos, décoration hétéroclite, plantes, effets personnels)
- "autre" : l'image ne montre pas l'intérieur d'un logement (façade, rue, plan, cuisine vide, etc.)

Réponds UNIQUEMENT par l'un de ces trois mots exacts : industrialise, personnel, autre
"""

MAPPING_IMAGE = {
    "industrialise" : 1,
    "personnel"     : 0,
    "autre"         : -1,
}

def analyser_image(listing_id: int, model, dir_images: str) -> int:
    """
    Analyse une image avec Gemini 2.5 Flash Vision.
    Retourne un score numérique (1, 0, ou -1).
    """
    chemin_image = os.path.join(dir_images, f"{listing_id}.jpg")

    if not os.path.exists(chemin_image):
        return -1

    for tentative in range(MAX_RETRIES):
        try:
            img = PIL.Image.open(chemin_image).convert("RGB")
            response = model.generate_content([PROMPT_IMAGE, img])
            reponse_brute = response.text.strip().lower()

            for cle in MAPPING_IMAGE:
                if cle in reponse_brute:
                    return MAPPING_IMAGE[cle]

            return -1

        except Exception as e:
            if tentative < MAX_RETRIES - 1:
                print(f"    [!] Tentative {tentative+1}/{MAX_RETRIES} — ID {listing_id} : {e}")
                time.sleep(PAUSE_API * 2)
            else:
                print(f"    [✗] Abandon après {MAX_RETRIES} tentatives — ID {listing_id}")
                return -1


# ==============================================================================
# PARTIE 5 — FEATURE 2 : NEIGHBORHOOD_IMPACT (Texte → Gemini 2.5 Flash NLP)
#
# Gemini 2.5 Flash lit les commentaires fusionnés du fichier <ID>.txt :
#   "hotelise"  → boîte à clés, code, agence, check-in auto, peu de contact → 1
#   "voisinage" → rencontre hôte, conseils quartier, vie locale, chaleur     → 0
#   "neutre"    → commentaires trop courts ou non informatifs                → -1
# ==============================================================================

PROMPT_TEXTE = """
Analyse ces commentaires de voyageurs Airbnb et classe l'expérience globale dans l'une de ces catégories :

- "hotelise" : l'hôte est absent, check-in via boîte à clés ou code, communication uniquement digitale, aucune rencontre humaine, style agence professionnelle
- "voisinage" : l'hôte accueille en personne, donne des conseils de quartier, crée du lien social, appartement chaleureux avec présence humaine
- "neutre" : commentaires trop courts, trop vagues ou ne permettant pas de trancher

Réponds UNIQUEMENT par l'un de ces trois mots exacts : hotelise, voisinage, neutre

Commentaires :
"""

MAPPING_TEXTE = {
    "hotelise"  : 1,
    "voisinage" : 0,
    "neutre"    : -1,
}

def analyser_texte(listing_id: int, model, dir_textes: str) -> int:
    """
    Analyse les commentaires d'une annonce avec Gemini 2.5 Flash NLP.
    Retourne un score numérique (1, 0, ou -1).
    """
    chemin_texte = os.path.join(dir_textes, f"{listing_id}.txt")

    if not os.path.exists(chemin_texte):
        return -1

    try:
        with open(chemin_texte, "r", encoding="utf-8") as f:
            contenu = f.read()
    except Exception:
        return -1

    contenu_tronque = contenu[:MAX_TEXT_CHARS]

    if len(contenu_tronque.strip()) < 50:
        return -1

    for tentative in range(MAX_RETRIES):
        try:
            response = model.generate_content(PROMPT_TEXTE + contenu_tronque)
            reponse_brute = response.text.strip().lower()

            for cle in MAPPING_TEXTE:
                if cle in reponse_brute:
                    return MAPPING_TEXTE[cle]

            return -1

        except Exception as e:
            if tentative < MAX_RETRIES - 1:
                print(f"    [!] Tentative {tentative+1}/{MAX_RETRIES} — ID {listing_id} : {e}")
                time.sleep(PAUSE_API * 2)
            else:
                print(f"    [✗] Abandon après {MAX_RETRIES} tentatives — ID {listing_id}")
                return -1


# ==============================================================================
# PARTIE 6 — BOUCLE D'ENRICHISSEMENT PRINCIPALE
#
# Pour chaque annonce : appel image → pause → appel texte → pause
# Checkpoint toutes les 50 annonces pour ne pas perdre le travail en cas
# de coupure réseau ou d'interruption manuelle.
# Reprise automatique sur les lignes non encore traitées (score == NaN).
# ==============================================================================

def lancer_enrichissement(df: pd.DataFrame, model) -> pd.DataFrame:
    """
    Enrichit le DataFrame avec les deux scores IA.
    Supporte la reprise après interruption grâce aux checkpoints.
    """

    # Initialisation des colonnes si elles n'existent pas encore
    if "standardization_score" not in df.columns:
        df["standardization_score"] = None
    if "neighborhood_impact" not in df.columns:
        df["neighborhood_impact"] = None

    # Masque des lignes non encore traitées
    masque = df["standardization_score"].isna() | df["neighborhood_impact"].isna()
    indices_a_traiter = df[masque].index.tolist()
    total = len(indices_a_traiter)

    if total == 0:
        print("[✓] Toutes les annonces ont déjà été enrichies.")
        return df

    print(f"[→] Enrichissement IA ({MODEL_NAME}) — {total} annonces à analyser...\n")

    compteurs = {"image_ok": 0, "image_skip": 0, "texte_ok": 0, "texte_skip": 0}

    for i, idx in enumerate(indices_a_traiter):
        listing_id = int(df.at[idx, "id"])

        # ── Appel 1 : Image ──
        score_image = analyser_image(listing_id, model, DIR_IMAGES)
        df.at[idx, "standardization_score"] = score_image
        if score_image != -1:
            compteurs["image_ok"] += 1
        else:
            compteurs["image_skip"] += 1

        time.sleep(PAUSE_API)

        # ── Appel 2 : Texte ──
        score_texte = analyser_texte(listing_id, model, DIR_TEXTES)
        df.at[idx, "neighborhood_impact"] = score_texte
        if score_texte != -1:
            compteurs["texte_ok"] += 1
        else:
            compteurs["texte_skip"] += 1

        time.sleep(PAUSE_API)

        # ── Progression toutes les 10 annonces ──
        if (i + 1) % 10 == 0 or (i + 1) == total:
            print(
                f"  [{i+1}/{total}] "
                f"🖼  img : {compteurs['image_ok']} ok / {compteurs['image_skip']} skip | "
                f"📝 txt : {compteurs['texte_ok']} ok / {compteurs['texte_skip']} skip"
            )

        # ── Checkpoint toutes les 50 annonces ──
        if (i + 1) % 50 == 0:
            df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
            print(f"  [💾] Checkpoint sauvegardé ({i+1}/{total})")

    return df


# ==============================================================================
# PARTIE 7 — SAUVEGARDE FINALE ET RAPPORT
# ==============================================================================

def sauvegarder_et_rapporter(df: pd.DataFrame) -> None:
    """Sauvegarde le CSV Silver final et affiche le rapport."""

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 65)
    print("  RAPPORT TRANSFORM — 05_transform.py")
    print(f"  Modèle utilisé : {MODEL_NAME}")
    print("=" * 65)
    print(f"  Annonces dans le fichier Silver final  : {len(df)}")
    print(f"  Colonnes totales                       : {len(df.columns)}")

    if "standardization_score" in df.columns:
        sc = df["standardization_score"].value_counts()
        print(f"\n  📷 standardization_score :")
        print(f"    Industrialisé  (1)  : {sc.get(1,  0)}")
        print(f"    Personnel      (0)  : {sc.get(0,  0)}")
        print(f"    Non analysé   (-1)  : {sc.get(-1, 0)}")

    if "neighborhood_impact" in df.columns:
        ni = df["neighborhood_impact"].value_counts()
        print(f"\n  📝 neighborhood_impact :")
        print(f"    Hôtélisé   (1)  : {ni.get(1,  0)}")
        print(f"    Voisinage  (0)  : {ni.get(0,  0)}")
        print(f"    Neutre    (-1)  : {ni.get(-1, 0)}")

    print(f"\n  Fichier Silver sauvegardé :")
    print(f"  {OUTPUT_CSV}")
    print("=" * 65)
    print("  [→] Prochaine étape : 06_load.py")
    print("=" * 65 + "\n")


# ==============================================================================
# PARTIE 8 — POINT D'ENTRÉE
#
# Reprise intelligente :
# - Si transformed_elysee.csv existe → on recharge et on continue
# - Sinon → on part de filtered_elysee.csv et on nettoie d'abord
# ==============================================================================

if __name__ == "__main__":

    # Étape 1 : Initialiser Gemini 2.5 Flash
    model = init_gemini(GEMINI_API_KEY)

    # Étape 2 : Charger le DataFrame (reprise ou premier lancement)
    if os.path.exists(OUTPUT_CSV):
        print(f"[→] Reprise détectée — chargement de : {OUTPUT_CSV}")
        df = pd.read_csv(OUTPUT_CSV, low_memory=False)
        print(f"[✓] {len(df)} lignes chargées (mode reprise)")
    else:
        print(f"[→] Première exécution — chargement de : {INPUT_CSV}")
        df = pd.read_csv(INPUT_CSV, low_memory=False)
        # Nettoyage uniquement au premier lancement
        df = nettoyer(df)

    # Étape 3 : Enrichissement IA
    df = lancer_enrichissement(df, model)

    # Étape 4 : Sauvegarde finale et rapport
    sauvegarder_et_rapporter(df)