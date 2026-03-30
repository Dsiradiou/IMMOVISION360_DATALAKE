"""
================================================================================
  ImmoVision 360 - Phase 2 : Extract
  Script : 04_extract.py
  Auteur : Siradiou
  Mission : Ouvrir la Zone Bronze (listings.csv.gz), sélectionner uniquement
            les colonnes utiles aux 3 hypothèses de la Maire, filtrer sur
            l'Élysée, et produire la Zone Silver intermédiaire :
            data/processed/filtered_elysee.csv
================================================================================
"""

# ==============================================================================
# PARTIE 0 — IMPORTS
# ==============================================================================

import os
import pandas as pd


# ==============================================================================
# PARTIE 1 — CONFIGURATION
# ==============================================================================

# --- Chemins ---
# Version complète avec prix (listings.csv.gz depuis data.insideairbnb.com)
CSV_SOURCE = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\listings.csv.gz"

DIR_PROCESSED = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed"

OUTPUT_CSV = os.path.join(DIR_PROCESSED, "filtered_elysee.csv")

# --- Filtre géographique ---
QUARTIER_CIBLE = "Élysée"


# ==============================================================================
# PARTIE 2 — SÉLECTION DES COLONNES (Le choix métier)
#
# Sur les 70+ colonnes de listings.csv, on ne garde que celles qui alimentent
# directement l'une des 3 hypothèses de la Maire de Paris.
#
# HYPOTHÈSE A — "Machine à Cash" (concentration des biens / industrialisation)
# HYPOTHÈSE B — "Déshumanisation" (lien social / présence humaine de l'hôte)
# HYPOTHÈSE C — "Standardisation visuelle" (score IA images — ajouté en 05_transform)
# ==============================================================================

COLS_TO_KEEP = [

    # --- IDENTIFIANTS (clés de jointure avec images et textes) ---
    "id",                           # Clé primaire → jointure avec <ID>.jpg et <ID>.txt
    "listing_url",                  # Lien de vérification manuelle si besoin

    # --- LOCALISATION ---
    "neighbourhood_cleansed",       # Quartier nettoyé → filtre Élysée
    "latitude",                     # Coordonnées GPS → cartes Phase 3
    "longitude",

    # --- DESCRIPTION DU BIEN ---
    "name",                         # Nom de l'annonce → signal qualitatif
    "property_type",                # Type de bien (appartement, loft...) → Hyp. A
    "room_type",                    # Logement entier / chambre → Hyp. A (industrialisation)
    "accommodates",                 # Capacité d'accueil → Hyp. A

    # --- PRIX (cœur de l'hypothèse économique) ---
    "price",                        # Prix par nuit → Hyp. A (hausse des prix)

    # --- DISPONIBILITÉ (proxy d'usage touristique vs résidentiel) ---
    "availability_365",             # Jours disponibles / an → Hyp. A
    "availability_90",              # Disponibilité sur 90j → Hyp. A

    # --- HÔTE : CONCENTRATION DES BIENS ---
    "host_id",                      # ID de l'hôte → Hyp. A (multi-propriétaires)
    "host_name",                    # Nom de l'hôte
    "host_since",                   # Ancienneté → distingue hôte particulier vs pro
    "host_is_superhost",            # Superhost → indicateur pro → Hyp. A
    "host_listings_count",          # Nb total annonces de l'hôte → Hyp. A
    "calculated_host_listings_count",               # Nb annonces calculé (plus fiable) → Hyp. A
    "calculated_host_listings_count_entire_homes",  # Nb logements entiers → Hyp. A

    # --- HÔTE : PRÉSENCE HUMAINE ---
    "host_response_time",           # Rapidité de réponse → Hyp. B (agence vs humain)
    "host_response_rate",           # Taux de réponse → Hyp. B
    "host_acceptance_rate",         # Taux d'acceptation → Hyp. B
    "host_identity_verified",       # Identité vérifiée → Hyp. B (confiance)
    "host_has_profile_pic",         # Photo de profil → Hyp. B (présence humaine)

    # --- AVIS ET NOTES (qualité perçue) ---
    "number_of_reviews",            # Volume d'avis → Hyp. B (activité)
    "number_of_reviews_ltm",        # Avis des 12 derniers mois → Hyp. B
    "first_review",                 # Date premier avis → ancienneté de l'annonce
    "last_review",                  # Date dernier avis → activité récente
    "review_scores_rating",         # Note globale → Hyp. B
    "review_scores_cleanliness",    # Propreté → Hyp. C (standardisation)
    "review_scores_checkin",        # Check-in → Hyp. B (boîte à clés ?)
    "review_scores_communication",  # Communication → Hyp. B
    "review_scores_location",       # Localisation → contexte géo
    "review_scores_value",          # Rapport qualité/prix → Hyp. A
    "reviews_per_month",            # Fréquence des avis → Hyp. A (rotation)

    # --- RÈGLES DE SÉJOUR ---
    "minimum_nights",               # Durée min → Hyp. A (location touristique vs bail)
    "instant_bookable",             # Résa instantanée → Hyp. A (gestion pro)

    # --- LÉGALITÉ ---
    "license",                      # Numéro d'enregistrement → Hyp. A (conformité légale)
]


# ==============================================================================
# PARTIE 3 — CHARGEMENT ET FILTRAGE
# ==============================================================================

def charger_et_filtrer(csv_path: str, quartier: str, colonnes: list) -> pd.DataFrame:
    """
    Charge listings.csv.gz, filtre sur le quartier cible,
    et ne conserve que les colonnes sélectionnées.
    """
    print(f"[→] Chargement de : {csv_path}")

    # compression="gzip" lit le .gz directement sans décompresser manuellement
    df = pd.read_csv(csv_path, compression="gzip", low_memory=False)
    print(f"[✓] CSV chargé — {len(df)} lignes, {len(df.columns)} colonnes")

    # Filtrage géographique
    df_elysee = df[df["neighbourhood_cleansed"] == quartier].copy()
    print(f"[✓] Après filtre '{quartier}' : {len(df_elysee)} annonces")

    # Vérification des colonnes disponibles
    colonnes_disponibles = [c for c in colonnes if c in df_elysee.columns]
    colonnes_absentes = [c for c in colonnes if c not in df_elysee.columns]

    if colonnes_absentes:
        print(f"\n  [!] Colonnes absentes du CSV (ignorées) :")
        for col in colonnes_absentes:
            print(f"      → {col}")

    df_filtre = df_elysee[colonnes_disponibles].copy()
    print(f"\n[✓] Colonnes conservées : {len(colonnes_disponibles)} / {len(colonnes)}")

    return df_filtre


# ==============================================================================
# PARTIE 4 — NETTOYAGE MINIMAL (pré-nettoyage avant Transform)
#
# On ne fait PAS le nettoyage complet ici — c'est le rôle du script 05.
# On fait seulement trois ajustements de FORMAT indispensables :
#
# 1. Conversion du prix : "$120.00" → 120.0
# 2. Conversion des booléens Airbnb : "t"/"f" → True/False
# 3. Conversion des taux : "97%" → 97.0
# ==============================================================================

def pre_nettoyer(df: pd.DataFrame) -> pd.DataFrame:
    """Applique les conversions de format minimales avant sauvegarde."""
    df = df.copy()

    # --- Conversion du prix : "$1,200.00" → 1200.0 ---
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .str.strip()
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        nb_prix_nuls = df["price"].isna().sum()
        if nb_prix_nuls > 0:
            print(f"  [!] {nb_prix_nuls} prix non convertibles → NaN (sera traité en 05_transform)")

    # --- Conversion des booléens Airbnb : "t"/"f" → True/False ---
    cols_bool = ["host_is_superhost", "host_has_profile_pic",
                 "host_identity_verified", "instant_bookable"]
    for col in cols_bool:
        if col in df.columns:
            df[col] = df[col].map({"t": True, "f": False})

    # --- Conversion des taux : "97%" → 97.0 ---
    for col in ["host_response_rate", "host_acceptance_rate"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print("[✓] Pré-nettoyage des formats appliqué (prix, booléens, taux)")
    return df


# ==============================================================================
# PARTIE 5 — SAUVEGARDE
# ==============================================================================

def sauvegarder(df: pd.DataFrame, output_path: str) -> None:
    """
    Crée le dossier /processed/ si nécessaire et sauvegarde le CSV filtré.
    utf-8-sig : ajoute un BOM pour que Excel ouvre correctement les accents.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[✓] Fichier Silver intermédiaire sauvegardé :")
    print(f"    {output_path}")


# ==============================================================================
# PARTIE 6 — RAPPORT D'EXTRACTION
# ==============================================================================

def afficher_rapport(df: pd.DataFrame) -> None:
    """Affiche un résumé du fichier extrait."""

    print("\n" + "=" * 60)
    print("  RAPPORT D'EXTRACTION — 04_extract.py")
    print("=" * 60)
    print(f"  Annonces extraites (Élysée)   : {len(df)}")
    print(f"  Colonnes conservées           : {len(df.columns)}")
    print(f"  Mémoire utilisée              : {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    # Aperçu des valeurs manquantes par colonne
    nan_counts = df.isna().sum()
    nan_cols = nan_counts[nan_counts > 0].sort_values(ascending=False)

    if not nan_cols.empty:
        print(f"\n  Colonnes avec valeurs manquantes (NaN) :")
        for col, count in nan_cols.items():
            pct = count / len(df) * 100
            print(f"    {col:<45} : {count:>4} ({pct:.1f}%)")
    else:
        print("  [✓] Aucune valeur manquante détectée")

    # Aperçu prix
    if "price" in df.columns:
        prix = df["price"].dropna()
        print(f"\n  Prix (€/nuit) :")
        print(f"    Min    : {prix.min():.0f} €")
        print(f"    Médiane: {prix.median():.0f} €")
        print(f"    Max    : {prix.max():.0f} €")
        print(f"    Moyenne: {prix.mean():.0f} €")

    # Aperçu multi-propriétaires
    if "calculated_host_listings_count" in df.columns:
        multi = df[df["calculated_host_listings_count"] > 1]
        print(f"\n  Hôtes multi-annonces : {len(multi)} ({len(multi)/len(df)*100:.1f}%)")

    print("=" * 60)
    print("  [→] Prochaine étape : 05_transform.py")
    print("=" * 60 + "\n")


# ==============================================================================
# PARTIE 7 — POINT D'ENTRÉE
# ==============================================================================

if __name__ == "__main__":

    # Étape 1 : Charger et filtrer
    df_filtre = charger_et_filtrer(CSV_SOURCE, QUARTIER_CIBLE, COLS_TO_KEEP)

    # Étape 2 : Pré-nettoyage des formats
    df_filtre = pre_nettoyer(df_filtre)

    # Étape 3 : Sauvegarder dans data/processed/
    sauvegarder(df_filtre, OUTPUT_CSV)

    # Étape 4 : Rapport
    afficher_rapport(df_filtre)