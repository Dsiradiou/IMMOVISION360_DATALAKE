"""
================================================================================
  ImmoVision 360 - Phase 1 : Bilan de Santé du Data Lake
  Script : 03_sanity_check.py
  Auteur : Siradiou
  Mission : Auditer le Data Lake après les scripts 1 et 2.
            Vérifier que chaque annonce Élysée a bien son image ET son texte.
            Produire un rapport complet à copier dans le README.md
================================================================================
"""

# ==============================================================================
# PARTIE 0 — IMPORTS
# ==============================================================================

import os           # Lecture du système de fichiers (listdir, exists)
import pandas as pd # Lecture du CSV pour le comptage théorique


# ==============================================================================
# PARTIE 1 — CONFIGURATION
# ==============================================================================

CSV_LISTINGS = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\listings.csv\listings.csv"

DIR_IMAGES = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\images"

DIR_TEXTES = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\texts"

QUARTIER_CIBLE = "Élysée"

# Nombre d'IDs orphelins à afficher dans le rapport (pour ne pas surcharger)
MAX_ORPHELINS_AFFICHES = 10


# ==============================================================================
# PARTIE 2 — COMPTAGE THÉORIQUE (ce qu'on ATTEND)
# On lit listings.csv et on extrait les IDs Élysée.
# C'est la "vérité terrain" : ce sont exactement ces IDs qu'on aurait dû
# télécharger en images et générer en textes.
# ==============================================================================

def charger_ids_theoriques(csv_path: str, quartier: str) -> set:
    """
    Lit listings.csv et retourne le set des IDs attendus pour l'Élysée.
    C'est notre référence : tout le reste sera comparé à ce set.
    """
    print(f"[→] Lecture du CSV source : {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    df_elysee = df[df["neighbourhood_cleansed"] == quartier]
    ids = set(df_elysee["id"].astype(int).tolist())
    print(f"[✓] IDs théoriques (attendus) : {len(ids)}\n")
    return ids


# ==============================================================================
# PARTIE 3 — COMPTAGE PHYSIQUE (ce qu'on A RÉELLEMENT)
# On lit le contenu des dossiers /images/ et /texts/ sur le disque dur.
# On extrait les IDs depuis les noms de fichiers (785412.jpg → 785412).
#
# POURQUOI EXTRAIRE L'ID DEPUIS LE NOM ?
# Parce que la règle de nommage des scripts 1 et 2 impose <ID>.jpg et <ID>.txt.
# Si le nommage a bien été respecté, on peut reconstruire l'ID depuis le nom de
# fichier et faire la comparaison avec les IDs théoriques du CSV.
# ==============================================================================

def lister_ids_physiques(dossier: str, extension: str) -> set:
    """
    Parcourt un dossier et retourne le set des IDs trouvés physiquement.
    Extension attendue : '.jpg' pour les images, '.txt' pour les textes.
    Les fichiers qui ne matchent pas le format <nombre><extension> sont ignorés.
    """
    ids = set()

    if not os.path.exists(dossier):
        print(f"  [!] Dossier introuvable : {dossier}")
        return ids

    for nom_fichier in os.listdir(dossier):
        # On vérifie que le fichier a la bonne extension
        if nom_fichier.lower().endswith(extension):
            # On extrait la partie avant l'extension et on vérifie que c'est un entier
            nom_sans_ext = os.path.splitext(nom_fichier)[0]
            if nom_sans_ext.isdigit():
                ids.add(int(nom_sans_ext))

    return ids


# ==============================================================================
# PARTIE 4 — ANALYSE DES ÉCARTS (La Jointure Physique)
# C'est le cœur du Sanity Check.
# On compare les deux sets (théorique vs physique) pour identifier :
#
# - Les ORPHELINS MANQUANTS : IDs présents dans le CSV mais absents du dossier
#   → image ou texte non téléchargé (erreur réseau, lien mort, etc.)
#
# - Les FANTÔMES : IDs présents dans le dossier mais absents du CSV
#   → fichiers en trop qui ne correspondent à aucune annonce connue
#   (peut arriver si on a téléchargé des données d'un autre quartier par erreur)
# ==============================================================================

def analyser_ecarts(ids_theoriques: set, ids_physiques: set, label: str) -> dict:
    """
    Compare le set théorique au set physique et retourne les statistiques.
    label : 'images' ou 'textes' (pour les messages d'affichage)
    """
    manquants = ids_theoriques - ids_physiques   # Dans CSV mais pas sur disque
    fantomes  = ids_physiques  - ids_theoriques  # Sur disque mais pas dans CSV
    presents  = ids_theoriques & ids_physiques   # Dans les deux (intersection)

    taux = (len(presents) / len(ids_theoriques) * 100) if ids_theoriques else 0

    return {
        "label"      : label,
        "theorique"  : len(ids_theoriques),
        "physique"   : len(ids_physiques),
        "presents"   : len(presents),
        "manquants"  : len(manquants),
        "fantomes"   : len(fantomes),
        "taux"       : taux,
        "ids_manquants": sorted(list(manquants)),
        "ids_fantomes" : sorted(list(fantomes)),
    }


# ==============================================================================
# PARTIE 5 — VÉRIFICATION CROISÉE IMAGES ↔ TEXTES
# Un niveau d'audit supplémentaire : on vérifie la cohérence ENTRE les deux
# dossiers. Idéalement, chaque annonce devrait avoir à la fois une image ET
# un fichier texte. Les cas où l'un existe sans l'autre sont des "orphelins
# croisés" — potentiellement problématiques pour la Phase 2 qui a besoin
# des deux modalités (image + texte) pour chaque annonce.
# ==============================================================================

def verifier_coherence_croisee(ids_images: set, ids_textes: set) -> dict:
    """
    Identifie les annonces qui n'ont qu'une image sans texte,
    ou qu'un texte sans image.
    """
    image_sans_texte = ids_images - ids_textes
    texte_sans_image = ids_textes - ids_images
    complets         = ids_images & ids_textes  # Les deux présents

    return {
        "complets"         : len(complets),
        "image_sans_texte" : len(image_sans_texte),
        "texte_sans_image" : len(texte_sans_image),
        "ids_image_sans_texte": sorted(list(image_sans_texte))[:MAX_ORPHELINS_AFFICHES],
        "ids_texte_sans_image": sorted(list(texte_sans_image))[:MAX_ORPHELINS_AFFICHES],
    }


# ==============================================================================
# PARTIE 6 — AFFICHAGE DU RAPPORT FINAL
# Ce rapport est conçu pour être copié-collé dans le README.md.
# Il répond exactement aux exigences du cahier des charges :
#   ✓ Total annonces CSV
#   ✓ Total images / textes téléchargés
#   ✓ Taux de complétion
#   ✓ Liste des orphelins (premiers IDs manquants)
# ==============================================================================

def afficher_rapport(stats_img: dict, stats_txt: dict, coherence: dict) -> None:
    """Affiche le rapport complet du Sanity Check."""

    separateur = "=" * 65

    print("\n" + separateur)
    print("   RAPPORT SANITY CHECK — ImmoVision 360 / Élysée")
    print("   (À copier dans README.md — section Audit des Données)")
    print(separateur)

    # --- Bloc Images ---
    print("\n  📷 AUDIT DES IMAGES (/data/raw/images/)")
    print(f"  {'IDs attendus (CSV Élysée)':<40}: {stats_img['theorique']}")
    print(f"  {'Fichiers .jpg présents sur disque':<40}: {stats_img['physique']}")
    print(f"  {'Images correctement ingérées':<40}: {stats_img['presents']}")
    print(f"  {'Images manquantes (orphelins)':<40}: {stats_img['manquants']}")
    print(f"  {'Fichiers fantômes (hors CSV)':<40}: {stats_img['fantomes']}")
    print(f"  {'Taux de complétion images':<40}: {stats_img['taux']:.1f}%")

    if stats_img['ids_manquants']:
        print(f"\n  Premiers IDs d'images manquantes :")
        for eid in stats_img['ids_manquants'][:MAX_ORPHELINS_AFFICHES]:
            print(f"    → {eid}")

    # --- Bloc Textes ---
    print(f"\n  📝 AUDIT DES TEXTES (/data/raw/texts/)")
    print(f"  {'IDs attendus (CSV Élysée)':<40}: {stats_txt['theorique']}")
    print(f"  {'Fichiers .txt présents sur disque':<40}: {stats_txt['physique']}")
    print(f"  {'Textes correctement générés':<40}: {stats_txt['presents']}")
    print(f"  {'Textes manquants (orphelins)':<40}: {stats_txt['manquants']}")
    print(f"  {'Fichiers fantômes (hors CSV)':<40}: {stats_txt['fantomes']}")
    print(f"  {'Taux de complétion textes':<40}: {stats_txt['taux']:.1f}%")

    if stats_txt['ids_manquants']:
        print(f"\n  Premiers IDs de textes manquants :")
        for eid in stats_txt['ids_manquants'][:MAX_ORPHELINS_AFFICHES]:
            print(f"    → {eid}")

    # --- Bloc Cohérence croisée ---
    print(f"\n  🔗 COHÉRENCE CROISÉE (Image + Texte)")
    print(f"  {'Annonces complètes (img + txt)':<40}: {coherence['complets']}")
    print(f"  {'Image présente, texte absent':<40}: {coherence['image_sans_texte']}")
    print(f"  {'Texte présent, image absente':<40}: {coherence['texte_sans_image']}")

    if coherence['ids_image_sans_texte']:
        print(f"\n  Exemples d'images sans texte :")
        for eid in coherence['ids_image_sans_texte']:
            print(f"    → {eid}")

    if coherence['ids_texte_sans_image']:
        print(f"\n  Exemples de textes sans image :")
        for eid in coherence['ids_texte_sans_image']:
            print(f"    → {eid}")

    # --- Verdict global ---
    print(f"\n  {'─' * 63}")
    taux_global = min(stats_img['taux'], stats_txt['taux'])
    if taux_global >= 95:
        verdict = "✅ EXCELLENT  — Data Lake prêt pour la Phase 2 (ETL)"
    elif taux_global >= 85:
        verdict = "⚠️  ACCEPTABLE — Quelques pertes, à documenter dans le README"
    else:
        verdict = "❌ INSUFFISANT — Trop de données manquantes, relancer les scripts"

    print(f"  VERDICT : {verdict}")
    print(f"  {'─' * 63}")
    print(separateur + "\n")


# ==============================================================================
# PARTIE 7 — POINT D'ENTRÉE
# ==============================================================================

if __name__ == "__main__":

    print("\n" + "=" * 65)
    print("   SANITY CHECK — ImmoVision 360")
    print("=" * 65)

    # Étape 1 : Charger les IDs théoriques depuis le CSV
    ids_theoriques = charger_ids_theoriques(CSV_LISTINGS, QUARTIER_CIBLE)

    # Étape 2 : Lister les IDs physiquement présents dans chaque dossier
    print("[→] Scan du dossier images...")
    ids_images = lister_ids_physiques(DIR_IMAGES, ".jpg")
    print(f"[✓] Fichiers .jpg trouvés : {len(ids_images)}")

    print("[→] Scan du dossier textes...")
    ids_textes = lister_ids_physiques(DIR_TEXTES, ".txt")
    print(f"[✓] Fichiers .txt trouvés : {len(ids_textes)}")

    # Étape 3 : Analyser les écarts pour chaque modalité
    stats_img = analyser_ecarts(ids_theoriques, ids_images, "images")
    stats_txt = analyser_ecarts(ids_theoriques, ids_textes, "textes")

    # Étape 4 : Vérifier la cohérence croisée images ↔ textes
    coherence = verifier_coherence_croisee(ids_images, ids_textes)

    # Étape 5 : Afficher le rapport final
    afficher_rapport(stats_img, stats_txt, coherence)