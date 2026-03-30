# README — Data Profiling : `filtered_elysee.csv`

**Projet :** ImmoVision 360 — Phase 2  
**Fichier audité :** `data/processed/filtered_elysee.csv`  
**Généré par :** `04_extract.py`  
**Lignes :** 2 625 annonces (quartier Élysée)  
**Colonnes :** 38 (sélectionnées sur 70+ disponibles dans `listings.csv.gz`)

---

## 1. Contexte et objectif

Ce document prouve que les données ont été **comprises et analysées** avant toute transformation automatique dans `05_transform.py`. Il constitue le livrable obligatoire de profiling exigé par le cahier des charges.

---

## 2. Valeurs manquantes (NaN)

15 colonnes présentent des valeurs manquantes.

| Colonne | NaN | % | Cause probable | Décision retenue |
|---|---|---|---|---|
| `price` | 2625 | 100.0% | Version visualisation sans prix | Utiliser `listings.csv.gz` (corrigé) |
| `host_response_time` | 681 | 26.0% | Hôtes sans réponses enregistrées | Imputer `'unknown'` (catégorie) |
| `host_response_rate` | 681 | 26.0% | Lié à `host_response_time` | Imputer médiane (97.0%) |
| `review_scores_checkin` | 661 | 25.2% | Annonces sans avis suffisants | Imputer médiane |
| `review_scores_cleanliness` | 661 | 25.2% | Annonces sans avis suffisants | Imputer médiane |
| `last_review` | 660 | 25.1% | Annonces jamais réservées | Laisser NaN (information utile) |
| `first_review` | 660 | 25.1% | Annonces jamais réservées | Laisser NaN (information utile) |
| `review_scores_location` | 660 | 25.1% | Annonces sans avis | Imputer médiane |
| `review_scores_value` | 660 | 25.1% | Annonces sans avis | Imputer médiane |
| `review_scores_rating` | 660 | 25.1% | Annonces sans avis | Imputer médiane |
| `review_scores_communication` | 660 | 25.1% | Annonces sans avis | Imputer médiane |
| `reviews_per_month` | 660 | 25.1% | Annonces sans avis → logiquement 0 | Imputer `0` (logique métier) |
| `host_acceptance_rate` | 574 | 21.9% | Hôtes sans historique d'acceptation | Imputer médiane |
| `license` | 515 | 19.6% | Annonces sans enregistrement légal | Laisser NaN (**signal fort Hyp. A**) |
| `host_is_superhost` | 129 | 4.9% | Compte supprimé ou données manquantes | Imputer `False` |

> **Note sur `price` :** La totalité des prix était NaN car le fichier `listings.csv` d'origine correspondait à la version *visualisation* d'Inside Airbnb, qui ne contient pas les prix. Corrigé en téléchargeant `listings.csv.gz` (version complète). La conversion `"$120.00"` → `120.0` est appliquée dans `04_extract.py`.

---

## 3. Statistiques descriptives

| Colonne | Min | Q25 | Médiane | Max | Observation |
|---|---|---|---|---|---|
| `latitude` | 48.86 | 48.87 | 48.87 | 48.88 | ✓ Cohérent — quartier Élysée |
| `longitude` | 2.30 | 2.30 | 2.31 | 2.33 | ✓ Cohérent — quartier Élysée |
| `accommodates` | 1 | 2 | 3 | 16 | ✓ Max 16 — logements entiers OK |
| `availability_365` | 0 | 125 | 286 | 365 | ✓ Forte dispersion — normal |
| `availability_90` | 0 | 21 | 59 | 90 | ✓ Cohérent |
| `host_listings_count` | 1 | 1 | 3 | 453 | ⚠ Max 453 — multi-proprio détecté |
| `calculated_host_listings_count` | 1 | 1 | 2 | 307 | ⚠ Max 307 — confirme Hyp. A |
| `number_of_reviews` | 0 | 5 | 29 | 2001 | ⚠ Max 2001 — outlier notable |
| `review_scores_rating` | 1.0 | 4.70 | 4.84 | 5.0 | ⚠ Min 1.0 — outlier à investiguer |
| `reviews_per_month` | 0.01 | 0.20 | 0.75 | 17.50 | ⚠ Max 17.5/mois — rotation très élevée |
| `minimum_nights` | 1 | 1 | 3 | 365 | ⚠ Max 365 nuits — outlier fort |

---

## 4. Valeurs aberrantes (Outliers)

Les outliers ne sont pas systématiquement supprimés. Dans le contexte de l'analyse de la gentrification, certains cas extrêmes sont précisément le signal que cherche la Maire de Paris.

| Outlier détecté | Hypothèse | Décision dans `05_transform.py` |
|---|---|---|
| `minimum_nights = 365` | A | Drop — location annuelle, hors scope touristique |
| `number_of_reviews > 500` | B | Cap à 500 ou conserver (signal d'activité intense) |
| `host_listings_count > 50` | A | **Conserver** — c'est exactement ce qu'on cherche (industrialisation) |
| `reviews_per_month > 10` | A | **Conserver** — signal fort de rotation touristique |
| `review_scores_rating = 1.0` | B | Investiguer — possibles faux avis ou erreurs de saisie |
| `price = 0 €` | A | Drop — prix nul incohérent |

---

## 5. Types de données et conversions

### Déjà appliquées dans `04_extract.py`

| Colonne | Avant | Après | Méthode |
|---|---|---|---|
| `price` | `str` `"$120.00"` | `float` `120.0` | Suppression `$` et `,` + `pd.to_numeric` |
| `host_is_superhost` | `str` `"t"/"f"` | `bool` | `.map({"t": True, "f": False})` |
| `host_response_rate` | `str` `"97%"` | `float` `97.0` | Suppression `%` + `pd.to_numeric` |
| `host_acceptance_rate` | `str` `"95%"` | `float` `95.0` | Suppression `%` + `pd.to_numeric` |

### À appliquer dans `05_transform.py`

| Colonne | Conversion | Objectif |
|---|---|---|
| `host_since`, `first_review`, `last_review` | `str` → `datetime` | Calcul d'ancienneté en jours |
| `host_response_time` | `str` catégorielle → encodage ordinal | `within an hour=4`, `within a few hours=3`, `within a day=2`, `a few days or more=1`, `unknown=0` |

---

## 6. Récapitulatif des décisions d'imputation

| Stratégie | Colonnes concernées | Justification |
|---|---|---|
| **Drop de la ligne** | `price = 0` ou NaN après correction | Prix nul incohérent — ligne inutilisable |
| **Imputer `0`** (logique métier) | `reviews_per_month` | Une annonce sans avis a logiquement 0 avis/mois |
| **Imputer la médiane** | `review_scores_*`, `host_response_rate`, `host_acceptance_rate` | Robuste aux outliers pour distributions asymétriques |
| **Imputer `'unknown'`** | `host_response_time` | Colonne catégorielle — créer une catégorie explicite |
| **Conserver NaN** | `first_review`, `last_review`, `license` | L'absence de licence est un signal fort Hyp. A ; l'absence de dates indique une annonce inactive |

---

## 7. Conclusion

Après cet audit, le fichier `filtered_elysee.csv` présente une qualité suffisante pour lancer le pipeline de transformation :

- **2 625 annonces** du quartier Élysée correctement isolées
- **38 colonnes** sélectionnées, toutes justifiées par les 3 hypothèses de la Maire
- Les NaN sont compris et traités par des stratégies adaptées à chaque cas
- Les outliers détectés (multi-proprio, rotation élevée) sont des **signaux métier précieux** — ils ne seront pas supprimés aveuglément
- Les types sont cohérents après les conversions de `04_extract.py`

**Le script `05_transform.py` peut être lancé en appliquant les règles définies dans ce document.**

---

*Document rédigé par Siradiou — ImmoVision 360, Phase 2 — Avril 2026*