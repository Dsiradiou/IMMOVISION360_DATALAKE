# Bloc A — Feuille de route d'analyse EDA
**Projet :** ImmoVision 360 — Périmètre Élysée  
**Table de référence :** `elysee_tabular`  
**Date :** Avril 2026

---

## Rappel des 3 questionnements majeurs (Chapitre 1)

| # | Questionnement majeur |
|---|----------------------|
| A | **Concentration économique** — L'offre est-elle dominée par quelques acteurs professionnels ? |
| B | **Déshumanisation de l'accueil** — Le lien social se brise-t-il au profit de processus automatisés ? |
| C | **Standardisation visuelle** — Les logements sont-ils devenus des produits financiers stériles ? |

---

## Feuille de route : 5 questions opérationnelles

| # | Question / Hypothèse | Variable(s) retenue(s) et nature | Graphique envisagé |
|---|----------------------|----------------------------------|-------------------|
| **1** | **Les hôtes multi-annonces ont-ils tendance à avoir un profil hôtélisé ?** *(questionnement A × B)* — Je soupçonne que les hôtes gérant de nombreuses annonces adoptent une gestion impersonnelle, se traduisant par un score `neighborhood_impact_score` = 1 (hôtélisé) plus fréquent. | `calculated_host_listings_count` — **quantitative** (proxy de concentration / gestion industrielle) × `neighborhood_impact_score` — **trichotomique ordonnée** (proxy du lien social : 1=hôtélisé, 0=voisinage, −1=non classé) | **Boîtes à moustaches** de `calculated_host_listings_count` selon les 3 modalités de `neighborhood_impact_score`. Axe X : modalités du score (libellés lisibles). Axe Y : nombre d'annonces par hôte. Filtre : traitement à part des −1 du score (commentaire sur leur poids). |
| **2** | **Comment se répartissent les annonces entre profil standardisé, personnel et non analysé ?** *(questionnement C)* — Hypothèse : une majorité des annonces de l'Élysée présente un profil « catalogue », signe d'une commercialisation intensive du parc immobilier. | `standardization_score` — **trichotomique** (1=industrialisé, 0=personnel, −1=non analysé). Variable unique, description univariée avant tout croisement. | **Diagramme en barres** des effectifs par modalité. Axe X : catégories avec libellés lisibles (« Industrialisé », « Personnel », « Non analysé »). Axe Y : nombre d'annonces. Note : proportion des −1 commentée séparément (limite d'analyse). |
| **3** | **La privatisation des biens détruit-elle le lien social ?** *(questionnement B × C)* — Hypothèse : les logements entiers, loués sans présence de l'hôte, génèrent davantage de profils hôtélisés que les chambres privées où l'hôte réside sur place. | `room_type_code` — **catégorielle nominale** (1=chambre privée, 2=logement entier ; codes 0 et 3 exclus du graphique principal) × `neighborhood_impact_score` — **trichotomique ordonnée** | **Barres groupées** des effectifs de `neighborhood_impact_score` (−1 / 0 / 1) par `room_type_code`. Axe X : type de logement (1 vs 2 uniquement). Axe Y : nombre d'annonces. Couleurs par modalité de score. Filtre : `room_type_code` ∈ {1, 2}. |
| **4** | **Les hôtes de logements entiers sont-ils plus réactifs que les hôtes de chambres privées ?** *(questionnement B)* — Hypothèse : les hôtes de logements entiers, souvent professionnels ou agences, affichent des taux de réponse plus élevés et des délais plus courts, signe d'une gestion automatisée. | `room_type_code` — **catégorielle nominale** (filtrée sur 1 et 2) × `host_response_rate_num` — **quantitative** (proxy de réactivité, 0–100 %) × `host_response_time_code` — **ordinale** (0=dans l'heure → 3=plusieurs jours) | **Deux visualisations complémentaires :** (1) Boîtes à moustaches de `host_response_rate_num` par `room_type_code` (1 vs 2). Axe X : type de logement. Axe Y : taux de réponse (%). (2) Barres des effectifs de `host_response_time_code` par `room_type_code`. Filtre : `room_type_code` ∈ {1, 2} ; exclusion des −1 de `host_response_time_code`. |
| **5** | **Les hôtes multi-annonces répondent-ils plus vite ? Signe d'une gestion professionnelle.** *(questionnement A × B)* — Hypothèse : les hôtes gérant de nombreuses annonces ont des délais de réponse plus courts et des taux plus élevés, ce qui trahit une organisation automatisée ou une agence. | `calculated_host_listings_count` — **quantitative** (proxy de professionnalisation) × `host_response_time_code` — **ordinale** (proxy du délai) × `host_response_rate_num` — **quantitative** (proxy de réactivité) | **Deux visualisations :** (1) Boîtes à moustaches de `calculated_host_listings_count` par `host_response_time_code` (0→3, ordre métier). Axe X : code délai. Axe Y : nombre d'annonces par hôte. (2) Nuage de points `host_response_rate_num` (axe X) vs `calculated_host_listings_count` (axe Y). Filtre : exclusion des −1 sur `host_response_time_code`. |

---

## Notes transversales

- Les scores `standardization_score` et `neighborhood_impact_score` issus du pipeline contiennent des **−1** (données non classables ou absentes). Ces lignes sont signalées dans chaque analyse et traitées à part — elles ne sont **pas** intégrées dans les calculs de tendance.
- `calculated_host_listings_count` peut présenter des valeurs très élevées (ex. 816 annonces pour « Blueground »). Une stratification ou un plafonnement visuel sera envisagé si les extrêmes écrasent la lecture du graphique.
- **Limite générale :** l'absence de la variable `price` dans `elysee_tabular` empêche de relier directement les profils d'hôtes aux dynamiques de prix — cette question reste ouverte et sera signalée dans le rapport final.
