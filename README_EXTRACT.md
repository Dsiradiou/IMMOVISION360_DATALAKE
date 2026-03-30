[→] Chargement de : C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\listings.csv.gz
[✓] CSV chargé — 81853 lignes, 79 colonnes
[✓] Après filtre 'Élysée' : 2625 annonces

[✓] Colonnes conservées : 38 / 38
  [!] 2625 prix non convertibles → NaN (sera traité en 05_transform)
[✓] Pré-nettoyage des formats appliqué (prix, booléens, taux)

[✓] Fichier Silver intermédiaire sauvegardé :
    C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\filtered_elysee.csv

============================================================
  RAPPORT D'EXTRACTION — 04_extract.py
============================================================
  Annonces extraites (Élysée)   : 2625
  Colonnes conservées           : 38
  Mémoire utilisée              : 2.41 MB

  Colonnes avec valeurs manquantes (NaN) :
    price                                         : 2625 (100.0%)
    host_response_time                            :  681 (25.9%)
    host_response_rate                            :  681 (25.9%)
    review_scores_checkin                         :  661 (25.2%)
    review_scores_cleanliness                     :  661 (25.2%)
    last_review                                   :  660 (25.1%)
    first_review                                  :  660 (25.1%)
    review_scores_location                        :  660 (25.1%)
    review_scores_value                           :  660 (25.1%)
    review_scores_rating                          :  660 (25.1%)
    review_scores_communication                   :  660 (25.1%)
    reviews_per_month                             :  660 (25.1%)
    host_acceptance_rate                          :  574 (21.9%)
    license                                       :  515 (19.6%)
    host_is_superhost                             :  129 (4.9%)

  Prix (€/nuit) :
    Min    : nan €
    Médiane: nan €
    Max    : nan €
    Moyenne: nan €

  Hôtes multi-annonces : 1524 (58.1%)
============================================================
  [→] Prochaine étape : 05_transform.py
============================================================