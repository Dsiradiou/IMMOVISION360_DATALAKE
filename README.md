=================================================================
   SANITY CHECK — ImmoVision 360
=================================================================
[→] Lecture du CSV source : C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\raw\tabular\listings.csv\listings.csv
[✓] IDs théoriques (attendus) : 2625

[→] Scan du dossier images...
[✓] Fichiers .jpg trouvés : 2497
[→] Scan du dossier textes...
[✓] Fichiers .txt trouvés : 1965

=================================================================
   RAPPORT SANITY CHECK — ImmoVision 360 / Élysée
   (À copier dans README.md — section Audit des Données)
=================================================================

  📷 AUDIT DES IMAGES (/data/raw/images/)
  IDs attendus (CSV Élysée)               : 2625
  Fichiers .jpg présents sur disque       : 2497
  Images correctement ingérées            : 2497
  Images manquantes (orphelins)           : 128
  Fichiers fantômes (hors CSV)            : 0
  Taux de complétion images               : 95.1%

  Premiers IDs d'images manquantes :
    → 3492606
    → 6211581
    → 6921651
    → 7299888
    → 19444226
    → 30394812
    → 33092275
    → 40464181
    → 42191976
    → 42555402

  📝 AUDIT DES TEXTES (/data/raw/texts/)
  IDs attendus (CSV Élysée)               : 2625
  Fichiers .txt présents sur disque       : 1965
  Textes correctement générés             : 1965
  Textes manquants (orphelins)            : 660
  Fichiers fantômes (hors CSV)            : 0
  Taux de complétion textes               : 74.9%

  Premiers IDs de textes manquants :
    → 1137844
    → 1363359
    → 3015969
    → 3139351
    → 3199889
    → 3467111
    → 3493067
    → 4656901
    → 4674903
    → 4806693

  🔗 COHÉRENCE CROISÉE (Image + Texte)
  Annonces complètes (img + txt)          : 1872
  Image présente, texte absent            : 625
  Texte présent, image absente            : 93

  Exemples d'images sans texte :
    → 1137844
    → 1363359
    → 3015969
    → 3139351
    → 3199889
    → 3467111
    → 3493067
    → 4656901
    → 4674903
    → 4806693

  Exemples de textes sans image :
    → 3492606
    → 6211581
    → 6921651
    → 7299888
    → 19444226
    → 30394812
    → 33092275
    → 42191976
    → 42555402
    → 44054719

  ───────────────────────────────────────────────────────────────
  VERDICT : ❌ INSUFFISANT — Trop de données manquantes, relancer les scripts
  ───────────────────────────────────────────────────────────────
=================================================================