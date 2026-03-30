C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Scripts\05_Transform.py:22: FutureWarning: 

All support for the `google.generativeai` package has ended. It will no longer be receiving 
updates or bug fixes. Please switch to the `google.genai` package as soon as possible.
See README for more details:

https://github.com/google-gemini/deprecated-generative-ai-python/blob/main/README.md

  import google.generativeai as genai
[✓] Modèle Gemini initialisé : gemini-2.5-flash-preview-04-17
[→] Reprise détectée — chargement de : C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\transformed_elysee.csv
[✓] 0 lignes chargées (mode reprise)
[✓] Toutes les annonces ont déjà été enrichies.

=================================================================
  RAPPORT TRANSFORM — 05_transform.py
  Modèle utilisé : gemini-2.5-flash-preview-04-17
=================================================================
  Annonces dans le fichier Silver final  : 0
  Colonnes totales                       : 44

  📷 standardization_score :
    Industrialisé  (1)  : 0
    Personnel      (0)  : 0
    Non analysé   (-1)  : 0

  📝 neighborhood_impact :
    Hôtélisé   (1)  : 0
    Voisinage  (0)  : 0
    Neutre    (-1)  : 0

  Fichier Silver sauvegardé :
  C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\transformed_elysee.csv
=================================================================
  [→] Prochaine étape : 06_load.py
=================================================================
