[→] Connexion à PostgreSQL...
    Host : localhost:5432
    Base : immovision_db
    User : postgres
[✓] Connexion établie avec succès

[→] Chargement du fichier Silver : C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\transformed_elysee.csv
[✓] 0 lignes chargées — 44 colonnes
[✓] Typage des colonnes appliqué
[→] Injection dans la table 'elysee_listings_silver'...
    Lignes à injecter : 0
    Colonnes          : 44
[✓] Injection terminée avec succès

[→] Vérification post-injection dans PostgreSQL...

=================================================================
  RAPPORT LOAD — 06_load.py
=================================================================
  Table PostgreSQL        : elysee_listings_silver
  Base de données         : immovision_db
  Lignes attendues        : 0
  Lignes dans PostgreSQL  : 0
  Cohérence               : ✓ COHÉRENT
  Prix moyen (€/nuit)     : None €

  📷 Distribution standardization_score :

  📝 Distribution neighborhood_impact :

  🏠 Top 5 hôtes multi-annonces :
=================================================================
  [✓] Data Warehouse opérationnel.
  [→] Capturez ce rapport pour votre README.md (screenshot pgAdmin)
=================================================================