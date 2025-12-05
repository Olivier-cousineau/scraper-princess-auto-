# scraper-princess-auto

Scripts pour récupérer les soldes Princess Auto. Le script principal scrappe la page clearance et distribue les produits aux magasins listés.

## Scripts

### `scripts/scrape_princessauto_clearance_all_stores.mjs`
- Utilise Playwright pour charger la page soldes et scroller afin de charger tous les produits.
- Extraie les tuiles produits (titre, URL, image, prix actuel, ancien prix) et écrit un fichier `data.json` par magasin dans `outputs/princessauto/<slug>/`.
- Supporte le sharding via les variables d'environnement `SHARD_TOTAL` et `SHARD_INDEX`.

## Données

- Les magasins sont définis dans `data/princessauto/princess_auto_stores.json`. Par défaut, le fichier est vide : remplis-le avec les magasins attendus (tableau d'objets avec `storeName`, `city`, `province`, `postalCode`, `phone`, `slug`).

## Pré-requis

- Node.js 20+
- Dépendances Playwright (installées via `npm install playwright` si besoin).

## Exécution

```bash
SHARD_TOTAL=1 SHARD_INDEX=0 node scripts/scrape_princessauto_clearance_all_stores.mjs
```

## Workflow GitHub Actions

Un workflow manuel est disponible dans **Actions → Scraper Princess Auto** pour lancer le scraping avec sharding configurable :

1. Clique sur « Run workflow ».
2. Renseigne `shard_total` (nombre de shards à créer) ; laisse `shard_index` vide pour lancer tous les shards ou fournis un index (0-based) pour n'en exécuter qu'un seul.
3. Les résultats sont archivés en artefacts sous le nom `outputs-shard-<index>`.

## Scraper Walmart.com (États-Unis)

Le répertoire `walmart_scraper/` contient un scraper Python asynchrone pour la recherche et les fiches produits Walmart en utilisant les données `__NEXT_DATA__`.

### Prérequis

- Python 3.11+
- Dépendances Python : `pip install -r walmart_scraper/requirements.txt`

### Exécution

```bash
# Depuis la racine du dépôt
python walmart_main.py --query "laptop" --pages 10 --concurrency 5

# Ou en module
python -m walmart_scraper.walmart_main --query "laptop" --pages 10 --concurrency 5
```

Les exports sont générés dans `walmart_scraper/output/` :

- `walmart_search.json` : résultats de recherche normalisés
- `walmart_products.json` : fiches produits détaillées avec reviews
- `walmart_products.csv` : vue synthétique normalisée
