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
