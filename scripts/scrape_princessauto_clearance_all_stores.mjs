import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// URL de base pour les soldes / liquidation Princess Auto.
// À ajuster si tu trouves une meilleure URL dédiée "Clearance".
const CLEARANCE_URL = "https://www.princessauto.com/en/category/Sale";

const ROOT_DIR = path.join(__dirname, "..");
const STORES_JSON = path.join(
  ROOT_DIR,
  "data",
  "princessauto",
  "princess_auto_stores.json"
);
const OUTPUT_ROOT = path.join(ROOT_DIR, "outputs", "princessauto");

/**
 * Pause simple
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Charge la liste complète des magasins depuis le JSON
 */
function loadStores() {
  if (!fs.existsSync(STORES_JSON)) {
    throw new Error(`Stores JSON not found at: ${STORES_JSON}`);
  }
  const raw = fs.readFileSync(STORES_JSON, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("princess_auto_stores.json must be an array");
  }
  return parsed;
}

/**
 * Retourne les magasins à traiter sur CE shard.
 * SHARD_INDEX et SHARD_TOTAL viennent des variables d'env (workflow matrix).
 */
function getStoresForThisShard(allStores) {
  const shardTotal = Number(process.env.SHARD_TOTAL ?? "1");
  const shardIndex = Number(process.env.SHARD_INDEX ?? "0");

  if (Number.isNaN(shardTotal) || shardTotal <= 0) {
    throw new Error(`Invalid SHARD_TOTAL: ${process.env.SHARD_TOTAL}`);
  }
  if (Number.isNaN(shardIndex) || shardIndex < 0 || shardIndex >= shardTotal) {
    throw new Error(
      `Invalid SHARD_INDEX: ${process.env.SHARD_INDEX} (total: ${shardTotal})`
    );
  }

  const selected = allStores.filter((_, idx) => idx % shardTotal === shardIndex);

  console.log(
    `🧩 Shard ${shardIndex + 1}/${shardTotal} – ${selected.length} magasin(s) à traiter`
  );

  return selected;
}

/**
 * Charge la page de soldes / liquidation
 */
async function loadClearancePage(page) {
  console.log(`➡️ Ouverture de la page de soldes: ${CLEARANCE_URL}`);
  await page.goto(CLEARANCE_URL, { waitUntil: "networkidle" });
  await page.waitForTimeout(4000);

  // Scroll progressif pour charger tous les produits
  console.log("↕️ Scroll vers le bas pour charger un max de produits...");
  let previousHeight = 0;
  for (let i = 0; i < 15; i++) {
    const currentHeight = await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
      return document.body.scrollHeight;
    });
    if (currentHeight === previousHeight) break;
    previousHeight = currentHeight;
    await sleep(1500);
  }
}

/**
 * Extraction des produits (à ajuster selon le DOM réel de Princess Auto)
 */
async function extractProducts(page) {
  console.log("🔍 Extraction des produits...");

  // À ajuster après avoir inspecté le site.
  const tiles = await page.$$(
    "div.product-tile, li.product-tile, div.product-grid__item, li.product-grid__item"
  );

  console.log(`🧱 Nombre de tuiles candidates: ${tiles.length}`);

  const products = [];

  for (const tile of tiles) {
    try {
      const title = await tile
        .$eval(
          ".product-name, .product-title, [itemprop='name'], a",
          (el) => el.innerText.trim()
        )
        .catch(() => null);

      const url = await tile
        .$eval("a", (el) => el.href)
        .catch(() => null);

      const imageUrl = await tile
        .$eval("img", (el) => el.src)
        .catch(() => null);

      const currentPriceText = await tile
        .$eval(
          ".sales, .price-sales, .product-price, .current-price, [data-price-type='finalPrice']",
          (el) => el.innerText.trim()
        )
        .catch(() => null);

      const originalPriceText = await tile
        .$eval(
          ".strike-through, .price-standard, .old-price, .was-price",
          (el) => el.innerText.trim()
        )
        .catch(() => null);

      if (!title || !url) {
        continue;
      }

      products.push({
        title,
        url,
        imageUrl,
        currentPriceText,
        originalPriceText,
      });
    } catch (error) {
      console.warn("⚠️ Erreur sur une tuile, produit ignoré:", error.message);
    }
  }

  console.log(`✅ Produits extraits: ${products.length}`);
  return products;
}

/**
 * Écrit les produits d'un magasin dans:
 * outputs/princessauto/<slug>/data.json
 */
function writeStoreOutput(store, products) {
  const slug = store.slug;
  const storeDir = path.join(OUTPUT_ROOT, slug);
  const outputFile = path.join(storeDir, "data.json");

  if (!fs.existsSync(storeDir)) {
    fs.mkdirSync(storeDir, { recursive: true });
  }

  const payload = {
    storeName: store.storeName,
    city: store.city,
    province: store.province,
    postalCode: store.postalCode,
    phone: store.phone,
    slug: store.slug,
    updatedAt: new Date().toISOString(),
    products,
  };

  fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2), "utf8");

  console.log(`💾 Écrit ${products.length} produits pour ${store.storeName} → ${outputFile}`);
}

/**
 * Main: on ouvre 1 navigateur, on traite tous les magasins de CE shard.
 * Pour l'instant, la même page clearance est utilisée pour chaque magasin.
 * Plus tard, tu pourras adapter CLEARANCE_URL par magasin (geo, param store, etc.).
 */
async function main() {
  const allStores = loadStores();
  const stores = getStoresForThisShard(allStores);

  if (stores.length === 0) {
    console.log("ℹ️ Aucun magasin à traiter sur ce shard, fin.");
    return;
  }

  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage();

    // On scrape une seule fois la page de soldes pour CE shard,
    // puis on réutilise les mêmes produits pour chaque magasin du shard.
    await loadClearancePage(page);
    const products = await extractProducts(page);

    for (const store of stores) {
      console.log(
        `🛒 Distribution des produits au magasin: ${store.storeName} (${store.slug})`
      );
      writeStoreOutput(store, products);
    }
  } catch (error) {
    console.error("❌ Erreur globale dans le scraper Princess Auto:", error);
    process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

main();
