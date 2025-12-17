import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

// URL clearance globale. Ajuste si Princess Auto change son routage.
const CLEARANCE_URL = "https://www.princessauto.com/en/clearance";

const STORES_JSON = path.join(
  ROOT_DIR,
  "public",
  "princessauto",
  "stores.json"
);
const OUTPUT_ROOT = path.join(ROOT_DIR, "outputs", "princessauto");
let activeBrowser = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureOutputsRoot() {
  if (!fs.existsSync(OUTPUT_ROOT)) {
    fs.mkdirSync(OUTPUT_ROOT, { recursive: true });
  }
}

function loadStores() {
  if (!fs.existsSync(STORES_JSON)) {
    throw new Error(`Stores JSON not found at: ${STORES_JSON}`);
  }
  const raw = fs.readFileSync(STORES_JSON, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("stores.json must be an array");
  }
  console.log(`🏪 Loaded ${parsed.length} Princess Auto stores from ${STORES_JSON}`);
  return parsed;
}

function getStoresForThisShard(allStores) {
  const totalShards = Number(process.env.TOTAL_SHARDS ?? "35");
  const rawShard = Number(process.env.SHARD_INDEX ?? "0");

  let shardIndex = rawShard;
  if (rawShard >= 1 && rawShard <= totalShards) shardIndex = rawShard - 1;

  if (Number.isNaN(totalShards) || totalShards <= 0) {
    throw new Error(`Invalid TOTAL_SHARDS: ${process.env.TOTAL_SHARDS}`);
  }

  if (Number.isNaN(shardIndex) || shardIndex < 0 || shardIndex >= totalShards) {
    throw new Error(
      `Invalid SHARD_INDEX: ${process.env.SHARD_INDEX} (total: ${totalShards})`
    );
  }

  const n = allStores.length;
  const s = totalShards;
  const base = Math.floor(n / s);
  const rem = n % s;
  const start = shardIndex * base + Math.min(shardIndex, rem);
  const end = start + base + (shardIndex < rem ? 1 : 0);
  const shardStores = allStores.slice(start, end);

  console.log(`Total stores: ${n}`);
  console.log(
    `Shard ${shardIndex + 1}/${s} handles ${shardStores.length} stores (${start}..${end - 1})`
  );
  if (shardStores.length === 0) {
    console.log("🟡 No stores assigned to this shard. Exiting cleanly.");
    process.exit(0);
  }

  if (shardStores.length > 0) {
    const storePreview = shardStores
      .map((store) => store.slug || store.storeId || store.id)
      .slice(0, 20)
      .join(", ");
    console.log(`🗃️ Stores in shard (max 20 shown): ${storePreview}`);
  }

  return shardStores;
}

function startSoftTimeout(maxMinutes, getBrowser) {
  if (!Number.isFinite(maxMinutes) || maxMinutes <= 0) return null;

  const maxMs = maxMinutes * 60 * 1000;

  return setTimeout(() => {
    console.warn(`⏳ MAX_RUN_MINUTES=${maxMinutes} reached. Exiting cleanly.`);
    const browser = getBrowser?.();
    if (browser) {
      browser
        .close()
        .catch((error) =>
          console.error("⚠️ Failed to close browser on soft timeout:", error)
        )
        .finally(() => process.exit(0));
    } else {
      process.exit(0);
    }
  }, maxMs);
}

async function loadClearancePage(page) {
  console.log(`➡️ Ouverture de la page de soldes: ${CLEARANCE_URL}`);
  await page.goto(CLEARANCE_URL, { waitUntil: "networkidle" });
  await page.waitForTimeout(3000);

  // Scroll progressif pour charger les produits ou déclencher un éventuel infinite scroll.
  console.log("↕️ Scroll vers le bas pour charger un max de produits...");
  let previousHeight = 0;
  for (let i = 0; i < 20; i++) {
    const { height, loadMoreClicked } = await page.evaluate(() => {
      const loadMoreButton = Array.from(
        document.querySelectorAll("button, a")
      ).find((btn) => /load more|show more|plus/i.test(btn.textContent || ""));
      if (loadMoreButton) {
        (loadMoreButton instanceof HTMLElement ? loadMoreButton : null)?.click();
      }
      window.scrollTo(0, document.body.scrollHeight);
      return {
        height: document.body.scrollHeight,
        loadMoreClicked: Boolean(loadMoreButton),
      };
    });

    if (height === previousHeight && !loadMoreClicked) {
      break;
    }
    previousHeight = height;
    await sleep(1500);
  }
}

async function extractProducts(page) {
  console.log("🔍 Extraction des produits...");

  const products = await page.evaluate(() => {
    const tiles = Array.from(
      document.querySelectorAll(
        "[data-testid='product-tile'], li.product-tile, div.product-tile, div.product-grid__item, li.product-grid__item"
      )
    );

    return tiles
      .map((tile) => {
        const getText = (selector) =>
          tile.querySelector(selector)?.textContent?.trim() || null;
        const getAttr = (selector, attr) =>
          tile.querySelector(selector)?.getAttribute(attr) || null;

        const title =
          getText("[itemprop='name']") ||
          getText(".product-name") ||
          getText(".product-title") ||
          getText("a");
        const rawUrl = getAttr("a", "href");
        const url = rawUrl ? new URL(rawUrl, document.baseURI).href : null;
        const imageUrl = getAttr("img", "src") || getAttr("img", "data-src");
        const currentPriceText =
          getText(".sales") ||
          getText(".price-sales") ||
          getText(".product-price") ||
          getText(".current-price") ||
          getText("[data-price-type='finalPrice']") ||
          getText(".price") ||
          null;
        const originalPriceText =
          getText(".strike-through") ||
          getText(".price-standard") ||
          getText(".old-price") ||
          getText(".was-price") ||
          null;

        if (!title || !url) return null;

        return {
          title,
          url,
          imageUrl,
          currentPriceText,
          originalPriceText,
        };
      })
      .filter(Boolean);
  });

  console.log(`✅ Produits extraits: ${products.length}`);
  return products;
}

function writeStoreOutput(store, products) {
  const slug = store.slug;
  const storeDir = path.join(OUTPUT_ROOT, slug);
  const outputFile = path.join(storeDir, "data.json");

  fs.mkdirSync(storeDir, { recursive: true });

  const payload = {
    storeName: store.name,
    city: store.city,
    province: store.province,
    address: store.address,
    storeId: store.storeId,
    postalCode: store.postalCode ?? null,
    phone: store.phone ?? null,
    slug: store.slug,
    updatedAt: new Date().toISOString(),
    products,
  };

  fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2), "utf8");

  console.log(
    `💾 Écrit ${products.length} produit(s) pour ${store.name} → ${outputFile}`
  );
}

async function main() {
  ensureOutputsRoot();

  const maxRunMinutes = Number(process.env.MAX_RUN_MINUTES ?? "150");
  const allStores = loadStores();
  const stores = getStoresForThisShard(allStores);
  const softTimeout = startSoftTimeout(maxRunMinutes, () => activeBrowser);

  if (stores.length === 0) {
    console.log("🟡 No stores assigned to this shard. Exiting cleanly.");
    process.exit(0);
  }

  activeBrowser = await chromium.launch({ headless: true });

  try {
    const page = await activeBrowser.newPage();
    await loadClearancePage(page);
    const products = await extractProducts(page);

    for (const store of stores) {
      console.log(
        `🛒 Distribution des produits au magasin: ${store.name} (${store.slug})`
      );
      writeStoreOutput(store, products);
    }
  } catch (error) {
    console.error("❌ Erreur globale dans le scraper Princess Auto:", error);
    process.exitCode = 1;
  } finally {
    if (softTimeout) clearTimeout(softTimeout);
    if (activeBrowser) await activeBrowser.close();
  }
}

main();
