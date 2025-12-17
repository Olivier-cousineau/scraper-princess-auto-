import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

const SALE_URL = "https://www.princessauto.com/en/category/Sale";
const STORES_JSON = path.join(ROOT_DIR, "public", "princessauto", "stores.json");
const OUTPUT_ROOT = path.join(ROOT_DIR, "outputs", "princessauto");
const DEBUG_OUTPUT_DIR = path.join(ROOT_DIR, "outputs", "debug");
let activeBrowser = null;

const PRODUCT_TILE_SELECTOR =
  "[data-testid='product-tile'], li.product-tile, div.product-tile, div.product-grid__item, li.product-grid__item";

function ensureOutputsRoot() {
  if (!fs.existsSync(OUTPUT_ROOT)) {
    fs.mkdirSync(OUTPUT_ROOT, { recursive: true });
  }
  if (!fs.existsSync(DEBUG_OUTPUT_DIR)) {
    fs.mkdirSync(DEBUG_OUTPUT_DIR, { recursive: true });
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

function hasExceededMaxRun(startedAt, maxRunMinutes) {
  if (!Number.isFinite(maxRunMinutes) || maxRunMinutes <= 0) {
    return false;
  }

  const elapsedMs = Date.now() - startedAt;
  return elapsedMs >= maxRunMinutes * 60 * 1000;
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
        .catch((error) => console.error("⚠️ Failed to close browser on soft timeout:", error))
        .finally(() => process.exit(0));
    } else {
      process.exit(0);
    }
  }, maxMs);
}

async function waitForProductsGrid(page) {
  const tiles = page.locator(PRODUCT_TILE_SELECTOR).first();
  const emptySelectors = ["text=No products", "text=No items", "text=0 items"];

  try {
    await tiles.waitFor({ state: "visible", timeout: 30000 });
  } catch (error) {
    for (const selector of emptySelectors) {
      const emptyState = page.locator(selector).first();
      if (await emptyState.isVisible({ timeout: 2000 }).catch(() => false)) {
        break;
      }
    }
  }
}

async function scrollProductListIntoView(page) {
  const resultsLocator = page.locator("text=/Results\s+\d+\s*-\s*\d+\s+of/i").first();
  const anchorLocator = page.locator("a[href*='/product/'], a[href*='/p/']");

  for (let i = 0; i < 6; i++) {
    const hasResultsText = await resultsLocator.isVisible({ timeout: 2000 }).catch(() => false);
    const anchorCount = await anchorLocator.count().catch(() => 0);
    if (hasResultsText || anchorCount > 0) {
      return;
    }
    await page.mouse.wheel(0, 800).catch(() => {});
    await page.waitForTimeout(800);
  }
}

async function navigateToSale(page) {
  console.log(`➡️ Navigating to sale page: ${SALE_URL}`);
  await page.goto(SALE_URL, { waitUntil: "domcontentloaded" });
  await waitForProductsGrid(page);
}

async function setMyStore(page, store) {
  const storeLabel = store.storeName || store.name || store.slug || store.city;
  console.log(`Setting My Store => ${storeLabel}`);

  const trigger = page.locator("text=My Store").first();
  await trigger.click({ timeout: 10000 });

  const modal = page.locator("[role='dialog'], .store-selector, .select-store");
  await modal.waitFor({ state: "visible", timeout: 15000 }).catch(() => {});

  const searchInput = page
    .locator(
      "input[placeholder*='postal' i], input[placeholder*='city' i], input[type='search'], input[name*='store' i]"
    )
    .first();
  if (await searchInput.isVisible({ timeout: 5000 }).catch(() => false)) {
    await searchInput.fill(store.storeId?.toString() || store.city || store.address || "");
    await searchInput.press("Enter").catch(() => {});
    await page.waitForTimeout(1500);
  }

  const candidates = [];
  if (store.storeId) {
    candidates.push(`[data-store-id='${store.storeId}']`);
  }
  if (store.address) {
    candidates.push(`.store-item:has-text('${store.address}')`);
  }
  if (store.city) {
    candidates.push(`.store-item:has-text('${store.city}')`);
  }
  candidates.push(`text=${store.name}`);

  let storeOption = null;
  for (const selector of candidates) {
    const loc = page.locator(selector).first();
    if (await loc.isVisible({ timeout: 2000 }).catch(() => false)) {
      storeOption = loc;
      break;
    }
  }

  if (storeOption) {
    await storeOption.scrollIntoViewIfNeeded().catch(() => {});
    await storeOption.click({ timeout: 10000 }).catch(() => {});
  }

  const makeMyStoreButton = page
    .locator("button:has-text('Set as My Store'), button:has-text('Make this my store'), text=Set as My Store")
    .first();
  if (await makeMyStoreButton.isVisible({ timeout: 5000 }).catch(() => false)) {
    await makeMyStoreButton.click({ timeout: 10000 });
  }

  if (storeLabel) {
    await page
      .waitForFunction(
        (expectedName) => {
          const header = Array.from(document.querySelectorAll("header, [data-testid='header']"));
          return header.some((el) => el.textContent?.toLowerCase().includes(expectedName));
        },
        storeLabel.toLowerCase(),
        { timeout: 15000 }
      )
      .catch(() => {});
  }
}

async function enableAvailableInMyStoreFilter(page) {
  const checkboxByRole = page.getByRole("checkbox", { name: /available in my store/i }).first();
  if ((await checkboxByRole.count().catch(() => 0)) > 0) {
    const alreadyChecked = await checkboxByRole.isChecked().catch(() => false);
    if (!alreadyChecked) {
      await checkboxByRole.check({ force: true }).catch(() => {});
    }
    await waitForProductsGrid(page);
    await page.waitForTimeout(2000);
    console.log("Available in My Store enabled via checkbox (role selector)");
    return true;
  }

  const labelLocator = page.locator("label:has-text('Available in My Store')").first();
  if ((await labelLocator.count().catch(() => 0)) > 0) {
    await labelLocator.click({ force: true }).catch(() => {});
    const nestedCheckbox = labelLocator.locator("input[type='checkbox']").first();
    if ((await nestedCheckbox.count().catch(() => 0)) > 0) {
      const nestedChecked = await nestedCheckbox.isChecked().catch(() => false);
      if (!nestedChecked) {
        await nestedCheckbox.check({ force: true }).catch(() => {});
      }
    }
    await waitForProductsGrid(page);
    await page.waitForTimeout(2000);
    console.log("Available in My Store enabled via label click");
    return true;
  }

  const textLocator = page.getByText(/available in my store/i).first();
  if ((await textLocator.count().catch(() => 0)) > 0) {
    await textLocator.click({ force: true }).catch(() => {});
    await waitForProductsGrid(page);
    await page.waitForTimeout(2000);
    console.log("Available in My Store enabled via text locator");
    return true;
  }

  console.warn("⚠️ Unable to locate Available in My Store filter");
  return false;
}

async function loadAllPages(page, maxPages, storeStartedAt, maxStoreMinutes) {
  let previousCount = await page.locator(PRODUCT_TILE_SELECTOR).count();
  let stagnantPages = 0;

  for (let pageIndex = 1; pageIndex <= maxPages; pageIndex++) {
    if (hasExceededMaxRun(storeStartedAt, maxStoreMinutes)) {
      console.warn(
        `⏹️ MAX_STORE_MINUTES=${maxStoreMinutes} reached for store. Exiting cleanly.`
      );
      process.exit(0);
    }

    const loadMoreButton = page
      .locator("button:has-text('Load more'), button:has-text('Show more'), button:has-text('View more')")
      .first();
    const nextButton = page.locator("a:has-text('Next'), button:has-text('Next')").first();

    const hasLoadMore = await loadMoreButton.isVisible({ timeout: 2000 }).catch(() => false);
    const hasNext = await nextButton.isVisible({ timeout: 2000 }).catch(() => false);

    if (!hasLoadMore && !hasNext) {
      break;
    }

    if (hasLoadMore) {
      await loadMoreButton.click({ timeout: 10000 }).catch(() => {});
    } else if (hasNext) {
      await nextButton.click({ timeout: 10000 }).catch(() => {});
    }

    await waitForProductsGrid(page);
    await page.waitForTimeout(2000);

    const currentCount = await page.locator(PRODUCT_TILE_SELECTOR).count();
    if (currentCount <= previousCount) {
      stagnantPages += 1;
    } else {
      stagnantPages = 0;
      previousCount = currentCount;
    }

    if (stagnantPages >= 2) {
      break;
    }
  }
}

async function extractProducts(page) {
  console.log("🔍 Extracting products...");

  const productAnchorCount = await page
    .locator("a[href*='/product/'], a[href*='/p/']")
    .count()
    .catch(() => 0);
  console.log(`🔗 Product anchors detected before extraction: ${productAnchorCount}`);

  const products = await page.evaluate((productSelector) => {
    const anchors = Array.from(
      document.querySelectorAll("a[href*='/product/'], a[href*='/p/']")
    );
    const deduped = new Map();

    for (const anchor of anchors) {
      const href = anchor.getAttribute("href") || anchor.href;
      if (!href) continue;
      const url = new URL(href, document.baseURI).href;
      if (deduped.has(url)) continue;

      const tile = anchor.closest(productSelector) || anchor.closest("article") || anchor;
      const getText = (selector) => tile.querySelector(selector)?.textContent?.trim() || null;
      const getAttr = (selector, attr) => tile.querySelector(selector)?.getAttribute(attr) || null;

      const titleFromAttr = anchor.getAttribute("title") || anchor.getAttribute("aria-label");
      const title =
        titleFromAttr ||
        (anchor.textContent || "").trim() ||
        getText("[itemprop='name']") ||
        getText(".product-name") ||
        getText(".product-title");

      const imageEl =
        anchor.querySelector("img") ||
        tile.querySelector("img") ||
        tile.querySelector("[data-testid='product-image'] img");
      const imageUrl =
        imageEl?.getAttribute("src") || imageEl?.getAttribute("data-src") || imageEl?.getAttribute("data-lazy") || null;

      const currentPriceText =
        getText(".sales") ||
        getText(".price-sales") ||
        getText(".product-price") ||
        getText(".current-price") ||
        getText("[data-price-type='finalPrice']") ||
        getText(".price") ||
        getText("[class*='current']") ||
        null;
      const originalPriceText =
        getText(".strike-through") ||
        getText(".price-standard") ||
        getText(".old-price") ||
        getText(".was-price") ||
        getText("[class*='was']") ||
        null;

      if (!title || !url) continue;

      deduped.set(url, {
        title,
        url,
        imageUrl,
        currentPrice: currentPriceText,
        originalPrice: originalPriceText,
      });
    }

    return Array.from(deduped.values());
  }, PRODUCT_TILE_SELECTOR);

  console.log(`Products extracted: ${products.length}`);
  return products;
}

function writeStoreOutput(store, products) {
  const slug = store.slug;
  const storeDir = path.join(OUTPUT_ROOT, slug);
  const outputFile = path.join(storeDir, "data.json");

  fs.mkdirSync(storeDir, { recursive: true });

  const payload = {
    storeName: store.name || store.storeName,
    city: store.city,
    province: store.province,
    address: store.address,
    storeId: store.storeId ?? store.id ?? null,
    postalCode: store.postalCode ?? null,
    phone: store.phone ?? null,
    slug: store.slug,
    updatedAt: new Date().toISOString(),
    products,
  };

  fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2), "utf8");

  console.log(`💾 Wrote ${products.length} product(s) for ${store.slug} → ${outputFile}`);
  return payload;
}

async function captureDebug(page, slug, stage) {
  const htmlPath = path.join(DEBUG_OUTPUT_DIR, `pa_${slug}_${stage}.html`);
  const screenshotPath = path.join(DEBUG_OUTPUT_DIR, `pa_${slug}_${stage}.png`);

  try {
    const html = await page.content();
    fs.writeFileSync(htmlPath, html, "utf8");
    await page.screenshot({ path: screenshotPath, fullPage: true });
    console.log(`📝 Saved debug HTML: ${htmlPath}`);
    console.log(`📸 Saved debug screenshot: ${screenshotPath}`);
    return [htmlPath, screenshotPath];
  } catch (error) {
    console.warn(`⚠️ Failed to capture debug artifacts for ${slug} (${stage}):`, error);
    return [];
  }
}

function deleteDebugArtifacts(paths) {
  for (const target of paths) {
    if (target && fs.existsSync(target)) {
      try {
        fs.unlinkSync(target);
      } catch (error) {
        console.warn(`⚠️ Failed to delete debug artifact ${target}:`, error);
      }
    }
  }
}

async function uploadDebugArtifactsIfAvailable(paths) {
  const existingPaths = paths.filter((p) => fs.existsSync(p));
  if (existingPaths.length === 0) return;

  if (process.env.GITHUB_ACTIONS !== "true") {
    console.log("ℹ️ Not running in GitHub Actions; skipping artifact upload.");
    return;
  }

  try {
    // Optional dependency; will log and continue if unavailable.
    const artifact = await import("@actions/artifact").catch(() => null);
    if (!artifact?.default?.create) {
      console.warn("⚠️ @actions/artifact not available; cannot upload debug artifacts.");
      return;
    }

    const client = artifact.default.create();
    const uploadName = `princessauto-debug-${Date.now()}`;
    await client.uploadArtifact(uploadName, existingPaths, DEBUG_OUTPUT_DIR, {
      continueOnError: true,
    });
    console.log(`⬆️ Uploaded debug artifacts: ${uploadName}`);
  } catch (error) {
    console.warn("⚠️ Failed to upload debug artifacts:", error);
  }
}

function updateIndex(indexEntries) {
  const indexPath = path.join(OUTPUT_ROOT, "index.json");
  const payload = {
    updatedAt: new Date().toISOString(),
    stores: indexEntries,
  };
  fs.mkdirSync(OUTPUT_ROOT, { recursive: true });
  fs.writeFileSync(indexPath, JSON.stringify(payload, null, 2), "utf8");
}

async function processStore(page, store, options) {
  const { maxPages, maxStoreMinutes } = options;
  const storeStartedAt = Date.now();
  const debugPaths = [];

  const ensureStoreTime = () => {
    if (hasExceededMaxRun(storeStartedAt, maxStoreMinutes)) {
      console.warn(`⏹️ MAX_STORE_MINUTES=${maxStoreMinutes} reached for store. Exiting.`);
      process.exit(0);
    }
  };

  try {
    ensureStoreTime();
    await navigateToSale(page);
    ensureStoreTime();
    await setMyStore(page, store);
    ensureStoreTime();
    await navigateToSale(page);
    debugPaths.push(...(await captureDebug(page, store.slug, "after_store")));
    try {
      const enabled = await enableAvailableInMyStoreFilter(page);
      if (!enabled) {
        console.warn("[PA] Filter failed (non-fatal): unavailable");
      }
    } catch (error) {
      console.warn("[PA] Filter failed (non-fatal):", error);
    }
    await scrollProductListIntoView(page);
    debugPaths.push(...(await captureDebug(page, store.slug, "after_filter")));
    ensureStoreTime();
    await loadAllPages(page, maxPages, storeStartedAt, maxStoreMinutes);
    const resultsText = await page
      .locator("text=/Results\s+\d+\s*-\s*\d+\s+of\s+\d+/i")
      .first()
      .textContent()
      .catch(() => null);
    let totalResultCount = null;
    if (resultsText) {
      const totalMatch = resultsText.match(/of\s+(\d+)/i);
      if (totalMatch?.[1]) {
        totalResultCount = Number(totalMatch[1]);
      }
      console.log(`ℹ️ Results summary: ${(resultsText || "").trim()}`);
    }

    const products = await extractProducts(page);

    const hasNoResultsText = await page
      .locator("text=/No results/i, text=/0 results/i, text=/No items/i")
      .first()
      .isVisible({ timeout: 2000 })
      .catch(() => false);

    const isZeroLegit = hasNoResultsText || totalResultCount === 0;
    const shouldKeepDebug = products.length === 0;
    if (shouldKeepDebug) {
      const reason = isZeroLegit
        ? "Page reports zero results"
        : "Page indicates results but extraction returned 0";
      console.warn(`⚠️ No products extracted for ${store.slug}. ${reason}. Keeping debug.`);
      await uploadDebugArtifactsIfAvailable(debugPaths);
    } else {
      deleteDebugArtifacts(debugPaths);
    }

    return writeStoreOutput(store, products);
  } catch (error) {
    console.error(`⚠️ Store failed (${store.slug}):`, error);
    return null;
  }
}

async function main() {
  ensureOutputsRoot();

  const maxRunMinutes = Number(process.env.MAX_RUN_MINUTES ?? "150");
  const maxStoreMinutes = Number(process.env.MAX_STORE_MINUTES ?? "20");
  const maxPages = Number(process.env.MAX_PAGES ?? "50");
  const startedAt = Date.now();
  const allStores = loadStores();
  const stores = getStoresForThisShard(allStores);
  const softTimeout = startSoftTimeout(maxRunMinutes, () => activeBrowser);

  if (stores.length === 0) {
    console.log("🟡 No stores assigned to this shard. Exiting cleanly.");
    process.exit(0);
  }

  activeBrowser = await chromium.launch({ headless: true });
  const indexEntries = [];

  try {
    const page = await activeBrowser.newPage({ viewport: { width: 1280, height: 720 } });

    for (const store of stores) {
      if (hasExceededMaxRun(startedAt, maxRunMinutes)) {
        console.warn(
          `⏹️ MAX_RUN_MINUTES=${maxRunMinutes} reached mid-run. Stopping shard cleanly.`
        );
        process.exit(0);
      }

      const result = await processStore(page, store, { maxPages, maxStoreMinutes });

      if (result) {
        indexEntries.push({
          storeId: result.storeId,
          storeName: result.storeName,
          city: result.city,
          province: result.province,
          address: result.address,
          slug: result.slug,
          productCount: result.products.length,
          updatedAt: result.updatedAt,
          dataPath: path.relative(ROOT_DIR, path.join(OUTPUT_ROOT, result.slug, "data.json")),
        });
        updateIndex(indexEntries);
      }
    }
  } catch (error) {
    console.error("❌ Global scraper error:", error);
    process.exitCode = 1;
  } finally {
    if (softTimeout) clearTimeout(softTimeout);
    if (activeBrowser) await activeBrowser.close();
  }
}

main();
