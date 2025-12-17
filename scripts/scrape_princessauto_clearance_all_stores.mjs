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

const MAX_PAGES_FALLBACK = 50;

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

async function getActivePageNumber(page) {
  const current = page.locator("[aria-current='page']").first();
  if ((await current.count().catch(() => 0)) === 0) return null;
  const visible = await current.isVisible({ timeout: 1500 }).catch(() => false);
  if (!visible) return null;
  const text = (await current.textContent().catch(() => null))?.trim();
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

async function getFirstProductSignature(page) {
  return page
    .evaluate(() => {
      const anchor =
        document.querySelector("a[href*='/product/'], a[href*='/p/']") ||
        document.querySelector("[data-testid='product-tile'] a");
      if (!anchor) return null;
      const title =
        anchor.getAttribute("title") ||
        anchor.getAttribute("aria-label") ||
        anchor.textContent?.trim() ||
        "";
      const href = anchor.getAttribute("href") || "";
      return `${title}::${href}`.trim();
    })
    .catch(() => null);
}

async function findNextButton(page) {
  const candidateSelectors = [
    "a[aria-label='Next']",
    "button[aria-label='Next']",
    "a:has-text('Next')",
    "button:has-text('Next')",
    "[class*='pagination'] a:has-text('>')",
    "[class*='pagination'] button:has-text('>')",
    "[role='navigation'] button:has-text('>')",
    "nav button:has-text('>')",
    "nav a:has-text('>')",
  ];

  for (const selector of candidateSelectors) {
    const locator = page.locator(selector).first();
    if ((await locator.count().catch(() => 0)) === 0) continue;
    const visible = await locator.isVisible({ timeout: 1500 }).catch(() => false);
    const disabled = await locator.isDisabled().catch(() => false);
    if (visible && !disabled) {
      return locator;
    }
  }
  return null;
}

async function waitForSignatureChange(page, previousSignature, previousPageIndicator, timeout = 15000) {
  return page
    .waitForFunction(
      ([prevSig, prevPage]) => {
        const anchor =
          document.querySelector("a[href*='/product/'], a[href*='/p/']") ||
          document.querySelector("[data-testid='product-tile'] a");
        const title =
          anchor?.getAttribute("title") ||
          anchor?.getAttribute("aria-label") ||
          anchor?.textContent?.trim() ||
          "";
        const href = anchor?.getAttribute("href") || "";
        const signature = anchor ? `${title}::${href}`.trim() : null;

        const currentPage = Array.from(document.querySelectorAll("[aria-current='page']"))
          .map((el) => el.textContent?.trim())
          .find(Boolean);

        const signatureChanged = signature && signature !== prevSig;
        const pageChanged = currentPage && currentPage !== prevPage;

        return signatureChanged || pageChanged;
      },
      [previousSignature, previousPageIndicator],
      { timeout }
    )
    .then(() => true)
    .catch(() => false);
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


async function loadAllPages(page, store, maxPages, storeStartedAt, maxStoreMinutes) {
  const maxPagesSafe = Number.isFinite(maxPages) && maxPages > 0 ? maxPages : MAX_PAGES_FALLBACK;
  const allProducts = [];
  const seenUrls = new Set();
  let currentSignature = await getFirstProductSignature(page);
  let currentPageIndicator = await getActivePageNumber(page);

  for (let pageIndex = 1; pageIndex <= maxPagesSafe; pageIndex++) {
    if (hasExceededMaxRun(storeStartedAt, maxStoreMinutes)) {
      console.warn(
        `⏹️ MAX_STORE_MINUTES=${maxStoreMinutes} reached for store. Exiting cleanly.`
      );
      process.exit(0);
    }

    await waitForProductsGrid(page);
    const sigBefore = currentSignature ?? (await getFirstProductSignature(page));
    const pageProducts = await extractProducts(page);
    for (const product of pageProducts) {
      if (product.url && !seenUrls.has(product.url)) {
        seenUrls.add(product.url);
        allProducts.push(product);
      }
    }

    const activePage = (await getActivePageNumber(page)) ?? currentPageIndicator ?? pageIndex;
    const truncatedSignature = (sigBefore || "n/a").slice(0, 120);
    console.log(
      `[PA] ${store.slug} page=${activePage} extracted=${pageProducts.length} first="${truncatedSignature}"`
    );

    const nextButton = await findNextButton(page);
    if (!nextButton) {
      break;
    }

    await nextButton.scrollIntoViewIfNeeded().catch(() => {});

    let advanced = false;
    for (let attempt = 1; attempt <= 2; attempt++) {
      const previousSignature = sigBefore;
      const previousIndicator = currentPageIndicator;
      await nextButton.click({ timeout: 10000 }).catch(() => {});
      const signatureChanged = await waitForSignatureChange(
        page,
        previousSignature,
        previousIndicator,
        15000
      );
      if (signatureChanged) {
        currentSignature = await getFirstProductSignature(page);
        currentPageIndicator = await getActivePageNumber(page);
        advanced = true;
        break;
      }
      await nextButton.scrollIntoViewIfNeeded().catch(() => {});
      await page.waitForTimeout(500);
    }

    if (!advanced) {
      console.warn(
        `[PA] Pagination stopped for ${store.slug}: signature did not change after clicking Next.`
      );
      break;
    }
  }

  return allProducts;
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
        image: imageUrl,
        priceRegular: originalPriceText || null,
        priceSale: currentPriceText || null,
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
  const csvFile = path.join(storeDir, "data.csv");

  fs.mkdirSync(storeDir, { recursive: true });

  const csvHeaders = ["title", "image", "url", "priceRegular", "priceSale", "storeSlug", "storeName"];
  const csvRows = [csvHeaders.join(",")];

  const escapeCsv = (value) => {
    if (value === null || value === undefined) return "";
    const str = String(value);
    if (/[",\n]/.test(str)) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  };

  for (const product of products) {
    const row = csvHeaders
      .map((key) => escapeCsv(Object.prototype.hasOwnProperty.call(product, key) ? product[key] : ""))
      .join(",");
    csvRows.push(row);
  }

  fs.writeFileSync(csvFile, csvRows.join("\n"), "utf8");

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
    await scrollProductListIntoView(page);
    debugPaths.push(...(await captureDebug(page, store.slug, "after_filter")));
    ensureStoreTime();
    const allProducts = await loadAllPages(page, store, maxPages, storeStartedAt, maxStoreMinutes);
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

    const allProductsWithStore = allProducts.map((product) => ({
      ...product,
      storeSlug: store.slug,
      storeName: store.name || store.storeName || null,
    }));

    const hasNoResultsText = await page
      .locator("text=/No results/i, text=/0 results/i, text=/No items/i")
      .first()
      .isVisible({ timeout: 2000 })
      .catch(() => false);

    const isZeroLegit = hasNoResultsText || totalResultCount === 0;
    if (allProductsWithStore.length === 0) {
      const reason = isZeroLegit
        ? "Page reports zero results"
        : "Page indicates results but extraction returned 0";
      console.warn(`⚠️ No products extracted for ${store.slug}. ${reason}. Keeping debug.`);
      await uploadDebugArtifactsIfAvailable(debugPaths);
    } else {
      deleteDebugArtifacts(debugPaths);
    }

    return writeStoreOutput(store, allProductsWithStore);
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
