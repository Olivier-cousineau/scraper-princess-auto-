import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

const SALE_URL = "https://www.princessauto.com/en/category/Sale";
const NRPP = parseInt(process.env.NRPP || "50", 10);
const MAX_PAGES = parseInt(process.env.MAX_PAGES || "100", 10);
const STORES_JSON = path.join(ROOT_DIR, "public", "princessauto", "stores.json");
const OUTPUT_ROOT = path.join(ROOT_DIR, "outputs", "princessauto");
const DEBUG_OUTPUT_DIR = path.join(ROOT_DIR, "outputs", "debug");
let activeBrowser = null;

const PRODUCT_TILE_SELECTOR =
  "[data-testid='product-tile'], li.product-tile, div.product-tile, div.product-grid__item, li.product-grid__item";
const PRODUCT_LINK_SELECTORS = [
  "a[href*='/product/']",
  "a[href*='/p/']",
  ".product-tile a",
  "[data-testid*='product'] a",
];

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
  const productSelectors = [...PRODUCT_LINK_SELECTORS, PRODUCT_TILE_SELECTOR];
  const emptySelectors = ["text=No products", "text=No items", "text=0 items"];

  try {
    await page.waitForSelector(productSelectors.join(", "), { timeout: 30000 });
  } catch (error) {
    for (const selector of emptySelectors) {
      const emptyState = page.locator(selector).first();
      if (await emptyState.isVisible({ timeout: 2000 }).catch(() => false)) {
        break;
      }
    }
  }
}

async function findNextHref(page) {
  const selectors = [
    "a[rel='next']",
    "a[aria-label*='next' i]",
    "a[title*='next' i]",
    ".pagination-next a",
    "a.pagination-next",
    "nav.pagination a[aria-label*='next' i]",
  ];

  const nextHref = await page.evaluate((nextSelectors) => {
    for (const selector of nextSelectors) {
      const el = document.querySelector(selector);
      if (!el) continue;
      const href = el.getAttribute("href") || el.getAttribute("data-href") || "";
      if (!href) continue;
      try {
        return new URL(href, document.baseURI).href;
      } catch (error) {
        continue;
      }
    }
    return null;
  }, selectors);

  return nextHref;
}

async function clickNextButtonIfAvailable(page, previousKey) {
  const selectors = [
    "button[aria-label*='next' i]",
    "button[title*='next' i]",
    ".pagination-next button",
    "button:has-text('Next')",
    "button:has-text('Suivant')",
  ];

  for (const selector of selectors) {
    const button = page.locator(selector).first();
    if (!(await button.isVisible({ timeout: 500 }).catch(() => false))) continue;
    if (await button.isDisabled().catch(() => false)) continue;

    console.log(`🔘 Clicking Next button via selector: ${selector}`);
    await button.click({ timeout: 10000 }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});

    const changed = await page
      .waitForFunction(
        (prevKey, selectors) => {
          const currentUrlNoHash = location.href.split("#")[0];
          const firstAnchor = Array.from(
            document.querySelectorAll(selectors.join(", "))
          ).find((el) => el.getAttribute("href") || el.href);
          const href = firstAnchor?.getAttribute("href") || firstAnchor?.href || "";
          const firstProductHref = href ? new URL(href, document.baseURI).href : "";
          const key = `${currentUrlNoHash}::${firstProductHref}`;
          return key !== prevKey;
        },
        previousKey,
        PRODUCT_LINK_SELECTORS,
        { timeout: 10000 }
      )
      .catch(() => false);

    if (changed) {
      console.log("✅ Detected page change after Next button click");
      return true;
    }

    console.warn("⚠️ Next button click did not change the page; trying other selectors");
  }

  return false;
}

async function navigateToSale(page) {
  console.log(`➡️ Navigating to sale page: ${SALE_URL}`);
  await page.goto(SALE_URL, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle").catch(() => {});
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


async function loadProductsByPagination(page, store, debugPaths = []) {
  const allProducts = [];
  const seen = new Set();
  const seenPages = new Set();
  let useNoPagination = false;
  let offset = Number(new URL(page.url()).searchParams.get("No") || "0");

  for (let pageNum = 1; pageNum <= MAX_PAGES; pageNum++) {
    try {
      await page.waitForLoadState("domcontentloaded");
      await page.waitForLoadState("networkidle").catch(() => {});
      await waitForProductsGrid(page);

      const productsOnPage = await extractProducts(page);
      const currentUrlNoHash = page.url().split("#")[0];
      const firstProductHref = productsOnPage[0]?.url || null;
      const pageKey = `${currentUrlNoHash}::${firstProductHref || ""}`;
      const isRepeatedPage = seenPages.has(pageKey);
      seenPages.add(pageKey);

      console.log(
        `📄 Page ${pageNum}: currentUrlNoHash=${currentUrlNoHash} | products=${productsOnPage.length} | firstProductHref=${firstProductHref || "none"}`
      );

      for (const product of productsOnPage) {
        if (product.url && !seen.has(product.url)) {
          seen.add(product.url);
          allProducts.push(product);
        }
      }

      const nextHref = await findNextHref(page);
      const nextUrlNoHash = nextHref ? nextHref.split("#")[0] : null;
      const hasNoLinks = await page
        .locator("a[href*='No='], a[href*='nrpp='], a[href*='NRPP=']")
        .count()
        .then((c) => c > 0)
        .catch(() => false);

      let resolvedNextHref = nextHref;
      if (
        resolvedNextHref === "#" ||
        (resolvedNextHref && resolvedNextHref.endsWith("#")) ||
        (nextUrlNoHash && nextUrlNoHash === currentUrlNoHash)
      ) {
        resolvedNextHref = null;
      }

      if (!resolvedNextHref && (useNoPagination || hasNoLinks)) {
        useNoPagination = true;
        offset += NRPP;
        const u = new URL(page.url());
        u.searchParams.set("Nrpp", String(NRPP));
        u.searchParams.set("No", String(offset));
        u.searchParams.delete("page");
        resolvedNextHref = u.toString();
      }

      console.log(
        `➡️ Next page ${pageNum + 1 <= MAX_PAGES ? pageNum + 1 : "-"}: nextHref=${
          nextHref || "none"
        } | nextUrlNoHash=${nextUrlNoHash || "none"} | resolved=${resolvedNextHref || "none"}`
      );

      if (isRepeatedPage) {
        console.log("🛑 Stop pagination: repeated page detected");
        break;
      }

      if (productsOnPage.length === 0) {
        console.log(`🛑 Stop pagination: no products on page ${pageNum}`);
        break;
      }

      if (pageNum === MAX_PAGES) {
        console.log(`🛑 Stop pagination: MAX_PAGES=${MAX_PAGES} reached`);
        break;
      }

      if (!resolvedNextHref) {
        const clicked = await clickNextButtonIfAvailable(page, pageKey);
        if (!clicked) {
          console.log(
            `🛑 Stop pagination: no valid next link or button on page ${pageNum}`
          );
          break;
        }
        continue;
      }

      await page.goto(resolvedNextHref, { waitUntil: "domcontentloaded" });
      await page.waitForLoadState("networkidle").catch(() => {});
      await waitForProductsGrid(page);
    } catch (error) {
      console.error(`⚠️ Failed to process page ${pageNum} for ${store.slug}:`, error);
      debugPaths.push(...(await captureDebug(page, store.slug, `page_${pageNum}_error`)));
      break;
    }
  }

  console.log(`[PA] ${store.slug} total products across pages=${allProducts.length}`);
  return allProducts;
}

async function extractProducts(page) {
  console.log("🔍 Extracting products...");

  const productAnchorCount = await page
    .locator(PRODUCT_LINK_SELECTORS.join(", "))
    .count()
    .catch(() => 0);
  console.log(`🔗 Product anchors detected before extraction: ${productAnchorCount}`);

  const products = await page.evaluate(({ productSelector, selectors }) => {
    const anchors = Array.from(document.querySelectorAll(selectors.join(", ")));
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
  }, { productSelector: PRODUCT_TILE_SELECTOR, selectors: PRODUCT_LINK_SELECTORS });

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

async function processStore(page, store) {
  const debugPaths = [];

  try {
    await navigateToSale(page);
    await setMyStore(page, store);
    await navigateToSale(page);
    debugPaths.push(...(await captureDebug(page, store.slug, "after_store")));
    const allProducts = await loadProductsByPagination(page, store, debugPaths);
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

      const result = await processStore(page, store);

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
