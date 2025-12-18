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
const CONCURRENCY = Number(process.env.PA_CONCURRENCY ?? "1");
const STORES_JSON = path.join(ROOT_DIR, "public", "princessauto", "stores.json");
const OUTPUT_ROOT = path.join(ROOT_DIR, "outputs", "princessauto");
const DEBUG_OUTPUT_DIR = path.join(ROOT_DIR, "outputs", "debug");
let activeBrowser = null;

const PRODUCT_TILE_SELECTOR =
  "[data-testid='product-tile'], [data-testid='product-card'], article[data-product-id], li.product-tile, div.product-tile, div.product-grid__item, li.product-grid__item";
const PRODUCT_LINK_SELECTORS = [
  "[data-testid='product-tile'] a[href*='/product/']",
  "[data-testid='product-card'] a[href*='/product/']",
  "a[href*='/product/']",
  "a[href*='/p/']",
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

async function getFirstGridHref(page) {
  const href = await page.evaluate(
    ({ tileSelector, linkSelectors }) => {
      const tiles = Array.from(document.querySelectorAll(tileSelector));
      const tile = tiles.find((el) =>
        linkSelectors.some((selector) => el.querySelector(selector))
      );
      if (!tile) return null;
      const firstAnchor = linkSelectors
        .map((selector) => tile.querySelector(selector))
        .find(Boolean);
      const rawHref = firstAnchor?.getAttribute("href") || firstAnchor?.href || null;
      if (!rawHref) return null;
      try {
        return new URL(rawHref, document.baseURI).href;
      } catch (error) {
        return null;
      }
    },
    { tileSelector: PRODUCT_TILE_SELECTOR, linkSelectors: PRODUCT_LINK_SELECTORS }
  );

  return href ? href.split("#")[0] : null;
}

async function getProductTileCount(page) {
  return page.evaluate((tileSelector, linkSelectors) => {
    return Array.from(document.querySelectorAll(tileSelector)).filter((tile) =>
      linkSelectors.some((selector) => tile.querySelector(selector))
    ).length;
  }, PRODUCT_TILE_SELECTOR, PRODUCT_LINK_SELECTORS);
}

async function waitForGridChange(page, previousHref, previousCount) {
  const contentChangeDetected = await page
    .waitForFunction(
      ({ tileSelector, linkSelectors, prevHref, prevCount }) => {
        const tiles = Array.from(document.querySelectorAll(tileSelector)).filter((tile) =>
          linkSelectors.some((selector) => tile.querySelector(selector))
        );
        if (tiles.length === 0) return false;

        const firstAnchor = linkSelectors
          .map((selector) => tiles[0].querySelector(selector))
          .find(Boolean);
        const href = firstAnchor?.getAttribute("href") || firstAnchor?.href || null;
        let normalizedHref = null;
        if (href) {
          try {
            normalizedHref = new URL(href, document.baseURI).href.split("#")[0];
          } catch (error) {
            normalizedHref = null;
          }
        }

        return (normalizedHref && normalizedHref !== prevHref) || tiles.length !== prevCount;
      },
      { tileSelector: PRODUCT_TILE_SELECTOR, linkSelectors: PRODUCT_LINK_SELECTORS, prevHref: previousHref, prevCount: previousCount },
      { timeout: 15000 }
    )
    .catch(() => false);

  if (!contentChangeDetected) {
    await page
      .waitForResponse(
        (resp) => /search|product/i.test(resp.url()) && resp.request().method() === "GET",
        { timeout: 10000 }
      )
      .catch(() => null);
  }

  return contentChangeDetected;
}

async function dismissMakeStoreModal(page) {
  const modal = page.locator("#makeStoreModal");
  if (!(await modal.count())) return;

  try {
    await modal.waitFor({ state: "visible", timeout: 1500 });
  } catch {
    return;
  }

  const ok = modal.getByRole("button", { name: /^ok$/i });
  if (await ok.count()) {
    await ok.click({ timeout: 5000 }).catch(() => {});
  } else {
    const closeBtn = modal
      .locator(
        "button.close, .modal-header button, [aria-label='Close'], [aria-label='Fermer']"
      )
      .first();

    if (await closeBtn.count()) {
      await closeBtn.click({ timeout: 5000 }).catch(() => {});
    } else {
      await page.keyboard.press("Escape").catch(() => {});
    }
  }

  await modal.waitFor({ state: "hidden", timeout: 8000 }).catch(() => {});
}

async function clickNextAndWaitForChange(page, previousHref, previousCount) {
  const nextBtn = page
    .locator(
      "a[aria-label='Next'], button[aria-label='Next'], a[rel='next'], .pagination-next a, .pagination-next button, a:has-text('Next'), button:has-text('Next')"
    )
    .first();

  if (!(await nextBtn.count())) {
    return false;
  }

  console.log("🔘 Clicking Next pagination control");

  const [contentChanged] = await Promise.all([
    waitForGridChange(page, previousHref, previousCount),
    page
      .waitForResponse((resp) => /search|product/i.test(resp.url()), { timeout: 15000 })
      .catch(() => null),
    nextBtn.click({ timeout: 10000 }),
  ]);

  await page.waitForLoadState("networkidle").catch(() => {});

  return contentChanged !== false;
}

async function navigateToSale(page) {
  console.log(`➡️ Navigating to sale page: ${SALE_URL}`);
  await page.goto(SALE_URL, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle").catch(() => {});
  await waitForProductsGrid(page);
}

function escapeRegex(text = "") {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractPostalFromAddress(address = "") {
  const postalMatch = address.match(/[A-Z]\d[A-Z]\s?\d[A-Z]\d/i);
  return postalMatch ? postalMatch[0].toUpperCase().replace(/\s+/, " ") : null;
}

function normalizePostal(p) {
  if (!p) return "";
  const s = String(p).toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (s.length === 6) return s.slice(0, 3) + " " + s.slice(3);
  return p.toUpperCase().trim();
}

async function ensureOnSalePage(page) {
  let recovered = false;
  if (page.url().includes("/locations")) {
    console.warn("⚠️ Detected /locations page; navigating back to Sale");
    await page.goto(SALE_URL, { waitUntil: "domcontentloaded" });
    recovered = true;
  }

  await page.waitForLoadState("domcontentloaded");
  await page.waitForTimeout(800);

  const productSelectors = [
    "a[href*='/product']",
    "a[href*='/produit']",
    "[data-testid*='product']",
    ".product",
  ];

  let productVisible = false;
  try {
    await page.waitForSelector(productSelectors.join(", "), { timeout: 30000 });
    const locator = page.locator(productSelectors.join(", ")).first();
    productVisible = await locator.isVisible({ timeout: 500 }).catch(() => false);
  } catch (error) {
    productVisible = false;
  }

  return { recovered, productVisible };
}

async function setStoreThenGoToSale(page, store, debugPaths = []) {
  const postal = normalizePostal(store.postalCode || store.postal || store.zip);
  const city = store.city || "";

  console.log(`Setting store using Locations page => postal=${postal} city=${city}`);

  await page.goto("https://www.princessauto.com/en/locations?origin=header", {
    waitUntil: "domcontentloaded",
  });

  await dismissMakeStoreModal(page);

  const input = page.locator("#addressInput").first();
  await input.waitFor({ state: "visible", timeout: 15000 });

  await input.click();
  await input.press("Control+A").catch(() => {});
  await input.type(city, { delay: 40 });
  await page.waitForTimeout(350);
  debugPaths.push(...(await captureDebug(page, store.slug, "after_type")));

  const suggestion = page
    .locator(
      "[role=\"option\"], .autocomplete li, .tt-suggestion, ul[role=\"listbox\"] li, .pac-item"
    )
    .filter({ hasText: new RegExp(city, "i") })
    .first();

  if (await suggestion.count()) {
    await suggestion.click({ timeout: 3000 }).catch(() => {});
  } else {
    await page.keyboard.press("Enter").catch(() => {});
  }
  await page.waitForTimeout(1200);
  debugPaths.push(...(await captureDebug(page, store.slug, "after_suggestion")));

  const makeBtns = page.getByRole("button", {
    name: /faire\s+de\s+.*\s+mon\s+magasin|make\s+.*\s+my\s+store/i,
  });

  const firstMakeBtn = makeBtns.first();
  try {
    await firstMakeBtn.waitFor({ state: "visible", timeout: 25000 });
  } catch (error) {
    console.warn("⚠️ Make my store buttons did not become visible within timeout", error);
  }
  debugPaths.push(...(await captureDebug(page, store.slug, "after_make_buttons")));

  if (!(await makeBtns.count())) {
    throw new Error("Make my store buttons not found on Locations page");
  }

  let target = makeBtns.first();

  const scoped = page.locator("body").locator(":scope").filter({ hasText: new RegExp(city, "i") });
  const inCity = scoped.getByRole("button", {
    name: /faire\s+de\s+.*\s+mon\s+magasin|make\s+.*\s+my\s+store/i,
  }).first();
  if (await inCity.count()) target = inCity;

  await target.click();
  debugPaths.push(...(await captureDebug(page, store.slug, "after_click_make_store")));
  await dismissMakeStoreModal(page);
  debugPaths.push(...(await captureDebug(page, store.slug, "after_modal_dismiss")));

  const saleLink = page.getByRole("link", { name: /vente|sale/i }).first();
  if (await saleLink.isVisible({ timeout: 5000 }).catch(() => false)) {
    await saleLink.click({ timeout: 10000 }).catch(() => {});
  } else {
    await page.goto(SALE_URL, { waitUntil: "domcontentloaded" });
  }
  if (page.url().includes("/locations")) {
    await page.goto(SALE_URL, { waitUntil: "domcontentloaded" });
  }

  await ensureOnSalePage(page);
}


async function loadProductsByPagination(page, store, debugPaths = []) {
  const allProducts = [];
  const seen = new Set();
  let prevSignature = null;
  let zeroGainStreak = 0;
  let recoveredFromLocations = false;

  for (let pageNum = 1; pageNum <= MAX_PAGES; pageNum++) {
    try {
      const { recovered, productVisible } = await ensureOnSalePage(page);
      recoveredFromLocations = recoveredFromLocations || recovered;
      await page.waitForLoadState("networkidle").catch(() => {});
      await waitForProductsGrid(page);

      let productsOnPage = await extractProducts(page);
      let productCount = productsOnPage.length;

      if (
        productsOnPage.length === 0 &&
        pageNum === 1 &&
        (recovered || recoveredFromLocations || !productVisible)
      ) {
        console.warn("⚠️ No products on first page after recovery attempt; retrying once...");
        await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
        await ensureOnSalePage(page);
        await page.waitForLoadState("networkidle").catch(() => {});
        await waitForProductsGrid(page);
        productsOnPage = await extractProducts(page);
        productCount = productsOnPage.length;
      }

      const currentUrlNoHash = page.url().split("#")[0];
      const firstProductHref = productsOnPage[0]?.url || (await getFirstGridHref(page));
      const currentPageUrls = productsOnPage
        .map((product) => product.url || product.id || product.title || "")
        .filter(Boolean);
      const signatureParts = [
        currentPageUrls.slice(0, 10).join("|"),
        currentPageUrls.slice(-10).join("|"),
      ];
      const pageSignature = signatureParts.join("||");

      console.log(
        `📄 Page ${pageNum}: currentUrlNoHash=${currentUrlNoHash} | products=${productCount} | firstProductHref=${firstProductHref || "none"}`
      );

      if (prevSignature && pageSignature === prevSignature) {
        console.log("🛑 Stop pagination: repeated page content detected");
        break;
      }
      prevSignature = pageSignature;

      let newUniqueCount = 0;
      for (const product of productsOnPage) {
        if (product.url && !seen.has(product.url)) {
          seen.add(product.url);
          allProducts.push(product);
          newUniqueCount += 1;
        }
      }

      if (newUniqueCount === 0) {
        zeroGainStreak += 1;
        if (zeroGainStreak >= 2) {
          console.log(
            "🛑 Stop pagination: zero new unique products found on two consecutive pages"
          );
          break;
        }
      } else {
        zeroGainStreak = 0;
      }

      console.log(
        `➡️ Next page ${pageNum + 1 <= MAX_PAGES ? pageNum + 1 : "-"}: via pagination control`
      );

      if (productsOnPage.length === 0) {
        console.log(`🛑 Stop pagination: no products on page ${pageNum}`);
        break;
      }

      if (pageNum === MAX_PAGES) {
        console.log(`🛑 Stop pagination: MAX_PAGES=${MAX_PAGES} reached`);
        break;
      }

      const gridChanged = await clickNextAndWaitForChange(
        page,
        firstProductHref,
        productCount || (await getProductTileCount(page))
      );

      if (!gridChanged) {
        console.log(
          `🛑 Stop pagination: Next control unavailable or grid unchanged on page ${pageNum}`
        );
        break;
      }

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
  const { products, tileCount, visibleProducts } = await page.evaluate(
    ({ productSelector, selectors }) => {
      const tiles = Array.from(document.querySelectorAll(productSelector)).filter((tile) =>
        selectors.some((selector) => tile.querySelector(selector))
      );
      const deduped = new Map();
      const visibleKeys = new Set();
      const viewportWidth = window.innerWidth || document.documentElement.clientWidth;
      const viewportHeight = window.innerHeight || document.documentElement.clientHeight;

      for (const tile of tiles) {
        const anchor = selectors
          .map((selector) => tile.querySelector(selector))
          .find((el) => el?.getAttribute("href") || el?.href);
        if (!anchor) continue;
        const href = anchor.getAttribute("href") || anchor.href;
        if (!href) continue;
        let url = null;
        try {
          url = new URL(href, document.baseURI).href.split("#")[0];
        } catch (error) {
          continue;
        }

        if (!url || deduped.has(url)) continue;

        const getText = (selector) => tile.querySelector(selector)?.textContent?.trim() || null;

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

        const rect = tile.getBoundingClientRect();
        const isVisible =
          rect.width > 0 &&
          rect.height > 0 &&
          rect.bottom > 0 &&
          rect.right > 0 &&
          rect.top < viewportHeight &&
          rect.left < viewportWidth;

        if (isVisible) {
          visibleKeys.add(url);
        }

        deduped.set(url, {
          title,
          url,
          image: imageUrl,
          priceRegular: originalPriceText || null,
          priceSale: currentPriceText || null,
        });
      }

      return {
        tileCount: tiles.length,
        products: Array.from(deduped.values()),
        visibleProducts: visibleKeys.size,
      };
    },
    { productSelector: PRODUCT_TILE_SELECTOR, selectors: PRODUCT_LINK_SELECTORS }
  );

  console.log(`🧱 Product tiles detected before extraction: ${tileCount}`);
  console.log(`🫧 Visible products in viewport: ${visibleProducts}`);
  console.log(`Products extracted: ${products.length}`);
  return products;
}

function writeStoreOutput(store, products, storeSynced = true) {
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
    storeSynced,
    products,
  };

  fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2), "utf8");

  console.log(`💾 Wrote ${products.length} product(s) for ${store.slug} → ${outputFile}`);
  return payload;
}

async function captureDebug(page, slug, stage, options = {}) {
  const { extraNotes = [] } = options;
  const htmlPath = path.join(DEBUG_OUTPUT_DIR, `pa_${slug}_${stage}.html`);
  const screenshotPath = path.join(DEBUG_OUTPUT_DIR, `pa_${slug}_${stage}.png`);

  try {
    const html = await page.content();
    const htmlWithNotes =
      extraNotes.length > 0
        ? `${html}\n<!-- DEBUG NOTES:\n${extraNotes.join("\n")}\n-->`
        : html;
    fs.writeFileSync(htmlPath, htmlWithNotes, "utf8");
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
  let storeSynced = true;

  try {
    await setStoreThenGoToSale(page, store, debugPaths);
    await ensureOnSalePage(page);
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

    const shouldKeepDebug = allProductsWithStore.length === 0 || !storeSynced;

    if (shouldKeepDebug) {
      const reason =
        allProductsWithStore.length === 0
          ? isZeroLegit
            ? "Page reports zero results"
            : "Page indicates results but extraction returned 0"
          : !storeSynced
            ? "Store sync unresolved"
            : "Potential mismatch between page and extraction";
      console.warn(`⚠️ Keeping debug for ${store.slug}. ${reason}.`);
      await uploadDebugArtifactsIfAvailable(debugPaths);
    } else {
      deleteDebugArtifacts(debugPaths);
    }

    return writeStoreOutput(store, allProductsWithStore, storeSynced);
  } catch (error) {
    console.error(`⚠️ Store failed (${store.slug}):`, error);
    debugPaths.push(...(await captureDebug(page, store.slug, "store_failed")));
    await uploadDebugArtifactsIfAvailable(debugPaths);
    return null;
  }
}

async function processStoreWithIsolatedContext(browser, store) {
  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
  });
  const page = await context.newPage();

  try {
    return await processStore(page, store);
  } finally {
    await context.close();
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

  console.log(`[PA] Processing stores with concurrency=${CONCURRENCY}`);
  activeBrowser = await chromium.launch({ headless: true });
  const indexEntries = [];
  let indexUpdateQueue = Promise.resolve();
  let storeIndex = 0;
  let stopRequested = false;

  try {
    const worker = async () => {
      while (true) {
        if (stopRequested || hasExceededMaxRun(startedAt, maxRunMinutes)) {
          if (!stopRequested) {
            console.warn(
              `⏹️ MAX_RUN_MINUTES=${maxRunMinutes} reached mid-run. Stopping shard cleanly.`
            );
          }
          stopRequested = true;
          return;
        }

        const currentIndex = storeIndex++;
        if (currentIndex >= stores.length) return;
        const store = stores[currentIndex];

        const result = await processStoreWithIsolatedContext(activeBrowser, store);

        if (result) {
          indexUpdateQueue = indexUpdateQueue.then(async () => {
            indexEntries.push({
              storeId: result.storeId,
              storeName: result.storeName,
              city: result.city,
              province: result.province,
              address: result.address,
              slug: result.slug,
              productCount: result.products.length,
              updatedAt: result.updatedAt,
              dataPath: path.relative(
                ROOT_DIR,
                path.join(OUTPUT_ROOT, result.slug, "data.json")
              ),
            });
            updateIndex(indexEntries);
          });

          await indexUpdateQueue;
        }
      }
    };

    const workers = Array.from(
      { length: Math.min(Math.max(CONCURRENCY, 1), stores.length) },
      () => worker()
    );

    await Promise.all(workers);
  } catch (error) {
    console.error("❌ Global scraper error:", error);
    process.exitCode = 1;
  } finally {
    if (softTimeout) clearTimeout(softTimeout);
    if (activeBrowser) await activeBrowser.close();
  }
}

main();
