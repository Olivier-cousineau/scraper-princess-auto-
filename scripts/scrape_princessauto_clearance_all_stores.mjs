import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const BASE_URL = "https://www.princessauto.com";
const SALE_BASE_URL = "https://www.princessauto.com/en/category/Sale";
const NRPP = parseInt(process.env.NRPP || "50", 10);
const SALE_URL = withInStoreFacet(SALE_BASE_URL);
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

function withInStoreFacet(url) {
  const u = new URL(url);
  u.searchParams.set("Nrpp", String(NRPP));
  u.searchParams.set("facet.availability", "56");
  return u.toString();
}

function attachJsonResponseCollector(page) {
  const collected = [];
  page.on("response", async (res) => {
    try {
      const url = res.url();
      const ct = (res.headers()["content-type"] || "").toLowerCase();

      if (!ct.includes("application/json")) return;
      if (!/cnstrc|constructor|algolia|search|browse/i.test(url)) return;

      const json = await res.json();
      collected.push({ url, json });
    } catch {}
  });
  return collected;
}

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

async function preparePageForExtraction(page) {
  await page.waitForLoadState("domcontentloaded");
  await page.waitForTimeout(1200);

  for (let i = 0; i < 6; i++) {
    await page.mouse.wheel(0, 2200);
    await page.waitForTimeout(700);
  }

  const productCandidates = [
    'a[href*="/product/"]',
    'a[href*="/produit/"]',
    'a[href*="/p/"]',
    "[data-testid*='product'] a",
    ".product a",
    ".product-tile a",
    ".productTile a",
  ];

  for (const sel of productCandidates) {
    try {
      await page.waitForSelector(sel, { timeout: 6000 });
      break;
    } catch {}
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

async function clickConstructorNextAndWait(page) {
  const listSel = "#cio-results-list, .cio-search-result-list, .cio-search-result-list-container";
  const itemSel = `${listSel} a.cio-search-result[href]`;

  const getHrefHash = async () => {
    const hrefs = await page.locator(itemSel).evaluateAll((els) =>
      els.map((a) => a.getAttribute("href") || "").filter(Boolean)
    );
    return hrefs.slice(0, 50).join("|");
  };

  const getActivePage = async () => {
    const el = page
      .locator("li.cio-search-page-selected, li.cio-search-page-active, li[aria-current='page']")
      .first();
    if (!(await el.count())) return "";
    return (await el.textContent())?.trim() || "";
  };

  const beforeHash = await getHrefHash();
  const beforeActive = await getActivePage();

  const nextLi = page.locator("li.cio-search-page-next").first();
  const cls = (await nextLi.getAttribute("class")) || "";
  if (/disabled/i.test(cls)) return { moved: false, reason: "next_disabled" };

  const nextA = page.locator("li.cio-search-page-next a[aria-label*='next']").first();
  if (!(await nextA.count())) return { moved: false, reason: "next_not_found" };

  const waitNetwork = page
    .waitForResponse(
      (res) => {
        const url = res.url();
        return /cnstrc|constructor/i.test(url) && res.status() === 200;
      },
      { timeout: 12000 }
    )
    .catch(() => null);

  await nextA.scrollIntoViewIfNeeded().catch(() => {});
  await nextA.click({ timeout: 10000 });

  const net = await waitNetwork;

  for (let i = 0; i < 24; i++) {
    await page.waitForTimeout(500);

    const afterActive = await getActivePage();
    if (afterActive && afterActive !== beforeActive) {
      return { moved: true, reason: "active_page_changed", network: !!net };
    }

    const afterHash = await getHrefHash();
    if (afterHash && afterHash !== beforeHash) {
      return { moved: true, reason: "href_hash_changed", network: !!net };
    }
  }

  if (net) return { moved: true, reason: "network_ok_but_same_hash" };

  return { moved: false, reason: "no_change" };
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

function normalizePrice(value) {
  if (value === null || value === undefined) return null;
  const str = String(value).replace(/[^0-9.,-]/g, "").trim();
  if (!str) return null;

  const hasCommaDecimal = str.includes(",") && !str.includes(".");
  let normalized = hasCommaDecimal ? str.replace(",", ".") : str;
  normalized = normalized.replace(/,/g, "").replace(/\s+/g, "");

  const num = parseFloat(normalized);
  return Number.isFinite(num) ? num : null;
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

async function ensureSaleFacetAndNrpp(page) {
  const currentUrl = page.url().split("#")[0];
  if (!/\/category\/sale/i.test(currentUrl)) {
    return { applied: false, url: currentUrl };
  }

  const targetUrl = withInStoreFacet(currentUrl);
  if (targetUrl === currentUrl) {
    return { applied: false, url: currentUrl };
  }

  console.log(
    `ℹ️ Applying in-store facet and Nrpp to Sale URL: ${targetUrl}`
  );
  await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle").catch(() => {});
  return { applied: true, url: targetUrl };
}

async function setStoreThenGoToSale(page, store, debugPaths = []) {
  const postal = normalizePostal(store.postalCode || store.postal || store.zip);
  const city = store.city || "";

  console.log(`Setting store using Locations page => postal=${postal} city=${city}`);

  const jsonResponses = attachJsonResponseCollector(page);

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

  await page.waitForTimeout(4000);
  await ensureOnSalePage(page);
  await ensureSaleFacetAndNrpp(page);
  return jsonResponses;
}


async function loadProductsByPagination(page, store, jsonResponses = [], debugPaths = []) {
  const allProducts = [];
  const seen = new Set();
  let prevSignature = null;
  let zeroGainStreak = 0;
  let recoveredFromLocations = false;
  let firstPageZeroRetry = false;
  let totalAnchors = 0;

  for (let pageNum = 1; pageNum <= MAX_PAGES; pageNum++) {
    try {
      let { recovered, productVisible } = await ensureOnSalePage(page);
      recoveredFromLocations = recoveredFromLocations || recovered;
      const facetApplied = await ensureSaleFacetAndNrpp(page);

      if (facetApplied.applied) {
        const postFacet = await ensureOnSalePage(page);
        recovered = recovered || postFacet.recovered;
        productVisible = postFacet.productVisible;
        recoveredFromLocations = recoveredFromLocations || postFacet.recovered;
      }
      await page.waitForLoadState("networkidle").catch(() => {});
      await waitForProductsGrid(page);

      const extractionMeta = await extractProductsWithFallbacks(
        page,
        jsonResponses
      );
      totalAnchors += extractionMeta.anchorsTotal || 0;
      let usedDomFallback = extractionMeta.usedDomFallback;
      let productsOnPage = normalizeProducts(extractionMeta.products);
      let productCount = productsOnPage.length;

      if (
        productsOnPage.length === 0 &&
        pageNum === 1 &&
        (recovered || recoveredFromLocations || !productVisible) &&
        !firstPageZeroRetry
      ) {
        console.warn("⚠️ No products on first page after recovery attempt; retrying once...");
        firstPageZeroRetry = true;
        await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
        await ensureOnSalePage(page);
        await page.waitForLoadState("networkidle").catch(() => {});
        await waitForProductsGrid(page);
        const retryExtraction = await extractProductsWithFallbacks(
          page,
          jsonResponses
        );
        usedDomFallback = usedDomFallback || retryExtraction.usedDomFallback;
        totalAnchors += retryExtraction.anchorsTotal || 0;
        productsOnPage = normalizeProducts(retryExtraction.products);
        productCount = productsOnPage.length;
      }

      const currentUrlNoHash = page.url().split("#")[0];
      const first = productsOnPage[0];
      const firstProductHref =
        first?.productUrl ||
        first?.href ||
        first?.url ||
        first?.link ||
        (await getFirstGridHref(page));
      const currentPageUrls = productsOnPage
        .map((product) =>
          product.productUrl ||
            product.href ||
            product.url ||
            product.id ||
            product.title ||
            product.name ||
            ""
        )
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

      const uniqueProducts = productsOnPage.filter((product) => {
        const key = product.productUrl || product.href || product.url;
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
      });

      allProducts.push(...uniqueProducts);
      const newUniqueCount = uniqueProducts.length;

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

      const res = await clickConstructorNextAndWait(page);
      console.log(`[PA] Next result: moved=${res.moved} reason=${res.reason}`);

      if (!res.moved) {
        console.log(`[PA] Stop pagination: ${res.reason}`);
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
  return { products: allProducts, anchorsTotal: totalAnchors };
}

async function extractProductsDomFallback(page) {
  const base = BASE_URL;

  const container = page
    .locator(
      'main, [role="main"], section:has-text("Sale"), section:has-text("Vente"), .product-grid, .products, .plp, .search-results'
    )
    .first();

  const scope = (await container.count()) ? container : page.locator("body");

  const items = await scope.$$eval("a[href]", (as) => {
    const out = [];
    for (const a of as) {
      const href = a.getAttribute("href") || "";
      if (!/\/product\/|\/p\/|\/produit\/|\/en\/p\/|\/fr\/p\//i.test(href)) continue;

      if (a.closest("header, footer, nav")) continue;

      const name = a.getAttribute("aria-label") || a.textContent?.trim() || "";

      out.push({ href, name });
    }
    return out;
  });

  const seen = new Set();
  const normalized = [];
  for (const it of items) {
    if (!it.href) continue;
    const abs = it.href.startsWith("http") ? it.href : base + it.href;
    if (seen.has(abs)) continue;
    seen.add(abs);
    normalized.push({ ...it, href: abs });
    if (normalized.length >= 50) break;
  }

  console.log(
    `[PA] DOM fallback uniqueCount=${normalized.length} sample=${normalized
      .slice(0, 3)
      .map((p) => p.href)
      .join(" | ")}`
  );

  return { products: normalized, anchorsTotal: normalized.length };
}

function extractProductsFromNetwork(responses) {
  const out = [];
  for (const r of responses) {
    const j = r.json;
    const candidates = [
      j?.response?.results,
      j?.results,
      j?.items,
      j?.products,
      j?.data?.products,
    ].filter(Array.isArray);

    for (const arr of candidates) {
      for (const p of arr) {
        const url = p?.url || p?.data?.url || p?.product_url || p?.link;
        const name = p?.name || p?.title;
        const image = p?.image_url || p?.image || p?.data?.image_url;
        const price = p?.price || p?.data?.price;
        if (url || name) out.push({ name, href: url, image, price });
      }
    }
  }

  const seen = new Set();
  return out.filter((x) => {
    const key = x.href || x.name;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function extractProductsFromConstructorDom(page) {
  const base = "https://www.princessauto.com";

  const list = page
    .locator("#cio-results-list, .cio-search-result-list, .cio-search-result-list-container")
    .first();
  const scope = (await list.count()) ? list : page.locator("body");

  const rows = await scope.locator("a.cio-search-result[href]").evaluateAll((els) => {
    const out = [];
    for (const a of els) {
      const href = a.getAttribute("href") || "";
      const name =
        a.getAttribute("title") || a.getAttribute("aria-label") || a.textContent?.trim() || "";

      const parent = a.parentElement;
      const card = parent?.querySelector(".cc-product-card") || a.closest(".cc-product-card");

      const sku = card?.getAttribute("data-oe-item-id") || "";
      const dataName = card?.getAttribute("data-oe-item-name") || "";
      const listPrice = card?.getAttribute("data-oe-item-list-price") || "";

      out.push({
        href,
        name: dataName || name,
        sku,
        listPrice,
      });
    }
    return out;
  });

  const seen = new Set();
  const normalized = [];
  for (const r of rows) {
    if (!r.href) continue;
    const abs = r.href.startsWith("http") ? r.href : base + r.href;
    if (seen.has(abs)) continue;
    seen.add(abs);
    normalized.push({ ...r, href: abs });
    if (normalized.length >= 50) break;
  }
  return { products: normalized, anchorsTotal: normalized.length };
}

async function extractProductsWithFallbacks(page, jsonResponses = []) {
  await preparePageForExtraction(page);
  console.log(
    `[PA] constructorDomCount=${await page.locator(".cio-search-result-list a.cio-search-result").count()}`
  );
  let { products, anchorsTotal = 0 } = await extractProductsFromConstructorDom(page);
  let usedDomFallback = false;
  let usedNetworkFallback = false;

  if (!products.length) {
    console.warn("[PA] Constructor.io extraction returned 0. Trying primary DOM extractor...");
    const primary = await extractProducts(page);
    products = primary.products;
    anchorsTotal = primary.anchorsTotal;
  }

  if (!products.length) {
    console.warn("[PA] Primary extraction returned 0. Trying DOM fallback...");
    const fallback = await extractProductsDomFallback(page);
    products = fallback.products;
    anchorsTotal = fallback.anchorsTotal;
    usedDomFallback = true;
  }

  const hasHref = products.some((p) =>
    normalizeHref(p?.productUrl || p?.href || p?.url || p?.link)
  );

  if (!products.length || !hasHref) {
    console.warn("[PA] DOM still 0. Trying network JSON fallback...");
    products = extractProductsFromNetwork(jsonResponses);
    anchorsTotal = anchorsTotal || products.length;
    usedNetworkFallback = true;
  }

  return { products, usedDomFallback, usedNetworkFallback, anchorsTotal };
}

async function extractProducts(page) {
  console.log("🔍 Extracting products...");
  const { products, tileCount, visibleProducts, anchorsTotal } = await page.evaluate(
    ({ productSelector, selectors }) => {
      const tiles = Array.from(document.querySelectorAll(productSelector)).filter((tile) =>
        selectors.some((selector) => tile.querySelector(selector))
      );
      const deduped = new Map();
      const visibleKeys = new Set();
      const viewportWidth = window.innerWidth || document.documentElement.clientWidth;
      const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
      let anchorsTotal = 0;

      const normalizeUrl = (href) => {
        if (!href) return null;
        try {
          const url = new URL(href, document.baseURI);
          if (url.searchParams.get("ratings") === "reviews") {
            url.searchParams.delete("ratings");
          }
          const cleanedSearch = url.searchParams.toString();
          const base = `${url.origin}${url.pathname}`;
          return `${base}${cleanedSearch ? `?${cleanedSearch}` : ""}`.split("#")[0];
        } catch (error) {
          return null;
        }
      };

      const normalizePriceText = (value) => {
        if (value === null || value === undefined) return null;
        const str = String(value).replace(/[^0-9.,-]/g, "").trim();
        if (!str) return null;

        const hasCommaDecimal = str.includes(",") && !str.includes(".");
        let normalized = hasCommaDecimal ? str.replace(",", ".") : str;
        normalized = normalized.replace(/,/g, "").replace(/\s+/g, "");

        const num = parseFloat(normalized);
        return Number.isFinite(num) ? num : null;
      };

      const extractSku = (tile) => {
        const attrSku = Array.from(tile.attributes)
          .map((attr) => ({ name: attr.name.toLowerCase(), value: attr.value?.trim() || "" }))
          .find(({ name }) => name.includes("sku") || name.includes("productid") || name.includes("product-id"));
        if (attrSku?.value) return attrSku.value;

        const dataSku = tile.getAttribute("data-sku") || tile.getAttribute("data-productid") || tile.getAttribute("data-product-id");
        if (dataSku) return dataSku.trim();

        const text = (tile.textContent || "").replace(/ratings=reviews/gi, "");
        const directMatch = text.match(/(?:SKU|UGS|Item|Model)\s*[:#]?\s*([A-Z0-9-]+)/i);
        if (directMatch?.[1]) return directMatch[1].trim();

        return "";
      };

      for (const tile of tiles) {
        const anchor = selectors
          .map((selector) => tile.querySelector(selector))
          .find((el) => el?.getAttribute("href") || el?.href);
        if (!anchor) continue;
        anchorsTotal += 1;
        const href = anchor.getAttribute("href") || anchor.href;
        if (!href) continue;
        const url = normalizeUrl(href);

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

        const sku = extractSku(tile);

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
          name: title,
          productUrl: url,
          imageUrl,
          priceRegular: normalizePriceText(originalPriceText),
          priceSale: normalizePriceText(currentPriceText),
          sku,
        });
      }

      return {
        tileCount: tiles.length,
        products: Array.from(deduped.values()),
        visibleProducts: visibleKeys.size,
        anchorsTotal,
      };
    },
    { productSelector: PRODUCT_TILE_SELECTOR, selectors: PRODUCT_LINK_SELECTORS }
  );

  console.log(`🧱 Product tiles detected before extraction: ${tileCount}`);
  console.log(`🫧 Visible products in viewport: ${visibleProducts}`);
  console.log(`Products extracted: ${products.length}`);
  return { products, anchorsTotal };
}

function normalizeHref(href) {
  if (!href) return null;
  try {
    const url = new URL(href, BASE_URL);
    if (url.searchParams.get("ratings") === "reviews") {
      url.searchParams.delete("ratings");
    }
    const cleanedSearch = url.searchParams.toString();
    const base = `${url.origin}${url.pathname}`;
    return `${base}${cleanedSearch ? `?${cleanedSearch}` : ""}`.split("#")[0];
  } catch (error) {
    return null;
  }
}

function normalizeProduct(product = {}) {
  const productUrl = normalizeHref(
    product.productUrl || product.href || product.url || product.link
  );
  const name = (product.name || product.title || "").trim();
  const imageUrl = product.imageUrl || product.image || null;
  const priceRegular = normalizePrice(product.priceRegular ?? product.price);
  const priceSale = normalizePrice(product.priceSale ?? product.price);
  const sku = product.sku ?? "";

  if (!productUrl) return null;

  return {
    name,
    imageUrl,
    productUrl,
    sku,
    priceRegular,
    priceSale,
  };
}

function normalizeProducts(products = []) {
  const seen = new Set();
  const out = [];

  for (const product of products) {
    const normalized = normalizeProduct(product);
    if (!normalized?.productUrl) continue;
    if (seen.has(normalized.productUrl)) continue;
    seen.add(normalized.productUrl);
    out.push(normalized);
  }

  return out;
}

function writeStoreOutput(store, products, storeSynced = true, anchorsTotal = 0) {
  const slug = store.slug;
  const storeDir = path.join(OUTPUT_ROOT, slug);
  const outputFile = path.join(storeDir, "data.json");
  const csvFile = path.join(storeDir, "data.csv");
  const scrapedAt = new Date().toISOString();

  fs.mkdirSync(storeDir, { recursive: true });

  const csvHeaders = [
    "storeSlug",
    "scrapedAt",
    "name",
    "imageUrl",
    "productUrl",
    "sku",
    "priceRegular",
    "priceSale",
  ];
  const csvRows = [csvHeaders.join(",")];

  const escapeCsv = (value) => {
    if (value === null || value === undefined) return "";
    const str = String(value);
    if (/[",\n]/.test(str)) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  };

  const productsWithMeta = products.map((product) => ({
    ...product,
    storeSlug: slug,
    scrapedAt,
  }));

  for (const product of productsWithMeta) {
    const row = csvHeaders.map((key) => escapeCsv(product[key])).join(",");
    csvRows.push(row);
  }

  fs.writeFileSync(csvFile, csvRows.join("\n"), "utf8");

  fs.writeFileSync(outputFile, JSON.stringify(productsWithMeta, null, 2), "utf8");

  const withSkuCount = productsWithMeta.filter((p) => p.sku).length;
  const missingSkuCount = productsWithMeta.length - withSkuCount;

  console.log(
    `📊 anchorsTotal=${anchorsTotal} uniqueProducts=${productsWithMeta.length} withSkuCount=${withSkuCount} missingSkuCount=${missingSkuCount}`
  );
  console.log(`💾 JSON written → ${outputFile}`);
  console.log(`💾 CSV written → ${csvFile}`);

  return {
    storeName: store.name || store.storeName,
    city: store.city,
    province: store.province,
    address: store.address,
    storeId: store.storeId ?? store.id ?? null,
    postalCode: store.postalCode ?? null,
    phone: store.phone ?? null,
    slug: store.slug,
    updatedAt: scrapedAt,
    storeSynced,
    products: productsWithMeta,
  };
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
    const jsonResponses = await setStoreThenGoToSale(page, store, debugPaths);
    await ensureOnSalePage(page);
    debugPaths.push(...(await captureDebug(page, store.slug, "after_store")));
    const { products: allProducts, anchorsTotal } = await loadProductsByPagination(
      page,
      store,
      jsonResponses,
      debugPaths
    );
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

    const hasNoResultsText = await page
      .locator("text=/No results/i, text=/0 results/i, text=/No items/i")
      .first()
      .isVisible({ timeout: 2000 })
      .catch(() => false);

    const isZeroLegit = hasNoResultsText || totalResultCount === 0;

    const shouldKeepDebug = allProducts.length === 0 || !storeSynced;

    if (shouldKeepDebug) {
      const reason =
        allProducts.length === 0
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

    return writeStoreOutput(store, allProducts, storeSynced, anchorsTotal);
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
