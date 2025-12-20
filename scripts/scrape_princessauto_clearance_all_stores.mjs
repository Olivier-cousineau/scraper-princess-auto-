import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const ROOT_DIR = process.cwd();
const BASE_URL = "https://www.princessauto.com";
const SALE_BASE_URL = "https://www.princessauto.com/en/category/Sale?page=1";
const NRPP = parseInt(process.env.NRPP || "50", 10);
const SALE_URL = withNrpp(SALE_BASE_URL);
const MAX_PAGES = parseInt(process.env.MAX_PAGES || "40", 10);
const CONCURRENCY = Number(
  process.env.STORE_CONCURRENCY ?? process.env.PA_CONCURRENCY ?? "2"
);
const PER_STORE_TIMEOUT_MINUTES = Number(
  process.env.PA_STORE_TIMEOUT_MINUTES ??
    process.env.STORE_TIMEOUT_MINUTES ??
    "25"
);
const STORES_JSON = path.join(ROOT_DIR, "public", "princessauto", "stores.json");
const OUTPUT_ROOT = path.join(ROOT_DIR, "public", "princessauto");
const DEBUG_OUTPUT_DIR = path.join(ROOT_DIR, "outputs", "debug");
let activeBrowser = null;

const PRODUCT_TILE_SELECTOR =
  "div.cc-product-card, [data-testid='product-card'], [data-testid='product-tile']";
const PRODUCT_LINK_SELECTORS = [
  "[data-testid='product-tile'] a[href*='/product/']",
  "[data-testid='product-card'] a[href*='/product/']",
  "a[href*='/product/']",
  "a[href*='/p/']",
];

function extractSkuFromUrl(href) {
  if (!href) return null;
  try {
    const u = new URL(href, BASE_URL);
    const skuId = u.searchParams.get("skuId");
    if (skuId && /^\d+$/.test(skuId)) return skuId;

    const pa = u.pathname.match(/\/PA\d{10,}/i);
    if (pa) return pa[0].replace("/", "");

    const digits = u.pathname.match(/(\d{6,10})/);
    if (digits) return digits[1];

    return null;
  } catch (error) {
    return null;
  }
}

function withInStoreFacet(url) {
  const u = new URL(url);
  u.searchParams.set("facet.availability", "56");
  return u.toString();
}

function withNrpp(url) {
  const u = new URL(url);
  u.searchParams.set("Nrpp", String(NRPP));
  return u.toString();
}

async function humanScrollToBottom(page) {
  await page.evaluate(async () => {
    const jitter = () => {
      window.scrollBy({ top: 120, behavior: "smooth" });
      window.scrollBy({ top: -80, behavior: "smooth" });
    };

    const step = async () => {
      const { scrollHeight, clientHeight } = document.documentElement;
      const maxScrollTop = scrollHeight - clientHeight;
      for (let y = 0; y < maxScrollTop; y += 650) {
        window.scrollTo({ top: y, behavior: "smooth" });
        jitter();
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      window.scrollTo({ top: maxScrollTop, behavior: "smooth" });
    };

    await step();
    await new Promise((resolve) => setTimeout(resolve, 350));
  });
}

async function waitForDomStable(page, { timeoutMs = 12000 } = {}) {
  return page.evaluate(
    (timeout) =>
      new Promise((resolve) => {
        let timeoutId = null;
        let idleId = null;

        const done = () => {
          clearTimeout(timeoutId);
          clearTimeout(idleId);
          observer.disconnect();
          resolve();
        };

        const observer = new MutationObserver(() => {
          clearTimeout(idleId);
          idleId = setTimeout(done, 800);
        });

        observer.observe(document.body, { childList: true, subtree: true });
        idleId = setTimeout(done, 800);
        timeoutId = setTimeout(done, timeout);
      }),
    timeoutMs
  );
}

async function extractProductsFromSalePage(page, { captureRejects = false } = {}) {
  return page.evaluate(({ tileSelector, linkSelectors, captureRejects }) => {
    const tiles = Array.from(document.querySelectorAll(tileSelector));
    const seen = new Set();
    const products = [];
    const rejectionStats = {
      missingHref: 0,
      missingTitle: 0,
      missingSalePrice: 0,
      missingSku: 0,
    };
    const rejectedSamples = [];

    const normalizeUrl = (href) => {
      if (!href || /ratings=reviews/i.test(href)) return null;
      try {
        const url = new URL(href, document.baseURI);
        url.search = "";
        url.hash = "";
        return `${url.origin}${url.pathname}`;
      } catch (error) {
        return null;
      }
    };

    const buildUrlFromItemId = (itemId) => {
      if (!itemId) return null;
      return `https://www.princessauto.com/en/product/${itemId}`;
    };

    const extractSkuFromHref = (href) => {
      if (!href) return null;
      try {
        const u = new URL(href, document.baseURI);
        const skuId = u.searchParams.get("skuId");
        if (skuId && /^\d+$/.test(skuId)) return skuId;

        const pa = u.pathname.match(/\/PA\d{10,}/i);
        if (pa) return pa[0].replace("/", "");

        const digits = u.pathname.match(/(\d{6,10})/);
        if (digits) return digits[1];

        return null;
      } catch (error) {
        return null;
      }
    };

    const pickImageUrl = (el) => {
      if (!el) return null;
      const candidates = [
        el.getAttribute("src"),
        el.getAttribute("data-src"),
        el.getAttribute("data-original"),
      ].filter(Boolean);
      const raw = candidates.find(Boolean) || null;
      if (!raw) return null;
      return raw.startsWith("//") ? `https:${raw}` : raw;
    };

    for (const tile of tiles) {
      const link = linkSelectors
        .map((selector) => tile.querySelector(selector))
        .find(Boolean);
      const rawHref = link?.getAttribute("href") || link?.href || null;
      const itemId = tile.getAttribute("data-oe-item-id");
      const productUrl = normalizeUrl(rawHref) || buildUrlFromItemId(itemId);

      let sku = null;
      if (itemId) {
        sku = itemId.replace(/^PA0+/, "");
        if (!sku) sku = itemId;
      }
      if (!sku) sku = itemId;

      const uniqueKey = sku || productUrl || itemId || rawHref;
      const name =
        tile.getAttribute("data-oe-item-name") ||
        tile.querySelector("img")?.getAttribute("alt") ||
        null;

      const img = tile.querySelector("img");
      const imageUrl =
        img?.getAttribute("src") || img?.getAttribute("data-src") || null;

      const parsedRegular = parseFloat(
        tile.getAttribute("data-oe-item-list-price") || ""
      );
      const priceRegular = Number.isNaN(parsedRegular) ? null : parsedRegular;

      const parsedSale = parseFloat(
        tile.getAttribute("data-oe-item-sale-price") || ""
      );
      const priceSale = Number.isNaN(parsedSale) ? null : parsedSale;

      const missingHref = !rawHref && !itemId;
      const missingTitle = !name;
      const missingSalePrice = priceSale === null;
      const missingSku = !sku;

      const rejected =
        name === null ||
        priceSale === null ||
        !productUrl ||
        !uniqueKey ||
        seen.has(uniqueKey);

      if (rejected && captureRejects) {
        if (missingHref) rejectionStats.missingHref += 1;
        if (missingTitle) rejectionStats.missingTitle += 1;
        if (missingSalePrice) rejectionStats.missingSalePrice += 1;
        if (missingSku) rejectionStats.missingSku += 1;

        if (rejectedSamples.length < 3) {
          const outer = tile.outerHTML || "";
          rejectedSamples.push(outer.slice(0, 1200));
        }
      }

      if (rejected) continue;

      products.push({
        productUrl,
        name,
        imageUrl,
        sku,
        priceRegular,
        priceSale,
      });

      seen.add(uniqueKey);
    }

    return { products, tilesFound: tiles.length, rejectionStats, rejectedSamples };
  }, { tileSelector: PRODUCT_TILE_SELECTOR, linkSelectors: PRODUCT_LINK_SELECTORS, captureRejects });
}

async function goToNextPageUI(page) {
  const selectors = [
    "a[rel='next']",
    "a[aria-label*='next' i]",
    "button[aria-label*='next' i]",
    ".pagination-next a",
    "a.pagination__next",
  ];

  let target = null;
  for (const sel of selectors) {
    const locator = page.locator(sel).first();
    if ((await locator.count()) && (await locator.isVisible().catch(() => false))) {
      target = locator;
      break;
    }
  }

  if (!target) return { moved: false, reason: "next_not_found" };

  const cls = (await target.getAttribute("class")) || "";
  const ariaDisabled = (await target.getAttribute("aria-disabled")) || "";
  if (/disabled/i.test(cls) || /true/i.test(ariaDisabled)) {
    return { moved: false, reason: "next_disabled" };
  }

  const prevSignature = await page.evaluate(({ tileSelector, linkSelectors }) => {
    const tiles = Array.from(document.querySelectorAll(tileSelector));
    const firstTile = tiles[0];
    if (!firstTile) return null;
    const link = linkSelectors
      .map((selector) => firstTile.querySelector(selector))
      .find(Boolean);
    return link?.getAttribute("href") || null;
  }, { tileSelector: PRODUCT_TILE_SELECTOR, linkSelectors: PRODUCT_LINK_SELECTORS });

  await Promise.all([
    target.click({ timeout: 12000 }).catch(() => {}),
    page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => null),
  ]);

  await waitForDomStable(page);

  const afterSignature = await page.evaluate(({ tileSelector, linkSelectors }) => {
    const tiles = Array.from(document.querySelectorAll(tileSelector));
    const firstTile = tiles[0];
    if (!firstTile) return null;
    const link = linkSelectors
      .map((selector) => firstTile.querySelector(selector))
      .find(Boolean);
    return link?.getAttribute("href") || null;
  }, { tileSelector: PRODUCT_TILE_SELECTOR, linkSelectors: PRODUCT_LINK_SELECTORS });

  const moved = (!!afterSignature && afterSignature !== prevSignature) || page.url().includes("page=");
  return { moved, reason: moved ? "navigated" : "no_change" };
}

async function saveDebug(page, outDir, prefix, extraNotes = []) {
  const htmlPath = path.join(outDir, `${prefix}.html`);
  const screenshotPath = path.join(outDir, `${prefix}.png`);

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
  } catch (error) {
    console.warn("⚠️ Failed to save debug artifacts:", error);
  }

  return [htmlPath, screenshotPath];
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
  const totalShards = Number(process.env.TOTAL_SHARDS ?? "43");
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

async function waitForSaleHydration(page) {
  await page.waitForLoadState("domcontentloaded");
  await page.waitForTimeout(1200);

  await page
    .waitForFunction(() => {
      const anchors = Array.from(document.querySelectorAll("a[href]"));
      const productAnchors = anchors.filter(
        (a) =>
          a.getAttribute("href") &&
          a.getAttribute("href").includes("/product/") &&
          a.getAttribute("href").length > "/product/".length
      );
      return productAnchors.length >= 10;
    }, { timeout: 15000 })
    .catch(() => {});

  const anchorSelector = "a[href*='/product/']:not([href*='ratings=reviews'])";
  const candidates = [anchorSelector, "div.cc-product-card"]; // signal readiness

  try {
    await Promise.race(
      candidates.map((selector) => page.waitForSelector(selector, { timeout: 15000 }))
    );
  } catch (error) {
    await page.waitForLoadState("networkidle").catch(() => {});
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

async function gatherProductAnchorMetrics(page) {
  return page.evaluate(() => {
    const anchors = Array.from(
      document.querySelectorAll("a[href*='/product/']:not([href*='ratings=reviews'])")
    );
    const rawHref = anchors[0]?.getAttribute("href") || anchors[0]?.href || null;
    let sampleAnchorHref = rawHref;
    if (rawHref) {
      try {
        sampleAnchorHref = new URL(rawHref, document.baseURI).href;
      } catch (error) {}
    }
    return { count: anchors.length, sampleAnchorHref };
  });
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
    await page.goto(withInStoreFacet(withNrpp(SALE_BASE_URL)), {
      waitUntil: "domcontentloaded",
    });
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

async function goToSaleWithFacetGuard(page) {
  const saleBaseUrl = SALE_URL;
  const facetSaleUrl = withInStoreFacet(saleBaseUrl);
  let usedFacetAvailability = false;

  const navigateAndMeasure = async (targetUrl) => {
    console.log(`➡️ Navigating to sale page: ${targetUrl}`);
    await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
    await waitForSaleHydration(page);
    const anchorMetrics = await gatherProductAnchorMetrics(page);
    return anchorMetrics;
  };

  let anchorMetrics = await navigateAndMeasure(facetSaleUrl);

  if (anchorMetrics.count === 0) {
    console.log("facet_availability_disabled_for_store=true");
    anchorMetrics = await navigateAndMeasure(saleBaseUrl);
  } else {
    usedFacetAvailability = true;
  }

  return { usedFacetAvailability, anchorMetrics };
}

async function logPageOneDiagnostics(
  page,
  { usedFacetAvailability, tilesFound, productAnchorsFound, sampleAnchorHref }
) {
  const pageTitle = (await page.title().catch(() => "")) || "";
  const hasNoResultsText = await page
    .locator("text=/No results/i")
    .first()
    .isVisible({ timeout: 1500 })
    .catch(() => false);

  console.log(`usedFacetAvailability=${usedFacetAvailability}`);
  console.log(`tilesFound=${tilesFound}`);
  console.log(`productAnchorsFound=${productAnchorsFound}`);
  console.log(`sampleAnchorHref=${sampleAnchorHref || ""}`);
  console.log(`pageTitle=${pageTitle} hasNoResultsText=${hasNoResultsText}`);
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

  const targetSaleUrl = withInStoreFacet(withNrpp(SALE_BASE_URL));
  const saleLink = page.getByRole("link", { name: /vente|sale/i }).first();
  if (await saleLink.isVisible({ timeout: 5000 }).catch(() => false)) {
    await saleLink.click({ timeout: 10000 }).catch(() => {});
  } else {
    await page.goto(targetSaleUrl, { waitUntil: "domcontentloaded" });
  }
  if (page.url().includes("/locations")) {
    await page.goto(targetSaleUrl, { waitUntil: "domcontentloaded" });
  }

  await page.waitForTimeout(1500);
  await ensureOnSalePage(page);
  await waitForDomStable(page);

  const saleNavigationMeta = await goToSaleWithFacetGuard(page);
  return { jsonResponses, saleNavigationMeta };
}


async function loadProductsByPagination(
  page,
  store,
  jsonResponses = [],
  debugPaths = [],
  saleNavigationMeta = {}
) {
  const allProducts = [];
  const seenUrls = new Set();
  let zeroGainStreak = 0;
  let totalTilesFound = 0;
  let uiTotal = null;
  const usedFacetAvailability = !!saleNavigationMeta?.usedFacetAvailability;

  const readUiTotal = async () => {
    const text = await page.textContent("text=/\\d+\\s*(?:items|results)/i").catch(() => null);
    if (!text) return null;
    const match = text.match(/(\d+)\s*(?:items|results)/i);
    return match?.[1] ? Number(match[1]) : null;
  };

  for (let pageNum = 1; pageNum <= MAX_PAGES; pageNum++) {
    try {
      await ensureOnSalePage(page);
      await waitForSaleHydration(page);
      await humanScrollToBottom(page);
      await waitForDomStable(page);

      const extraction = await extractProductsFromSalePage(page, {
        captureRejects: pageNum === 1,
      });
      totalTilesFound += extraction.tilesFound || 0;
      const normalized = normalizeProducts(extraction.products);

      const uniqueProducts = [];
      for (const product of normalized) {
        const key =
          product.sku || extractSkuFromUrl(product.productUrl) || product.productUrl || null;
        if (key && seenUrls.has(key)) continue;
        if (key) seenUrls.add(key);
        uniqueProducts.push(product);
      }

      allProducts.push(...uniqueProducts);

      if (uiTotal === null) {
        uiTotal = await readUiTotal();
        if (uiTotal) {
          console.log(`ℹ️ UI total (approx): ${uiTotal}`);
        }
      }

      console.log(
        `📄 Page ${pageNum} extracted=${extraction.products.length} added=${uniqueProducts.length} totalUnique=${allProducts.length} url=${page.url()}`
      );
      console.log(
        `[PA] Page ${pageNum} tile diagnostics: tilesFound=${extraction.tilesFound}`
      );

      if (pageNum === 1 && extraction.rejectionStats) {
        const stats = extraction.rejectionStats;
        console.log(
          `[PA] Page 1 reject stats: missingHref=${stats.missingHref} missingTitle=${stats.missingTitle} missingSalePrice=${stats.missingSalePrice} missingSku=${stats.missingSku}`
        );
        extraction.rejectedSamples.forEach((sample, idx) => {
          console.log(`[PA] Page 1 rejected sample #${idx + 1}: ${sample}`);
        });
      }

      const extractedCount = extraction.products.length;

      if (uniqueProducts.length < 3) {
        if (pageNum === 1 && extractedCount === 0) {
          console.log("[PA] Skipping low gain evaluation for page 1 with zero extraction");
        } else {
          zeroGainStreak += 1;
          if (zeroGainStreak >= 2) {
            console.log("🛑 Stop pagination: low gain on two consecutive pages");
            break;
          }
        }
      } else {
        zeroGainStreak = 0;
      }

      if (pageNum === MAX_PAGES) {
        console.log(`🛑 Stop pagination: MAX_PAGES=${MAX_PAGES} reached`);
        break;
      }

      const nextRes = await goToNextPageUI(page);
      console.log(`[PA] Next result: moved=${nextRes.moved} reason=${nextRes.reason}`);
      if (!nextRes.moved) break;
    } catch (error) {
      console.error(`⚠️ Failed to process page ${pageNum} for ${store.slug}:`, error);
      debugPaths.push(...(await captureDebug(page, store.slug, `page_${pageNum}_error`)));
      break;
    }
  }

  if (uiTotal && allProducts.length < uiTotal * 0.8) {
    const notes = [`uiTotal=${uiTotal}`, `products=${allProducts.length}`];
    debugPaths.push(...(await saveDebug(page, DEBUG_OUTPUT_DIR, `${store.slug}_final_low_coverage`, notes)));
  }

  if (usedFacetAvailability) {
    console.log("usedFacetAvailability=true");
  }
  console.log(`[PA] ${store.slug} total products across pages=${allProducts.length}`);
  return { products: allProducts, tilesFound: totalTilesFound };
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

async function extractProductsWithFallbacks(
  page,
  jsonResponses = [],
  pageNum = 1,
  anchorMetrics
) {
  await preparePageForExtraction(page);

  const metrics = anchorMetrics || (await gatherProductAnchorMetrics(page));
  const productAnchorsFound = metrics?.count ?? 0;
  const sampleAnchorHref = metrics?.sampleAnchorHref ?? null;

  const primary = await extractProducts(page, pageNum);
  const normalizedPrimary = normalizeProducts(primary.products);

  const shouldUseAnchorFallback =
    (normalizedPrimary.length === 0 && primary.tilesFound > 0) ||
    (primary.tilesFound === 0 && productAnchorsFound > 0);

  if (shouldUseAnchorFallback) {
    console.warn(
      "⚠️ Primary tile extraction insufficient; using anchor-based fallback extraction"
    );
    const anchorFallback = await extractProductsFromAnchors(page);
    console.log(
      `FALLBACK_ANCHORS_USED=true anchorsFound=${anchorFallback.tilesFound} productsAfterDedup=${anchorFallback.products.length}`
    );
    return {
      products: anchorFallback.products,
      usedDomFallback: true,
      usedNetworkFallback: false,
      anchorsTotal: anchorFallback.tilesFound,
      tilesFound: primary.tilesFound,
      tileWithSkuCount: anchorFallback.tileWithSkuCount,
      tileWithPricesCount: anchorFallback.tileWithPricesCount,
      tileWithUrlCount: anchorFallback.tileWithUrlCount,
      productAnchorsFound,
      sampleAnchorHref,
    };
  }

  return {
    products: primary.products,
    usedDomFallback: false,
    usedNetworkFallback: false,
    anchorsTotal: primary.tilesFound,
    tilesFound: primary.tilesFound,
    tileWithSkuCount: primary.tileWithSkuCount,
    tileWithPricesCount: primary.tileWithPricesCount,
    tileWithUrlCount: primary.tileWithUrlCount,
    productAnchorsFound,
    sampleAnchorHref,
  };
}

async function extractProducts(page, pageNum = 1) {
  console.log("🔍 Extracting products with Princess Auto card selectors...");
  const {
    products,
    tilesFound,
    debugSample,
    tileWithSkuCount,
    tileWithPricesCount,
    tileWithUrlCount,
  } = await page.evaluate(
    ({ tileSelector, shouldSample }) => {
      const tiles = Array.from(document.querySelectorAll(tileSelector));
      const seen = new Set();
      const products = [];

      const normalizeUrl = (href) => {
        if (!href || /ratings=reviews/i.test(href)) return null;
        try {
          const url = new URL(href, document.baseURI);
          url.search = "";
          url.hash = "";
          return `${url.origin}${url.pathname}`;
        } catch (error) {
          return null;
        }
      };

      const buildUrlFromItemId = (itemId) => {
        if (!itemId) return null;
        return `https://www.princessauto.com/en/product/${itemId}`;
      };

      const extractSkuFromHref = (href) => {
        if (!href) return null;
        try {
          const u = new URL(href, document.baseURI);
          const skuId = u.searchParams.get("skuId");
          if (skuId && /^\d+$/.test(skuId)) return skuId;

          const pa = u.pathname.match(/\/PA\d{10,}/i);
          if (pa) return pa[0].replace("/", "");

          const digits = u.pathname.match(/(\d{6,10})/);
          if (digits) return digits[1];

          return null;
        } catch (error) {
          return null;
        }
      };

      const tileStats = {
        tileWithSkuCount: 0,
        tileWithPricesCount: 0,
        tileWithUrlCount: 0,
      };

      let debugSample = null;

      for (const tile of tiles) {
        const card =
          tile.closest(".cc-product-card, .cc-product, li, .grid-item, [data-id]") || tile;

        const productLink = card.querySelector(
          "a[href*='/product/']:not([href*='ratings=reviews'])"
        );
        const href = productLink?.getAttribute("href") || productLink?.href || null;
        const itemId = card.getAttribute("data-oe-item-id");
        const productUrl = normalizeUrl(href) || buildUrlFromItemId(itemId);
        if (productUrl) tileStats.tileWithUrlCount += 1;

        const name =
          card.getAttribute("data-oe-item-name") ||
          card.querySelector("img")?.getAttribute("alt") ||
          null;

        const imgEl =
          card.querySelector("img[src]") || card.querySelector("[data-bind*='primarySmallImageURL']");
        const imageUrl =
          imgEl?.getAttribute("src") || imgEl?.getAttribute("data-src") || null;

        let sku = null;
        if (itemId) {
          sku = itemId.replace(/^PA0+/, "");
          if (!sku) sku = itemId;
        }
        if (!sku) sku = itemId;
        if (sku) tileStats.tileWithSkuCount += 1;

        const parsedRegular = parseFloat(
          card.getAttribute("data-oe-item-list-price") || ""
        );
        const priceRegular = Number.isNaN(parsedRegular) ? null : parsedRegular;

        const parsedSale = parseFloat(
          card.getAttribute("data-oe-item-sale-price") || ""
        );
        const priceSale = Number.isNaN(parsedSale) ? null : parsedSale;

        if (priceRegular !== null || priceSale !== null)
          tileStats.tileWithPricesCount += 1;

        if (!debugSample && shouldSample) {
          const inner = card.innerHTML || "";
          debugSample = {
            sampleTileInnerHTML: inner.slice(0, 500),
            sampleFoundHref: productUrl || href,
            sampleFoundName: name,
            sampleFoundImg: imageUrl,
            sampleFoundSkuText: sku,
            sampleFoundBeforePrice: priceRegular,
            sampleFoundAfterPrice: priceSale,
          };
        }

        const uniqueKey = sku || productUrl || itemId || href;
        if (
          name === null ||
          priceSale === null ||
          !productUrl ||
          !uniqueKey ||
          seen.has(uniqueKey)
        )
          continue;

        products.push({
          name,
          imageUrl,
          productUrl,
          sku,
          priceRegular,
          priceSale,
        });

        seen.add(uniqueKey);
      }

      return {
        products,
        tilesFound: tiles.length,
        debugSample,
        tileWithSkuCount: tileStats.tileWithSkuCount,
        tileWithPricesCount: tileStats.tileWithPricesCount,
        tileWithUrlCount: tileStats.tileWithUrlCount,
      };
    },
    { tileSelector: PRODUCT_TILE_SELECTOR, shouldSample: pageNum === 1 }
  );

  console.log(
    `🧱 tilesFound=${tilesFound} tileWithSkuCount=${tileWithSkuCount} tileWithPricesCount=${tileWithPricesCount} tileWithUrlCount=${tileWithUrlCount}`
  );
  console.log(`Products extracted: ${products.length}`);
  if (debugSample) {
    console.log("[PA] sampleTileInnerHTML", debugSample.sampleTileInnerHTML);
    console.log("[PA] sampleFoundHref", debugSample.sampleFoundHref);
    console.log("[PA] sampleFoundName", debugSample.sampleFoundName);
    console.log("[PA] sampleFoundImg", debugSample.sampleFoundImg);
    console.log("[PA] sampleFoundSkuText", debugSample.sampleFoundSkuText);
    console.log("[PA] sampleFoundBeforePrice", debugSample.sampleFoundBeforePrice);
    console.log("[PA] sampleFoundAfterPrice", debugSample.sampleFoundAfterPrice);
  }
  return {
    products,
    tilesFound,
    tileWithSkuCount,
    tileWithPricesCount,
    tileWithUrlCount,
  };
}

async function extractProductsFromAnchors(page) {
  const { products, tilesFound, tileWithSkuCount, tileWithPricesCount, tileWithUrlCount } =
    await page.evaluate(() => {
    const anchors = Array.from(
      document.querySelectorAll("a[href*='/product/']:not([href*='ratings=reviews'])")
    );
    const seen = new Set();
    const products = [];
    let tileWithSkuCount = 0;
    let tileWithPricesCount = 0;
    let tileWithUrlCount = 0;

    const normalizeUrl = (href) => {
      if (!href || /ratings=reviews/i.test(href)) return null;
      try {
        const url = new URL(href, document.baseURI);
        url.search = "";
        url.hash = "";
        return `${url.origin}${url.pathname}`;
      } catch (error) {
        return null;
      }
    };

    for (const anchor of anchors) {
      const href = anchor.getAttribute("href") || anchor.href || null;
      const productUrl = normalizeUrl(href);
      if (productUrl) tileWithUrlCount += 1;

      const card =
        anchor.closest(".cc-product-card, .cc-product, li, .grid-item, [data-id]") || anchor;
      const itemId = card?.getAttribute("data-oe-item-id") || null;
      const name =
        card?.getAttribute("data-oe-item-name") ||
        card?.querySelector("img")?.getAttribute("alt") ||
        null;

      const imgEl =
        card?.querySelector("img[src]") || card?.querySelector("[data-bind*='primarySmallImageURL']") ||
        null;
      const imageUrl =
        imgEl?.getAttribute("src") || imgEl?.getAttribute("data-src") || null;

      const parsedRegular = parseFloat(
        card?.getAttribute("data-oe-item-list-price") || ""
      );
      const priceRegular = Number.isNaN(parsedRegular) ? null : parsedRegular;

      const parsedSale = parseFloat(card?.getAttribute("data-oe-item-sale-price") || "");
      const priceSale = Number.isNaN(parsedSale) ? null : parsedSale;

      if (priceRegular !== null || priceSale !== null) tileWithPricesCount += 1;

      let sku = null;
      if (itemId) {
        sku = itemId.replace(/^PA0+/, "");
        if (!sku) sku = itemId;
      }
      if (!sku) sku = itemId;
      if (sku) tileWithSkuCount += 1;

      const uniqueKey = sku || productUrl || itemId || href;
      if (
        name === null ||
        priceSale === null ||
        !productUrl ||
        !uniqueKey ||
        seen.has(uniqueKey)
      )
        continue;

      products.push({
        productUrl,
        name,
        imageUrl,
        priceRegular,
        priceSale,
        sku,
      });

      seen.add(uniqueKey);
    }

    return {
      products,
      tilesFound: anchors.length,
      tileWithSkuCount,
      tileWithPricesCount,
      tileWithUrlCount,
    };
  });

  return { products, tilesFound, tileWithSkuCount, tileWithPricesCount, tileWithUrlCount };
}

function normalizeHref(href) {
  if (!href) return null;
  if (/ratings=reviews/i.test(href)) return null;
  try {
    const url = new URL(href, BASE_URL);
    url.search = "";
    url.hash = "";
    return `${url.origin}${url.pathname}`;
  } catch (error) {
    return null;
  }
}

function normalizeProduct(product = {}) {
  const productUrl = normalizeHref(
    product.productUrl || product.href || product.url || product.link
  );
  const rawName = product.name ?? null;
  const name = typeof rawName === "string" && rawName.trim() ? rawName.trim() : null;
  const rawImageUrl = product.imageUrl || product.image || null;
  const imageUrl = rawImageUrl
    ? rawImageUrl.startsWith("//")
      ? `https:${rawImageUrl}`
      : rawImageUrl
    : null;
  const priceRegular = normalizePrice(product.priceRegular);
  const priceSale = normalizePrice(product.priceSale);
  const sku = product.sku ?? null;

  if (!productUrl) return null;
  if (name === null) return null;
  if (priceSale === null) return null;

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
    const rawHref = product.productUrl || product.href || product.url || product.link || null;
    const productIdFromUrl = extractSkuFromUrl(rawHref);
    const normalized = normalizeProduct({ ...product, productIdFromUrl });
    if (!normalized?.productUrl) continue;
    const key = normalized.sku || productIdFromUrl || normalized.productUrl || rawHref;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
  }

  return out;
}

function writeStoreOutput(store, products, storeSynced = true, tilesFound = 0) {
  const slug = store.slug;
  const storeDir = path.join(OUTPUT_ROOT, slug);
  const outputFile = path.join(storeDir, "data.json");
  const csvFile = path.join(storeDir, "data.csv");
  const scrapedAt = new Date().toISOString();

  fs.mkdirSync(storeDir, { recursive: true });

  const csvHeaders = [
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

  const normalizedProducts = products;

  for (const product of normalizedProducts) {
    const row = csvHeaders.map((key) => escapeCsv(product[key])).join(",");
    csvRows.push(row);
  }

  fs.writeFileSync(csvFile, csvRows.join("\n"), "utf8");

  fs.writeFileSync(outputFile, JSON.stringify(normalizedProducts, null, 2), "utf8");

  const withSkuCount = normalizedProducts.filter((p) => p.sku).length;
  const withSalePriceCount = normalizedProducts.filter((p) => p.priceSale !== null).length;

  console.log(`tilesFound=${tilesFound}`);
  console.log(`withSkuCount=${withSkuCount}`);
  console.log(`withSalePriceCount=${withSalePriceCount}`);
  console.log(`writtenProducts=${normalizedProducts.length}`);
  console.log(`Saved: public/princessauto/${slug}/data.csv`);
  console.log(`Saved: public/princessauto/${slug}/data.json`);

  try {
    const parsed = JSON.parse(fs.readFileSync(outputFile, "utf8"));
    if (!Array.isArray(parsed) || parsed.length === 0) {
      console.error(`❌ Output for ${slug} is empty or invalid at ${outputFile}`);
      process.exitCode = 1;
    }
  } catch (error) {
    console.error(`❌ Failed to read output for ${slug} at ${outputFile}:`, error);
    process.exitCode = 1;
  }

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
    products: normalizedProducts,
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
    const { jsonResponses, saleNavigationMeta } = await setStoreThenGoToSale(
      page,
      store,
      debugPaths
    );
    await ensureOnSalePage(page);
    debugPaths.push(...(await captureDebug(page, store.slug, "after_store")));
    const { products: allProducts, tilesFound } = await loadProductsByPagination(
      page,
      store,
      jsonResponses,
      debugPaths,
      saleNavigationMeta
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

    return writeStoreOutput(store, allProducts, storeSynced, tilesFound);
  } catch (error) {
    console.error(`⚠️ Store failed (${store.slug}):`, error);
    debugPaths.push(...(await captureDebug(page, store.slug, "store_failed")));
    await uploadDebugArtifactsIfAvailable(debugPaths);
    return null;
  }
}

async function processStoreWithTimeout(browser, store) {
  const timeoutMs = PER_STORE_TIMEOUT_MINUTES * 60 * 1000;
  const hasTimeout = Number.isFinite(timeoutMs) && timeoutMs > 0;
  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
  });
  const page = await context.newPage();

  let timeoutId = null;
  let timedOut = false;

  try {
    const timeoutPromise = new Promise((resolve) => {
      if (!hasTimeout) return;
      timeoutId = setTimeout(() => {
        timedOut = true;
        console.warn(
          `⏱️ Store timeout reached (${PER_STORE_TIMEOUT_MINUTES} minutes) for ${store.slug}. Skipping store and continuing.`
        );
        resolve(null);
      }, timeoutMs);
    });

    const resultPromise = processStore(page, store);
    const result = await Promise.race([resultPromise, timeoutPromise]);
    return { result, timedOut };
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    await context.close().catch(() => {});
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

        const { result, timedOut } = await processStoreWithTimeout(activeBrowser, store);

        if (timedOut) {
          continue;
        }

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

main()
  .catch((error) => {
    console.error("❌ Unhandled error in Princess Auto scraper:", error);
    process.exitCode = 1;
  })
  .finally(() => {
    process.exit(0);
  });
