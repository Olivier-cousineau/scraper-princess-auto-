#!/usr/bin/env node
//
// Upsert scraped deals into Supabase.
//
// Usage:
//   node scripts/upsert_to_supabase.mjs <retailer_slug> <json_glob_or_file>
//
// Examples:
//   node scripts/upsert_to_supabase.mjs bestbuy outputs/bestbuy/clearance.json
//   node scripts/upsert_to_supabase.mjs canadiantire 'public/canadiantire/*/data.json'
//   node scripts/upsert_to_supabase.mjs canac 'public/canac/*.json'
//   node scripts/upsert_to_supabase.mjs ikea 'public/ikea/*/data.json'
//   node scripts/upsert_to_supabase.mjs sportinglife 'public/sportinglife/*.json' --exclude=stores.json
//   node scripts/upsert_to_supabase.mjs princessauto 'public/princessauto/*/data.json'
//
// Environment:
//   SUPABASE_URL            - Supabase project URL
//   SUPABASE_SERVICE_KEY    - Supabase service role key
//
// This script is designed to be copied into each scraper repo and run as a
// GitHub Actions step after the publish-to-econoplus step.
//

import fs from "node:fs";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import { glob } from "glob";

// ── Config ──────────────────────────────────────────────────────────────────

const BATCH_SIZE = 500;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY env vars");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── CLI args ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const retailerSlug = args[0];
const filePattern = args[1];
const flags = Object.fromEntries(
  args.slice(2).filter((a) => a.startsWith("--")).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);
const excludeFiles = flags.exclude ? flags.exclude.split(",") : [];

if (!retailerSlug || !filePattern) {
  console.error("Usage: node upsert_to_supabase.mjs <retailer_slug> <glob_pattern>");
  process.exit(1);
}

// ── Price parser (same logic as normalize_deals.mjs) ────────────────────────

function parsePrice(input) {
  if (input === null || input === undefined) return null;
  if (typeof input === "number" && Number.isFinite(input)) return input;
  if (typeof input !== "string") return null;
  const cleaned = input.replace(/\u00a0/g, " ").replace(/\$/g, "").replace(/Maintenant/gi, "").trim();
  const match = cleaned.match(/[-\d.,]+/);
  if (!match) return null;
  const raw = match[0].replace(/\s+/g, "");
  const normalized = raw.replace(/\.(?=\d{3}(\D|$))/g, "").replace(/,/g, ".");
  const value = parseFloat(normalized);
  return Number.isFinite(value) ? value : null;
}

function computeDiscount(regular, sale) {
  if (!Number.isFinite(regular) || !Number.isFinite(sale) || regular <= 0 || sale > regular) return null;
  return Math.round((1 - sale / regular) * 100);
}

// ── Retailer-specific extractors ────────────────────────────────────────────
// Each returns { externalId, title, url, imageUrl, sku, brand, category,
//                salePrice, regularPrice, discountPct, badges, availability,
//                storeExternalId? }

const extractors = {
  bestbuy(item, _filePath) {
    return {
      externalId: item.url?.match(/\/(\d+)\.aspx/)?.[1] ?? item.url ?? null,
      title: item.title ?? item.name,
      url: item.url,
      imageUrl: item.image ?? null,
      sku: null,
      brand: null,
      category: null,
      salePrice: parsePrice(item.salePrice ?? item.price),
      regularPrice: parsePrice(item.price_raw ?? item.price),
      discountPct: null,
      badges: null,
      availability: null,
      storeExternalId: "online",
    };
  },

  canadiantire(item, filePath) {
    const storeId = item.store_id ?? path.basename(path.dirname(filePath)).match(/^(\d+)/)?.[1] ?? null;
    return {
      externalId: item.sku ?? item.product_id ?? item.url,
      title: item.title ?? item.name,
      url: item.url ?? item.link,
      imageUrl: item.image ?? item.image_url,
      sku: item.sku,
      brand: item.brand ?? null,
      category: null,
      salePrice: parsePrice(item.liquidation_price ?? item.sale_price ?? item.price),
      regularPrice: parsePrice(item.regular_price),
      discountPct: parsePrice(item.discount_percent),
      badges: Array.isArray(item.badges) ? item.badges : null,
      availability: item.availability ?? null,
      storeExternalId: storeId,
    };
  },

  canac(item, _filePath) {
    return {
      externalId: item.sku || item.url,
      title: item.name,
      url: item.url,
      imageUrl: item.image,
      sku: item.sku || null,
      brand: null,
      category: item.category ?? null,
      salePrice: parsePrice(item.price_sale),
      regularPrice: parsePrice(item.price_regular),
      discountPct: parsePrice(item.discount_pct),
      badges: null,
      availability: item.stock_text ?? null,
      storeExternalId: item.store_id ?? null,
    };
  },

  sportinglife(item, filePath) {
    const productId = item.link?.match(/\/(\d{5,})/)?.[1] ?? item.link ?? item.name;
    return {
      externalId: productId,
      title: item.name,
      url: item.link ? (item.link.startsWith("http") ? item.link : `https://www.sportinglife.ca${item.link}`) : null,
      imageUrl: item.image ?? null,
      sku: null,
      brand: item.brand ?? null,
      category: null,
      salePrice: parsePrice(item.price),
      regularPrice: parsePrice(item.originalPrice),
      discountPct: parsePrice(item.discount),
      badges: null,
      availability: null,
      storeExternalId: null, // resolved from file wrapper
    };
  },

  princessauto(item, _filePath) {
    const url = item.productUrl ?? item.url;
    const productId = item.sku ?? url?.match(/\/(\d+)\/?/)?.[1] ?? url ?? item.name;
    return {
      externalId: productId,
      title: item.name,
      url: url ? (url.startsWith("http") ? url : `https://www.princessauto.com${url}`) : null,
      imageUrl: item.imageUrl ?? null,
      sku: item.sku ?? null,
      brand: item.brand ?? null,
      category: null,
      salePrice: parsePrice(item.priceSale ?? item.currentPrice),
      regularPrice: parsePrice(item.priceRegular ?? item.originalPrice),
      discountPct: parsePrice(item.discountPercent),
      badges: null,
      availability: item.availability ?? null,
      storeExternalId: null, // resolved from file wrapper
    };
  },

  ikea(item, _filePath) {
    const productId = item.url?.match(/(\d{8})/)?.[1] ?? item.url ?? item.name;
    const priceParsed = parsePrice(item.price);
    return {
      externalId: productId,
      title: [item.name, item.typeName].filter(Boolean).join(" - "),
      url: item.url ? (item.url.startsWith("http") ? item.url : `https://www.ikea.com${item.url}`) : null,
      imageUrl: item.image ?? null,
      sku: null,
      brand: "IKEA",
      category: item.typeName ?? null,
      salePrice: priceParsed,
      regularPrice: null,
      discountPct: null,
      badges: null,
      availability: item.inStockStore ?? null,
      storeExternalId: null, // resolved from file wrapper
    };
  },

  sportexpert(item, _filePath) {
    return {
      externalId: item.url ?? item.title,
      title: item.title,
      url: item.url ? (item.url.startsWith("http") ? item.url : `https://www.sportsexperts.ca${item.url}`) : null,
      imageUrl: item.image_url ?? null,
      sku: null,
      brand: item.brand ?? null,
      category: null,
      salePrice: parsePrice(item.price_sale),
      regularPrice: parsePrice(item.price_regular),
      discountPct: null,
      badges: item.discount_label ? [item.discount_label] : null,
      availability: null,
      storeExternalId: "online",
    };
  },
};

// ── File loading helpers ────────────────────────────────────────────────────

function loadJsonFile(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw);
}

function extractItems(data, retailerSlug, filePath) {
  // Some retailers wrap items in an object
  if (Array.isArray(data)) return { items: data, storeExternalId: null };

  // Sporting Life / Princess Auto / IKEA: { products: [...] }
  if (data.products && Array.isArray(data.products)) {
    const storeKey = data.store?.storeKey ?? data.store?.slug ?? data.slug ?? data.storeId?.toString() ?? null;
    return { items: data.products, storeExternalId: storeKey };
  }

  // SportExpert: { items: [...] }
  if (data.items && Array.isArray(data.items)) {
    return { items: data.items, storeExternalId: "online" };
  }

  // IKEA: { products: [...] } — already handled above

  console.warn(`Unknown data shape in ${filePath}, skipping`);
  return { items: [], storeExternalId: null };
}

function inferStoreExternalId(filePath, retailerSlug) {
  // Try to infer store external ID from file path
  const parts = filePath.split("/");

  if (retailerSlug === "canadiantire") {
    // public/canadiantire/0001-alliston-on/data.json → "0001"
    const folder = parts.find((p) => /^\d{4}/.test(p));
    return folder?.match(/^(\d{4})/)?.[1] ?? null;
  }
  if (retailerSlug === "canac") {
    // public/canac/39_AUB_liquidation.json → "39"
    const file = path.basename(filePath, ".json");
    return file.match(/^(\d+)/)?.[1] ?? null;
  }
  if (retailerSlug === "ikea" || retailerSlug === "princessauto") {
    // public/ikea/montreal-qc/data.json → slug
    const idx = parts.indexOf(retailerSlug);
    return idx >= 0 && idx + 1 < parts.length ? parts[idx + 1] : null;
  }
  if (retailerSlug === "sportinglife") {
    // public/sportinglife/anderson-road-calgary-ab.json → slug
    const file = path.basename(filePath, ".json");
    return file !== "stores" ? file : null;
  }
  if (retailerSlug === "bestbuy") {
    return "online";
  }
  if (retailerSlug === "sportexpert") {
    return "online";
  }
  return null;
}

// ── Supabase upsert logic ───────────────────────────────────────────────────

async function getRetailerId(slug) {
  const { data, error } = await supabase
    .from("retailers")
    .select("id")
    .eq("slug", slug)
    .single();
  if (error || !data) {
    throw new Error(`Retailer "${slug}" not found in Supabase: ${error?.message}`);
  }
  return data.id;
}

async function getOrCreateStore(retailerId, externalId, name) {
  if (!externalId) return null;

  const { data: existing } = await supabase
    .from("stores")
    .select("id")
    .eq("retailer_id", retailerId)
    .eq("external_id", externalId)
    .single();

  if (existing) return existing.id;

  const { data: created, error } = await supabase
    .from("stores")
    .insert({ retailer_id: retailerId, external_id: externalId, name: name ?? externalId })
    .select("id")
    .single();

  if (error) {
    // Might have been created concurrently
    const { data: retry } = await supabase
      .from("stores")
      .select("id")
      .eq("retailer_id", retailerId)
      .eq("external_id", externalId)
      .single();
    if (retry) return retry.id;
    throw new Error(`Failed to create store ${externalId}: ${error.message}`);
  }
  return created.id;
}

const storeIdCache = new Map();

async function resolveStoreId(retailerId, externalId, name) {
  if (!externalId) return null;
  const key = `${retailerId}:${externalId}`;
  if (storeIdCache.has(key)) return storeIdCache.get(key);
  const id = await getOrCreateStore(retailerId, externalId, name);
  storeIdCache.set(key, id);
  return id;
}

async function upsertBatch(retailerId, products, deals) {
  // 1. Deduplicate products by external_id (keep last occurrence)
  const deduped = new Map();
  for (const p of products) {
    deduped.set(p.external_id, p);
  }
  const uniqueProducts = [...deduped.values()];

  // 2. Upsert products
  if (uniqueProducts.length > 0) {
    const { error: prodErr } = await supabase
      .from("products")
      .upsert(uniqueProducts, { onConflict: "retailer_id,external_id", ignoreDuplicates: false });
    if (prodErr) {
      console.error(`Product upsert error: ${prodErr.message}`);
      return { productsUpserted: 0, dealsUpserted: 0, errors: 1 };
    }
  }

  // 2. Resolve product IDs for deals
  const externalIds = [...new Set(products.map((p) => p.external_id))];
  const productIdMap = new Map();

  for (let i = 0; i < externalIds.length; i += 100) {
    const batch = externalIds.slice(i, i + 100);
    const { data: resolved } = await supabase
      .from("products")
      .select("id, external_id")
      .eq("retailer_id", retailerId)
      .in("external_id", batch);
    if (resolved) {
      for (const p of resolved) {
        productIdMap.set(p.external_id, p.id);
      }
    }
  }

  // 3. Build deal rows with resolved product_id, deduplicate by (product_id, store_id)
  const dealDeduped = new Map();
  for (const d of deals) {
    const productId = productIdMap.get(d._external_id);
    if (!productId || !d.store_id) continue;
    const { _external_id, ...rest } = d;
    const key = `${productId}:${d.store_id}`;
    dealDeduped.set(key, { ...rest, product_id: productId });
  }
  const dealRows = [...dealDeduped.values()];

  // 4. Upsert deals
  let dealsUpserted = 0;
  for (let i = 0; i < dealRows.length; i += BATCH_SIZE) {
    const batch = dealRows.slice(i, i + BATCH_SIZE);
    const { error: dealErr } = await supabase
      .from("deals")
      .upsert(batch, { onConflict: "product_id,store_id", ignoreDuplicates: false });
    if (dealErr) {
      console.error(`Deal upsert error (batch ${i}): ${dealErr.message}`);
    } else {
      dealsUpserted += batch.length;
    }
  }

  return { productsUpserted: products.length, dealsUpserted, errors: 0 };
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const extractor = extractors[retailerSlug];
  if (!extractor) {
    console.error(`No extractor for retailer "${retailerSlug}". Supported: ${Object.keys(extractors).join(", ")}`);
    process.exit(1);
  }

  // Resolve files
  const files = await glob(filePattern);
  const filteredFiles = files.filter((f) => {
    const base = path.basename(f);
    if (excludeFiles.includes(base)) return false;
    if (base === "stores.json" || base === "index.json" || base === "products-index.json") return false;
    if (base === "_orchestrator_state.json") return false;
    return true;
  });

  if (filteredFiles.length === 0) {
    console.log(`No files matched pattern "${filePattern}"`);
    process.exit(0);
  }

  console.log(`Found ${filteredFiles.length} files for ${retailerSlug}`);

  const retailerId = await getRetailerId(retailerSlug);
  let totalProducts = 0;
  let totalDeals = 0;
  let totalErrors = 0;

  for (const filePath of filteredFiles) {
    let data;
    try {
      data = loadJsonFile(filePath);
    } catch (e) {
      console.warn(`Failed to parse ${filePath}: ${e.message}`);
      totalErrors++;
      continue;
    }

    const { items, storeExternalId: wrapperStoreId } = extractItems(data, retailerSlug, filePath);

    if (items.length === 0) {
      console.log(`Skipping empty file: ${filePath}`);
      continue;
    }

    const inferredStoreId = wrapperStoreId ?? inferStoreExternalId(filePath, retailerSlug);

    // Process items in batches
    const products = [];
    const deals = [];

    for (const item of items) {
      const extracted = extractor(item, filePath);
      if (!extracted || !extracted.externalId || !extracted.title) continue;

      const storeExtId = extracted.storeExternalId ?? inferredStoreId;
      if (!storeExtId) {
        continue;
      }

      const storeId = await resolveStoreId(retailerId, storeExtId, storeExtId);
      if (!storeId) continue;

      const discountPct = extracted.discountPct ?? computeDiscount(extracted.regularPrice, extracted.salePrice);

      products.push({
        retailer_id: retailerId,
        external_id: String(extracted.externalId),
        sku: extracted.sku,
        title: extracted.title,
        brand: extracted.brand,
        url: extracted.url,
        image_url: extracted.imageUrl,
        category: extracted.category,
        last_seen_at: new Date().toISOString(),
        is_active: true,
      });

      deals.push({
        _external_id: String(extracted.externalId),
        store_id: storeId,
        current_price: extracted.salePrice,
        original_price: extracted.regularPrice,
        discount_pct: discountPct,
        badges: extracted.badges,
        availability: extracted.availability,
        is_on_sale: true,
        last_scraped_at: new Date().toISOString(),
      });
    }

    // Upsert in batches
    for (let i = 0; i < products.length; i += BATCH_SIZE) {
      const prodBatch = products.slice(i, i + BATCH_SIZE);
      const dealBatch = deals.slice(i, i + BATCH_SIZE);
      const result = await upsertBatch(retailerId, prodBatch, dealBatch);
      totalProducts += result.productsUpserted;
      totalDeals += result.dealsUpserted;
      totalErrors += result.errors;
    }

    console.log(`${filePath}: ${items.length} items → ${products.length} products, ${deals.length} deals`);
  }

  console.log(`\nDone: ${totalProducts} products, ${totalDeals} deals upserted, ${totalErrors} errors`);

  // Record scrape run
  const { error: runErr } = await supabase.from("scrape_runs").insert({
    retailer_id: retailerId,
    stores_scraped: storeIdCache.size,
    products_found: totalProducts,
    status: totalErrors > 0 ? "partial" : "success",
    completed_at: new Date().toISOString(),
  });
  if (runErr) {
    console.warn(`Failed to record scrape run: ${runErr.message}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
