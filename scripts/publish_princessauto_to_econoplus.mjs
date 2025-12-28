import fs from "fs/promises";
import path from "path";

const args = process.argv.slice(2);

function getArg(flag) {
  const index = args.indexOf(flag);
  if (index === -1 || index === args.length - 1) return undefined;
  return args[index + 1];
}

const storesPath = getArg("--stores");
const outputsDir = getArg("--outputs");
const econoplusDir = getArg("--econoplus");

if (!storesPath || !outputsDir || !econoplusDir) {
  console.error(
    "Usage: node scripts/publish_princessauto_to_econoplus.mjs --stores <path> --outputs <path> --econoplus <path>"
  );
  process.exit(1);
}

async function fileExists(targetPath) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function loadJson(jsonPath, fallback) {
  if (!(await fileExists(jsonPath))) return fallback;
  const raw = await fs.readFile(jsonPath, "utf8");
  return JSON.parse(raw);
}

async function main() {
  const resolvedStoresPath = path.resolve(storesPath);
  const resolvedOutputsDir = path.resolve(outputsDir);
  const resolvedEconoplusDir = path.resolve(econoplusDir);

  const stores = await loadJson(resolvedStoresPath, []);
  const storesBySlug = new Map(stores.map((store) => [store.slug, store]));

  await fs.mkdir(resolvedEconoplusDir, { recursive: true });
  await fs.cp(resolvedStoresPath, path.join(resolvedEconoplusDir, "stores.json"));

  const outputEntries = (await fileExists(resolvedOutputsDir))
    ? await fs.readdir(resolvedOutputsDir, { withFileTypes: true })
    : [];

  const publishedSlugs = [];

  for (const entry of outputEntries) {
    if (!entry.isDirectory()) continue;

    const slug = entry.name;
    const outputDataPath = path.join(resolvedOutputsDir, slug, "data.json");
    const outputCsvPath = path.join(resolvedOutputsDir, slug, "data.csv");

    if (!(await fileExists(outputDataPath))) continue;

    const outputData = await loadJson(outputDataPath, []);
    const normalizedOutput = Array.isArray(outputData)
      ? { products: outputData }
      : outputData ?? {};
    const products = Array.isArray(normalizedOutput.products)
      ? normalizedOutput.products
      : [];
    const updatedAt = normalizedOutput.updatedAt ?? new Date().toISOString();
    const storeMetadata = storesBySlug.get(slug) ?? {};

    const merged = {
      ...normalizedOutput,
      ...storeMetadata,
      slug,
      updatedAt,
      products,
    };

    const targetDir = path.join(resolvedEconoplusDir, slug);
    await fs.mkdir(targetDir, { recursive: true });

    await fs.writeFile(
      path.join(targetDir, "data.json"),
      JSON.stringify(merged, null, 2)
    );

    if (await fileExists(outputCsvPath)) {
      await fs.cp(outputCsvPath, path.join(targetDir, "data.csv"));
    }

    publishedSlugs.push(slug);
  }

  await fs.writeFile(
    path.join(resolvedEconoplusDir, "index.json"),
    JSON.stringify(
      {
        updatedAt: new Date().toISOString(),
        storesPublished: publishedSlugs,
      },
      null,
      2
    )
  );

  console.log(JSON.stringify(publishedSlugs));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
