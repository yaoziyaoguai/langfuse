import { z } from "zod";

export const PRODUCT_MODULES = [
  "dashboards",
  "tracing",
  "evaluation",
  "prompt-management",
  "playground",
  "datasets",
] as const;

export const ProductModule = z.enum(PRODUCT_MODULES);
export type ProductModule = z.infer<typeof ProductModule>;

export function getVisibleProductModules(
  visibleModules?: string,
  hiddenModules?: string,
): ProductModule[] {
  if (visibleModules) return parseProductModules(visibleModules);
  if (!hiddenModules) return [...PRODUCT_MODULES];

  const hidden = new Set(parseProductModules(hiddenModules));
  return PRODUCT_MODULES.filter((module) => !hidden.has(module));
}

function parseProductModules(value: string): ProductModule[] {
  const parsed = value
    .split(",")
    .map((module) => module.trim().toLowerCase())
    .filter(Boolean)
    .filter(
      (module): module is ProductModule =>
        ProductModule.safeParse(module).success,
    );
  return [...new Set(parsed)];
}
