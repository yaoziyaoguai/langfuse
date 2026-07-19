import type { Model } from "@langfuse/shared";
import {
  findModel,
  instrumentAsync,
  logger,
  matchPricingTier,
  recordIncrement,
  traceException,
} from "@langfuse/shared/src/server";
import type { Decimal } from "decimal.js";

import { tokenCountAsync } from "../features/tokenisation/async-usage";
import { tokenCount } from "../features/tokenisation/usage";
import type { EventCanonicalizerDependencies } from "./EventCanonicalizer";

type ResolveGenerationUsage =
  EventCanonicalizerDependencies["resolveGenerationUsage"];
type GenerationUsageInput = Parameters<ResolveGenerationUsage>[0];
type FindModel = typeof findModel;

function normalizeUsage(
  providedUsageDetails: Readonly<Record<string, number>>,
): Record<string, number> {
  const normalized: Record<string, number> = {};
  for (const [key, value] of Object.entries(providedUsageDetails)) {
    const number = Number(value);
    if (Number.isFinite(number) && number >= 0) normalized[key] = number;
  }
  return normalized;
}

function calculateCosts(input: {
  readonly modelPrices: readonly { usageType: string; price: Decimal }[];
  readonly providedCosts: Readonly<Record<string, number>>;
  readonly usage: Readonly<Record<string, number>>;
}): { costDetails: Record<string, number>; totalCost: number | null } {
  const provided = Object.entries(input.providedCosts).filter(
    ([, value]) => value != null && Number.isFinite(Number(value)),
  );
  if (provided.length > 0) {
    const costDetails = Object.fromEntries(
      provided.map(([key, value]) => [key, Number(value)]),
    );
    const total =
      costDetails.total ??
      (provided.every(([key]) => key === "input" || key === "output")
        ? (costDetails.input ?? 0) + (costDetails.output ?? 0)
        : null);
    if (total !== null) costDetails.total = total;
    return { costDetails, totalCost: total };
  }

  const priced = Object.entries(input.usage).flatMap(([usageType, units]) => {
    const price = input.modelPrices.find(
      (candidate) => candidate.usageType === usageType,
    );
    return price
      ? [[usageType, price.price.mul(units).toNumber()] as const]
      : [];
  });
  const costDetails = Object.fromEntries(priced);
  if (priced.length === 0) return { costDetails, totalCost: null };
  const totalCost =
    costDetails.total ?? priced.reduce((sum, [, value]) => sum + value, 0);
  costDetails.total = totalCost;
  return { costDetails, totalCost };
}

export class AnalyticsGenerationUsageResolver {
  constructor(
    private readonly dependencies: {
      readonly findModel?: FindModel;
      readonly countAsync?: typeof tokenCountAsync;
      readonly countSync?: typeof tokenCount;
    } = {},
  ) {}

  readonly resolve: ResolveGenerationUsage = async (input) => {
    const { model, pricingTiers } = await (
      this.dependencies.findModel ?? findModel
    )({
      projectId: input.projectId,
      model: input.providedModelName,
    });
    const usageDetails = await this.resolveUsage(input, model);
    const matchedTier =
      pricingTiers.length > 0
        ? matchPricingTier(pricingTiers, usageDetails)
        : null;
    const modelPrices = matchedTier
      ? Object.entries(matchedTier.prices).map(([usageType, price]) => ({
          usageType,
          price,
        }))
      : [];
    const costs = calculateCosts({
      modelPrices,
      providedCosts: input.providedCostDetails,
      usage: usageDetails,
    });
    return {
      internalModelId: model?.id ?? null,
      usageDetails,
      costDetails: costs.costDetails,
      totalCost: costs.totalCost,
      usagePricingTierId: matchedTier?.pricingTierId ?? null,
      usagePricingTierName: matchedTier?.pricingTierName ?? null,
    };
  };

  private async resolveUsage(
    input: GenerationUsageInput,
    model: Model | null | undefined,
  ): Promise<Record<string, number>> {
    const provided = normalizeUsage(input.providedUsageDetails);
    if (!model || input.level === "ERROR" || Object.keys(provided).length > 0) {
      if (!("total" in provided) && Object.keys(provided).length > 0) {
        provided.total = Object.values(provided).reduce(
          (sum, value) => sum + value,
          0,
        );
      }
      return provided;
    }

    try {
      let inputCount: number | undefined;
      let outputCount: number | undefined;
      await instrumentAsync({ name: "token-count" }, async (span) => {
        try {
          [inputCount, outputCount] = await Promise.all([
            (this.dependencies.countAsync ?? tokenCountAsync)({
              text: input.input,
              model,
            }),
            (this.dependencies.countAsync ?? tokenCountAsync)({
              text: input.output,
              model,
            }),
          ]);
        } catch (error) {
          logger.warn(
            "Async tokenization failed; using synchronous tokenization",
            error,
          );
          inputCount = (this.dependencies.countSync ?? tokenCount)({
            text: input.input,
            model,
          });
          outputCount = (this.dependencies.countSync ?? tokenCount)({
            text: input.output,
            model,
          });
        }
        if (inputCount != null) {
          span.setAttribute("langfuse.tokenization.input-count", inputCount);
          recordIncrement("langfuse.tokenisedTokens", inputCount);
        }
        if (outputCount != null) {
          span.setAttribute("langfuse.tokenization.output-count", outputCount);
          recordIncrement("langfuse.tokenisedTokens", outputCount);
        }
        span.setAttribute(
          "langfuse.tokenization.tokenizer",
          model.tokenizerId || "unknown",
        );
      });
      const usage: Record<string, number> = {};
      if (inputCount != null) usage.input = inputCount;
      if (outputCount != null) usage.output = outputCount;
      if (inputCount != null || outputCount != null) {
        usage.total = (inputCount ?? 0) + (outputCount ?? 0);
      }
      return usage;
    } catch (error) {
      traceException(error);
      logger.error("Tokenization failed; continuing without usage", {
        error,
        projectId: input.projectId,
        spanId: input.spanId,
      });
      return {};
    }
  }
}

let lastMismatchLogAt = 0;

export function warnOnUsageTotalMismatch(
  rawUsage: Readonly<Record<string, number>>,
  identity: { readonly projectId: string; readonly spanId: string },
): void {
  const usage = normalizeUsage(rawUsage);
  const total = usage.total;
  if (total == null) return;
  const bucketSum = Object.entries(usage)
    .filter(([key]) => key !== "total")
    .reduce((sum, [, value]) => sum + value, 0);
  if (bucketSum <= total + Math.max(1, total * 0.01)) return;
  recordIncrement("langfuse.ingestion.usage_details.total_mismatch", 1, {
    write_path: "doris",
  });
  const now = Date.now();
  if (now - lastMismatchLogAt < 60_000) return;
  lastMismatchLogAt = now;
  logger.warn("Provided usage buckets exceed provided total", {
    projectId: identity.projectId,
    observationId: identity.spanId,
    providedTotal: total,
    bucketSum,
  });
}
