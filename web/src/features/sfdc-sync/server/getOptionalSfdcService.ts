import { env } from "@/src/env.mjs";

export async function getOptionalSfdcService() {
  // 自托管部署不加载上游 Cloud 集成；Cloud 继续使用原有服务。
  if (!env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION) return null;

  const { getSfdcService } = await import("@/src/ee/features/sfdc-sync/server");
  return getSfdcService();
}
