// See: https://vercel.com/docs/observability/otel-overview
export async function register() {
  // This variable is set in the .env file or environment variables
  // Value is true if NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT is "true" or undefined
  const isInitLoadingEnabled =
    process.env.NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT !== undefined
      ? process.env.NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT === "true"
      : true;

  if (process.env.NEXT_RUNTIME === "nodejs") {
    if (isInitLoadingEnabled) {
      console.log("Running init scripts...");
      await import("./observability.config");
    }

    const { initializeWebAnalyticsRuntime } =
      await import("./server/analyticsRuntime");
    await initializeWebAnalyticsRuntime();

    if (isInitLoadingEnabled) {
      await import("./initialize");
    }
  }
}
