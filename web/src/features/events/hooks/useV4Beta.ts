import { useCallback } from "react";

type SetV4EnabledOptions = {
  onSuccess?: () => void | Promise<void>;
};

/** Compatibility hook for views that predate the Doris-only canonical model. */
export function useV4Beta() {
  const setBetaEnabled = useCallback(
    async (_enabled: boolean, options?: SetV4EnabledOptions) => {
      await options?.onSuccess?.();
    },
    [],
  );

  const enableWithIntro = useCallback(async (options?: SetV4EnabledOptions) => {
    await options?.onSuccess?.();
  }, []);

  const noOp = useCallback(() => undefined, []);

  return {
    isBetaEnabled: true,
    canToggleV4: false,
    isInitializing: false,
    setBetaEnabled,
    enableWithIntro,
    showIntroDialog: false,
    confirmIntroDialog: noOp,
    dismissIntroDialog: noOp,
    isLoading: false,
  };
}
