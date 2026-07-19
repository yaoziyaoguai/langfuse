import Page from "@/src/components/layouts/page";
import {
  COMMUNITY_CAPABILITIES,
  type CommunityCapability,
} from "@/src/features/capabilities/communityAvailability";

export function UnavailableFeaturePage({
  capability,
}: {
  capability: CommunityCapability;
}) {
  const unavailable = COMMUNITY_CAPABILITIES[capability];
  return (
    <Page headerProps={{ title: "Feature unavailable" }}>
      <div className="bg-card mx-auto flex max-w-2xl flex-col gap-4 rounded-lg border p-6">
        <div className="text-muted-foreground font-mono text-sm">
          {unavailable.code}
        </div>
        <h1 className="text-xl font-semibold">{unavailable.message}</h1>
        <p className="text-muted-foreground text-sm">{unavailable.recovery}</p>
      </div>
    </Page>
  );
}

export const EvaluationsUnavailablePage = () => (
  <UnavailableFeaturePage capability="evaluations" />
);
export const ExperimentsUnavailablePage = () => (
  <UnavailableFeaturePage capability="experiments" />
);
export const MonitorsUnavailablePage = () => (
  <UnavailableFeaturePage capability="monitors" />
);
