export type IngestionMaskingInput<T> = {
  data: T;
  projectId: string;
  orgId?: string;
  propagatedHeaders?: Readonly<Record<string, string>>;
};

export type IngestionMaskingResult<T> = {
  success: boolean;
  data: T;
  masked: boolean;
  error?: string;
};
