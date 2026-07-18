ALTER TABLE "analytics_entity_heads"
ADD COLUMN "lookup_id" TEXT;

CREATE INDEX "analytics_entity_heads_project_id_entity_type_lookup_id_idx"
ON "analytics_entity_heads"("project_id", "entity_type", "lookup_id");
