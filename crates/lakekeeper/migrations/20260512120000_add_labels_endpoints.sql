-- Add LoadLabels / UpdateLabels endpoints to the api_endpoints enum.
-- Companion to the IRC UpdateLabels REST verb proposal
-- (see projects/iceberg-governance/labels/labels-crud-followup.md).

ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'catalog-v1-load-labels';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'catalog-v1-update-labels';
