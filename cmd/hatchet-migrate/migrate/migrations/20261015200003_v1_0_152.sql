-- +goose Up
-- Fork ordering: keep this migration after 20261015200000 so fork-lineage
-- databases apply it in the same boot as the engine that requires it.
-- +goose StatementBegin
-- Identifies the listener session that last activated the worker, so only that session can
-- deactivate it when listener sessions overlap.
ALTER TABLE "Worker" ADD COLUMN "lastListenerSessionId" UUID;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "Worker" DROP COLUMN "lastListenerSessionId";
-- +goose StatementEnd
