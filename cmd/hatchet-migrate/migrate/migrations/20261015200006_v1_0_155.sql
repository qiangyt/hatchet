-- +goose Up
-- Fork ordering: keep this migration after 20261015200000 so fork-lineage
-- databases apply it in the same boot as the engine that requires it.
-- +goose StatementBegin
ALTER TABLE v1_dags_olap ADD COLUMN is_dag_operator BOOLEAN NOT NULL DEFAULT FALSE;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE v1_dags_olap DROP COLUMN is_dag_operator;
-- +goose StatementEnd
