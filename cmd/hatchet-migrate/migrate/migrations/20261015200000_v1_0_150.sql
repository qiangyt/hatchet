-- +goose Up
-- Guards the engine-critical internal tenant (seeded by
-- 20240216133745_v0_11_0 with fixed uuid
-- 8d420720-ef03-41dc-9c73-1c93f276db97) against deletion.
--
-- The pre-existing prevent_internal_name_or_slug trigger fires on INSERT and
-- UPDATE whenever the row's name or slug is 'internal', which already blocks
-- any UPDATE of the row (including soft-deletes) — but a bare DELETE bypassed
-- it: the internal tenant could be hard-deleted by a direct SQL session, after
-- which the OLAP controller's partition maintenance job stops resolving it and
-- silently stops creating daily partitions (its lookup maps the missing row to
-- nil). Once the boot-time partitions ran out, every REST read against the
-- OLAP tables failed fleet-wide. This trigger closes the DELETE gap and states
-- the soft-delete invariant explicitly.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION prevent_internal_tenant_deletion()
RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD."slug" = 'internal' OR left(OLD."slug", 17) = 'internal_deleted_' THEN
      RAISE EXCEPTION 'the internal tenant (%) cannot be deleted: partition maintenance depends on it', OLD."id";
    END IF;
    RETURN OLD;
  END IF;
  -- TG_OP = 'UPDATE'
  IF OLD."slug" = 'internal' AND NEW."deletedAt" IS NOT NULL THEN
    RAISE EXCEPTION 'the internal tenant cannot be soft-deleted: partition maintenance depends on it';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_internal_tenant_deletion_before_update_or_delete
BEFORE UPDATE OR DELETE ON "Tenant"
FOR EACH ROW EXECUTE FUNCTION prevent_internal_tenant_deletion();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS check_internal_tenant_deletion_before_update_or_delete ON "Tenant";
DROP FUNCTION IF EXISTS prevent_internal_tenant_deletion();
-- +goose StatementEnd
