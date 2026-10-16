ALTER TABLE automation_orders ADD COLUMN remaining_volume_nullable TEXT;
UPDATE automation_orders
SET remaining_volume_nullable = remaining_volume;
ALTER TABLE automation_orders DROP COLUMN remaining_volume;
ALTER TABLE automation_orders RENAME COLUMN remaining_volume_nullable TO remaining_volume;
