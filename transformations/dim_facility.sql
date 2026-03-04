-- ============================================================
-- Populate dim_facility dimension
-- ============================================================

USE university_dw;

DELETE FROM dim_facility;

INSERT INTO dim_facility (facility_id, building, room_number, room_type, capacity, has_projector, has_computers)
SELECT 
    facility_id,
    building,
    room_number,
    room_type,
    capacity,
    has_projector,
    has_computers
FROM staging_facilities;

SELECT COUNT(*) AS total_facilities FROM dim_facility;
SELECT TOP 5 * FROM dim_facility;
