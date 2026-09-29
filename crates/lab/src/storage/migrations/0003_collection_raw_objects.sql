CREATE TABLE collection_raw_objects (
    request_id TEXT NOT NULL REFERENCES collections(request_id),
    raw_object_id TEXT NOT NULL REFERENCES raw_objects(id),
    linked_index INTEGER NOT NULL CHECK (linked_index >= 0),
    PRIMARY KEY (request_id, raw_object_id),
    UNIQUE (request_id, linked_index)
) STRICT;

INSERT INTO collection_raw_objects(request_id, raw_object_id, linked_index)
SELECT request_id, raw_object_id,
       ROW_NUMBER() OVER (
           PARTITION BY request_id
           ORDER BY market, page_index, raw_object_id
       ) - 1
FROM collection_pages;
