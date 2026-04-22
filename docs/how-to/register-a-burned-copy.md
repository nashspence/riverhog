# Register a burned copy

Once an image has been explicitly finalized, downloaded, and burned, register the physical copy so archival coverage
can be counted.

CLI example:

```bash
arc copy add 20260420T040001Z BR-021-A --at 'Shelf B1'
```

The first positional argument is the finalized `image_id`.
Finalized image ids use compact UTC basic form `YYYYMMDDTHHMMSSZ`.

Equivalent API request:

```http
POST /v1/images/20260420T040001Z/copies
Content-Type: application/json
```

```json
{
  "id": "BR-021-A",
  "location": "Shelf B1"
}
```

Notes:

- registration is valid only after explicit image finalization has created the finalized image id
- the physical copy identity is `(volume_id, id)`
- the user-supplied `id` must be unique within that finalized image/`volume_id`
- `location` is mutable metadata and is not part of the copy identity

Registering a copy does not change hot presence by itself. It updates archival coverage for files contained in the
image.
