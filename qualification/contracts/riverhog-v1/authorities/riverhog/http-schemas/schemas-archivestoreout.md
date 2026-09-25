# schemas: ArchiveStoreOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivestoreout:3c6054140a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9491946c66"></a>

- <a id="s-b4d948d077"></a>`type`: `"object"`
- <a id="s-7bbbd444b8"></a>`additionalProperties`: `false`
- <a id="s-65dd1e7fb5"></a>`required`: `["store","incarnation_id","administrative_state","configured","reachable","readable","writable","read_mode","read_priority","write_target","collections","objects","stored_bytes","download_allowance"]`
- <a id="s-f6da33e223"></a>`title`: `"ArchiveStoreOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3be39d85de"></a>`administrative_state` | yes | anyOf=[(type="string"; enum=["bound","disabled","retired"]); (type="null")]; title="Administrative State" |  |
| <a id="s-ed8363ddd4"></a>`collections` | yes | type="integer"; title="Collections" |  |
| <a id="s-2c7e745f43"></a>`configured` | yes | type="boolean"; title="Configured" |  |
| <a id="s-6d9173b8e5"></a>`download_allowance` | yes | anyOf=[([ArchiveDownloadAllowanceOut](schemas-archivedownloadallowanceout.md)); (type="null")] |  |
| <a id="s-89fd01c631"></a>`incarnation_id` | yes | anyOf=[(type="string"; pattern="^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"); (type="null")]; title="Incarnation Id" |  |
| <a id="s-e4bb5e060b"></a>`objects` | yes | type="integer"; title="Objects" |  |
| <a id="s-eb629a8a71"></a>`reachable` | yes | type="boolean"; title="Reachable" |  |
| <a id="s-831daffcba"></a>`read_mode` | yes | anyOf=[(type="string"; enum=["immediate","restore_required"]); (type="null")]; title="Read Mode" |  |
| <a id="s-97417b240e"></a>`read_priority` | yes | type="integer"; title="Read Priority" |  |
| <a id="s-8554c52951"></a>`readable` | yes | type="boolean"; title="Readable" |  |
| <a id="s-889112d32d"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-61213aadde"></a>`stored_bytes` | yes | type="integer"; title="Stored Bytes" |  |
| <a id="s-2afca4502d"></a>`writable` | yes | type="boolean"; title="Writable" |  |
| <a id="s-94e63b11f8"></a>`write_target` | yes | type="boolean"; title="Write Target" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveDownloadAllowanceOut](schemas-archivedownloadallowanceout.md)
- [ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-5c8b59dd48"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveStoreOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1b82c3c247c2924b5b2a17af5b2d70338d992f26b066c283d8fa16b40b15dec -->

```json
{
  "additionalProperties": false,
  "properties": {
    "administrative_state": {
      "anyOf": [
        {
          "enum": [
            "bound",
            "disabled",
            "retired"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Administrative State"
    },
    "collections": {
      "title": "Collections",
      "type": "integer"
    },
    "configured": {
      "title": "Configured",
      "type": "boolean"
    },
    "download_allowance": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveDownloadAllowanceOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "incarnation_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Incarnation Id"
    },
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "reachable": {
      "title": "Reachable",
      "type": "boolean"
    },
    "read_mode": {
      "anyOf": [
        {
          "enum": [
            "immediate",
            "restore_required"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Read Mode"
    },
    "read_priority": {
      "title": "Read Priority",
      "type": "integer"
    },
    "readable": {
      "title": "Readable",
      "type": "boolean"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "writable": {
      "title": "Writable",
      "type": "boolean"
    },
    "write_target": {
      "title": "Write Target",
      "type": "boolean"
    }
  },
  "required": [
    "store",
    "incarnation_id",
    "administrative_state",
    "configured",
    "reachable",
    "readable",
    "writable",
    "read_mode",
    "read_priority",
    "write_target",
    "collections",
    "objects",
    "stored_bytes",
    "download_allowance"
  ],
  "title": "ArchiveStoreOut",
  "type": "object"
}
```

</details>
