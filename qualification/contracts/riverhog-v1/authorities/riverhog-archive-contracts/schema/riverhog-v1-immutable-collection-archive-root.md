# Riverhog v1 immutable collection archive root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-archive-contracts:riverhog-v1-immutable-collection-archive-root:684e181178 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-6eb5b547a1"></a>

- <a id="s-37b1703eda"></a>`type`: `"object"`
- <a id="s-61a8fe496e"></a>`$comment`: `"This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveManifest is the canonical semantic, identity, and canonical-JSON authority."`
- <a id="s-161d22fcb6"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json"`
- <a id="s-fff8ac27cc"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-5ad4418e63"></a>`additionalProperties`: `false`
- <a id="s-76e3db406c"></a>`required`: `["format","archive_generation","storage_profile","tree","volume_sequence"]`
- <a id="s-59b7295b1c"></a>`title`: `"Riverhog v1 immutable collection archive root"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-191b5cabb5"></a>`archive_generation` | yes | [sha256](#s-e2bfff1bf6) |  |
| <a id="s-aa139f3599"></a>`format` | yes | const="collection-archive-manifest/v1" |  |
| <a id="s-c57132ccc2"></a>`provenance` | no | [provenance](#s-7606eb291c) |  |
| `storage_profile` | yes | [See field `storage_profile`](#s-c05c8a5832) |  |
| <a id="s-d428c80832"></a>`tree` | yes | [tree](#s-9c81514a27) |  |
| `volume_sequence` | yes | [See field `volume_sequence`](#s-86c4eff453) |  |

### Definitions

- [nonnegative_count](#s-8d2ec15c3e)
- [positive_count](#s-d9f1da74ca)
- [provenance](#s-7606eb291c)
- [provenance_root](#s-d91794edfc)
- [sha256](#s-e2bfff1bf6)
- [tree](#s-9c81514a27)

### <a id="s-c05c8a5832"></a>field `storage_profile`

- <a id="s-6b11fe085d"></a>`type`: `"object"`
- <a id="s-d893c23142"></a>`additionalProperties`: `false`
- <a id="s-f6e3a5cc08"></a>`required`: `["encryption","pack_index","part_digest","selective_read"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36aa1e1d78"></a>`encryption` | yes | const="age-v1-scrypt" |  |
| <a id="s-972143cd89"></a>`pack_index` | yes | const="riverhog-pack-index/v1" |  |
| <a id="s-9954d0b8f6"></a>`part_digest` | yes | const="sha256" |  |
| <a id="s-575d46f7d5"></a>`selective_read` | yes | const="age-chunk-range/v1" |  |

### <a id="s-86c4eff453"></a>field `volume_sequence`

- <a id="s-172183f828"></a>`type`: `"object"`
- <a id="s-540e1c268b"></a>`additionalProperties`: `false`
- <a id="s-26232ae7d6"></a>`required`: `["sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-01983c97a3"></a>`sha256` | yes | [sha256](#s-e2bfff1bf6) |  |

### <a id="s-8d2ec15c3e"></a>definition `nonnegative_count`

- <a id="s-6ea7ae8039"></a>`type`: `"string"`
- <a id="s-c7de713964"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-d9f1da74ca"></a>definition `positive_count`

- <a id="s-2c7c77b10b"></a>`type`: `"string"`
- <a id="s-892f71da82"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### <a id="s-7606eb291c"></a>definition `provenance`

- <a id="s-7b216361af"></a>`type`: `"object"`
- <a id="s-2184f9e653"></a>`additionalProperties`: `false`
- <a id="s-9e693ca269"></a>`required`: `["identity","root"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38c378fa9b"></a>`identity` | yes | [sha256](#s-e2bfff1bf6) |  |
| <a id="s-a9637dfd0a"></a>`root` | yes | [provenance_root](#s-d91794edfc) |  |

### <a id="s-d91794edfc"></a>definition `provenance_root`

- <a id="s-734f0d6bbc"></a>`type`: `"object"`
- <a id="s-84ea1e26d7"></a>`additionalProperties`: `false`
- <a id="s-4296c52561"></a>`required`: `["id","kind","path","plaintext_bytes","sha256","stored_bytes","stored_sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e27fe3e8f6"></a>`id` | yes | const="provenance-root" |  |
| <a id="s-2e4cc4be97"></a>`kind` | yes | const="provenance-root" |  |
| <a id="s-d55eebfc87"></a>`path` | yes | const="provenance/root.json.age" |  |
| <a id="s-034a2d82de"></a>`plaintext_bytes` | yes | [positive_count](#s-d9f1da74ca) |  |
| <a id="s-4ae17b1c80"></a>`sha256` | yes | [sha256](#s-e2bfff1bf6) |  |
| <a id="s-cf7e974cb4"></a>`stored_bytes` | yes | [positive_count](#s-d9f1da74ca) |  |
| <a id="s-12cdea5673"></a>`stored_sha256` | yes | [sha256](#s-e2bfff1bf6) |  |

### <a id="s-e2bfff1bf6"></a>definition `sha256`

- <a id="s-b3e08853d8"></a>`type`: `"string"`
- <a id="s-a10c33b9d8"></a>`pattern`: `"^[0-9a-f]{64}$"`

### <a id="s-9c81514a27"></a>definition `tree`

- <a id="s-0968586cb0"></a>`type`: `"object"`
- <a id="s-e38e900039"></a>`additionalProperties`: `false`
- <a id="s-1fc82a71b9"></a>`required`: `["files","bytes","sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8c328ddc3"></a>`bytes` | yes | [nonnegative_count](#s-8d2ec15c3e) |  |
| <a id="s-77bf49c805"></a>`files` | yes | [positive_count](#s-d9f1da74ca) |  |
| <a id="s-b87d90fcbb"></a>`sha256` | yes | [sha256](#s-e2bfff1bf6) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-e2bfff1bf6) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-58615ad0c1"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-545116de4c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../../../evidence/sources/authorities.md#src-fc0a3e3cab) — [packages/riverhog-archive-contracts/schemas/collection-archive-manifest-v1.schema.json](../../../../../../packages/riverhog-archive-contracts/schemas/collection-archive-manifest-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-manifest-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f3b23911cd74f4710819572c60631386201fe7c28abc72fa90714011ec89e95 -->

```json
{
  "$comment": "This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveManifest is the canonical semantic, identity, and canonical-JSON authority.",
  "$defs": {
    "nonnegative_count": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    },
    "positive_count": {
      "pattern": "^[1-9][0-9]*(?![\\s\\S])",
      "type": "string"
    },
    "provenance": {
      "additionalProperties": false,
      "properties": {
        "identity": {
          "$ref": "#/$defs/sha256"
        },
        "root": {
          "$ref": "#/$defs/provenance_root"
        }
      },
      "required": [
        "identity",
        "root"
      ],
      "type": "object"
    },
    "provenance_root": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "const": "provenance-root"
        },
        "kind": {
          "const": "provenance-root"
        },
        "path": {
          "const": "provenance/root.json.age"
        },
        "plaintext_bytes": {
          "$ref": "#/$defs/positive_count"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        },
        "stored_bytes": {
          "$ref": "#/$defs/positive_count"
        },
        "stored_sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "id",
        "kind",
        "path",
        "plaintext_bytes",
        "sha256",
        "stored_bytes",
        "stored_sha256"
      ],
      "type": "object"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "tree": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "$ref": "#/$defs/nonnegative_count"
        },
        "files": {
          "$ref": "#/$defs/positive_count"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "files",
        "bytes",
        "sha256"
      ],
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "format": {
      "const": "collection-archive-manifest/v1"
    },
    "provenance": {
      "$ref": "#/$defs/provenance"
    },
    "storage_profile": {
      "additionalProperties": false,
      "properties": {
        "encryption": {
          "const": "age-v1-scrypt"
        },
        "pack_index": {
          "const": "riverhog-pack-index/v1"
        },
        "part_digest": {
          "const": "sha256"
        },
        "selective_read": {
          "const": "age-chunk-range/v1"
        }
      },
      "required": [
        "encryption",
        "pack_index",
        "part_digest",
        "selective_read"
      ],
      "type": "object"
    },
    "tree": {
      "$ref": "#/$defs/tree"
    },
    "volume_sequence": {
      "additionalProperties": false,
      "properties": {
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "sha256"
      ],
      "type": "object"
    }
  },
  "required": [
    "format",
    "archive_generation",
    "storage_profile",
    "tree",
    "volume_sequence"
  ],
  "title": "Riverhog v1 immutable collection archive root",
  "type": "object"
}
```

</details>
