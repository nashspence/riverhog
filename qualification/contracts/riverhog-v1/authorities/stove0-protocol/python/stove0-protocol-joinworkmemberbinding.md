# stove0_protocol.JoinWorkMemberBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinworkmemberbinding:f2f4b9c73b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b3c9dba8f8"></a>
- <a id="s-dab55dfc97"></a>`distribution`: `stove0-protocol`
- <a id="s-da35b8f414"></a>`module`: `stove0_protocol`
- <a id="s-2392f4a239"></a>`name`: `JoinWorkMemberBinding`
- <a id="s-1884ee9af7"></a>`unit`: `export`

### Declared structure

- <a id="s-35c931e3a9"></a>`kind`: `"class"`
- <a id="s-a31d624dc0"></a>`signature`: `"\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, artifact_selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-67495a11aa"></a>

- <a id="s-e860510ec8"></a>`type`: `"object"`
- <a id="s-197f5dd9e2"></a>`additionalProperties`: `false`
- <a id="s-76c8cdb261"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cad6409d2f"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-78f0cfdd96"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b1a1d64d3b"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-2aca03ce44"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-2b67ee6758"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinWorkMemberBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e2af7a5362ca2441351139a5420f584a76eb917d61b163fbc29c0df56e7e08b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "artifact_selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "producer_settlement_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "settlement_sha256",
        "artifact_selection_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, artifact_selection_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinWorkMemberBinding",
  "unit": "export"
}
```

</details>
