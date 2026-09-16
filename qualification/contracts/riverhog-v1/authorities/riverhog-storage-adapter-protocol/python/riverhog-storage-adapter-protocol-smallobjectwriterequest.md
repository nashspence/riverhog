# riverhog_storage_adapter_protocol.SmallObjectWriteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-smallob-bb7878c9d1:27eaf5a542 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-058b84cc00"></a>
- <a id="s-64a43edcb7"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-b21ea1961e"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-704366a094"></a>`name`: `SmallObjectWriteRequest`
- <a id="s-ac1f34235c"></a>`unit`: `export`

### Declared structure

- <a id="s-f780091ab5"></a>`kind`: `"class"`
- <a id="s-989b1f79b8"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate'], mode: Literal['create_only', 'replace_current'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-8522318b62"></a>

- <a id="s-84cb71abcd"></a>`type`: `"object"`
- <a id="s-ed1ca2fe8e"></a>`additionalProperties`: `false`
- <a id="s-4eb62601f3"></a>`required`: `["object_path","content_type","required_identity_assertions","placement","mode","stored_bytes","stored_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9465e41ab7"></a>`content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-64b62e7b27"></a>`expected_current_stored_sha256` | no | anyOf=(type="string"; pattern="^[0-9a-f]{64}$") \| (type="null"); default=null |  |
| <a id="s-79af9aeeff"></a>`mode` | yes | type="string"; enum=["create_only","replace_current"] |  |
| <a id="s-a8eadba612"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-b9984bcb75"></a>`placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-cb413a6c39"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |
| <a id="s-0152b13085"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-75cc37e215"></a>`stored_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_replacement_fence](riverhog-storage-adapter-protocol-smallobjectwriterequest-validate-replacement-fence.md)
- [canonical_metadata](riverhog-storage-adapter-protocol-smallobjectwriterequest-canonical-metadata.md)
- [canonical_path](riverhog-storage-adapter-protocol-smallobjectwriterequest-canonical-path.md)

## Governing policies

- <a id="pa-c39f4266c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.SmallObjectWriteRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d15fb5abd3599b5035478bd3016e4e8281467e33cbd763c5513ed2bf13eca0c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "content_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "expected_current_stored_sha256": {
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
        "mode": {
          "enum": [
            "create_only",
            "replace_current"
          ],
          "type": "string"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "placement": {
          "enum": [
            "archive",
            "immediate"
          ],
          "type": "string"
        },
        "required_identity_assertions": {
          "additionalProperties": {
            "type": "string"
          },
          "maxProperties": 64,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 16384,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-object-identity-assertion-envelope"
          }
        },
        "stored_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "stored_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "object_path",
        "content_type",
        "required_identity_assertions",
        "placement",
        "mode",
        "stored_bytes",
        "stored_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate'], mode: Literal['create_only', 'replace_current'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "SmallObjectWriteRequest",
  "unit": "export"
}
```

</details>
