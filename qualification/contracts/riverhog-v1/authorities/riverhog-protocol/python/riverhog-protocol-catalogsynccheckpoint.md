# riverhog_protocol.CatalogSyncCheckpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsynccheckpoint:aa221b8bb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b897c77a22"></a>
- <a id="s-5a6f81591e"></a>`distribution`: `riverhog-protocol`
- <a id="s-e38dfd13d2"></a>`module`: `riverhog_protocol`
- <a id="s-f303c7898e"></a>`name`: `CatalogSyncCheckpoint`
- <a id="s-e958f819f3"></a>`unit`: `export`

### Declared structure

- <a id="s-d6b8c82e54"></a>`kind`: `"class"`
- <a id="s-9dada88db2"></a>`signature`: `"\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], catalog_cursor: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-013e1b52c8"></a>

- <a id="s-64033b90ee"></a>`type`: `"object"`
- <a id="s-c72c1e934d"></a>`additionalProperties`: `false`
- <a id="s-b0d7ce36a1"></a>`required`: `["source_identity","authorization_view_identity","catalog_cursor"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cdc54045ed"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6ec2def91"></a>`catalog_cursor` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-619001cf1f"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1"; default="riverhog-catalog-sync/v1" |  |
| <a id="s-5a3c7515cb"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-beddb74af8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncCheckpoint`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c325bb30d3dd96cb9b34c2de05f871600e0c91478783083a642a852e92055412 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "authorization_view_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "catalog_cursor": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "format": {
          "const": "riverhog-catalog-sync/v1",
          "default": "riverhog-catalog-sync/v1",
          "type": "string"
        },
        "source_identity": {
          "maxLength": 64,
          "minLength": 64,
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "source_identity",
        "authorization_view_identity",
        "catalog_cursor"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], catalog_cursor: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncCheckpoint",
  "unit": "export"
}
```

</details>
