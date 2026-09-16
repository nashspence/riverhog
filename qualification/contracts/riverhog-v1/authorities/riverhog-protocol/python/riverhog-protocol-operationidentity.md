# riverhog_protocol.OperationIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-operationidentity:e18650c54f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-294f208ff3"></a>
- <a id="s-734719f001"></a>`distribution`: `riverhog-protocol`
- <a id="s-6afd9e340d"></a>`module`: `riverhog_protocol`
- <a id="s-a0647494df"></a>`name`: `OperationIdentity`
- <a id="s-67e6ed20da"></a>`unit`: `export`

### Declared structure

- <a id="s-b86a7c63a0"></a>`kind`: `"class"`
- <a id="s-8d619705ba"></a>`signature`: `"\"(id: 'str', sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-8a393812ce"></a>`id` | `'str'` | `required` |
| <a id="s-3d2d35cb8f"></a>`sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-protocol-operationidentity-as-dict.md)
- [from_mapping](riverhog-protocol-operationidentity-from-mapping.md)

## Governing policies

- <a id="pa-2fd02d453f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.OperationIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 666f935d6f42955d4225d9163174cbe5263ac428dfdfd316a6ce8488b74dcca9 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(id: 'str', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "OperationIdentity",
  "unit": "export"
}
```

</details>
