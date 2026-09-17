# riverhog_application_access.COLLECTION_TRANSFORMS_CONTROL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-tr-0e035b2fa5:d12da4bc3b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b884903a0"></a>
- <a id="s-4fd25738b9"></a>`distribution`: `riverhog-application-access`
- <a id="s-db01486068"></a>`module`: `riverhog_application_access`
- <a id="s-d841284cb8"></a>`name`: `COLLECTION_TRANSFORMS_CONTROL`
- <a id="s-eaa0eb1c3c"></a>`unit`: `export`

### Declared structure

- <a id="s-581821995f"></a>`kind`: `"constant"`
- <a id="s-88def83cc7"></a>`value`: `"collection-transforms:control"`

## Governing policies

- <a id="pa-af0d2bc616"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTION_TRANSFORMS_CONTROL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0257214411b4e1e315fa7b4221c2b68ecb9c2de6d42e17628103c68a4262896f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-transforms:control"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTION_TRANSFORMS_CONTROL",
  "unit": "export"
}
```

</details>
