# riverhog_age.AEAD_TAG_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-aead-tag-size:e1085ae7b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7522e520fb"></a>
- <a id="s-917075b4b5"></a>`distribution`: `riverhog-age`
- <a id="s-affce83449"></a>`module`: `riverhog_age`
- <a id="s-e1ebf7e88d"></a>`name`: `AEAD_TAG_SIZE`
- <a id="s-8affec0908"></a>`unit`: `export`

### Declared structure

- <a id="s-687a2f9f21"></a>`kind`: `"constant"`
- <a id="s-af27c4db1f"></a>`value`: `16`

## Governing policies

- <a id="pa-814c45fbce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.AEAD_TAG_SIZE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98303f6a2fbbf46b38c1dd1578c53bff76cfc5870579c7b4dcee05c23ae8e2b2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 16
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "AEAD_TAG_SIZE",
  "unit": "export"
}
```

</details>
