# riverhog_provenance.DEFAULT_OBSERVER_AGENT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-default-observer-agent-id:c2719cfdcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-177398903b"></a>
- <a id="s-33f2fbc66d"></a>`distribution`: `riverhog-provenance`
- <a id="s-f218b2ca41"></a>`module`: `riverhog_provenance`
- <a id="s-6647cab9bc"></a>`name`: `DEFAULT_OBSERVER_AGENT_ID`
- <a id="s-46f4be152f"></a>`unit`: `export`

### Declared structure

- <a id="s-4cf5a08995"></a>`kind`: `"constant"`
- <a id="s-cbdca0dc20"></a>`value`: `"urn:uuid:43dd8300-a6bd-5f58-9cfd-4d5f8ec9421b"`

## Governing policies

- <a id="pa-c938b93708"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.DEFAULT_OBSERVER_AGENT_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f4e470c87022a5dc8373e3ea32aff964b3e2c2b02e5cab6f29596b2dfc6db88 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "urn:uuid:43dd8300-a6bd-5f58-9cfd-4d5f8ec9421b"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "DEFAULT_OBSERVER_AGENT_ID",
  "unit": "export"
}
```

</details>
