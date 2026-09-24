# riverhog_application_access.APPLICATION_PERMISSIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-application-permissions:004992de49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2783287f06"></a>
- <a id="s-4f9a991362"></a>`distribution`: `riverhog-application-access`
- <a id="s-5a0c73e726"></a>`module`: `riverhog_application_access`
- <a id="s-211a002942"></a>`name`: `APPLICATION_PERMISSIONS`
- <a id="s-2a07b36096"></a>`unit`: `export`

### Declared structure

- <a id="s-0206a147c3"></a>`kind`: `"constant"`
- <a id="s-d40c2a6c34"></a>`value`: `["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-processing:control","collection-processing:execute","collection-tags:manage","collections:create","collections:delete","events:read","events:read_all","keys:manage","provenance:export","provenance:read","quotas:manage","retrieval:manage"]`

## Governing policies

- <a id="pa-f4024a05fa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.APPLICATION_PERMISSIONS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 495e60bf53323268f909575b459d5d03698eec7087487c438792b43e9895f0a9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "archives:manage",
      "archives:read",
      "catalog:read",
      "collection-descriptions:manage",
      "collection-processing:control",
      "collection-processing:execute",
      "collection-tags:manage",
      "collections:create",
      "collections:delete",
      "events:read",
      "events:read_all",
      "keys:manage",
      "provenance:export",
      "provenance:read",
      "quotas:manage",
      "retrieval:manage"
    ]
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "APPLICATION_PERMISSIONS",
  "unit": "export"
}
```

</details>
