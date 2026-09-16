# stove0_core.RecipeDefinition.identity_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition-identity-document:cee5506a21 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7cc2fd5b30"></a>
- <a id="s-1c66359161"></a>`distribution`: `stove0-server`
- <a id="s-d72aad15d5"></a>`module`: `stove0_core`
- <a id="s-58d4b31dbf"></a>`name`: `identity_document`
- <a id="s-ed9cfb785a"></a>`owner`: `stove0_core.RecipeDefinition`
- <a id="s-b7e6c53df0"></a>`unit`: `member`

### Declared structure

- <a id="s-5217620aa6"></a>`kind`: `"method"`
- <a id="s-079ad42d2f"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-core-recipedefinition.md)

## Governing policies

- <a id="pa-40b977fd97"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition.identity_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2433e477a25d1b4d93bed9bd68c7b897f5a307906d8603b4ab86845fd2b6487 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "identity_document",
  "owner": "stove0_core.RecipeDefinition",
  "unit": "member"
}
```

</details>
