# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-base-url:a52c86db5b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-00fcde6edd) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b7a4f6f148"></a>
| Field | Shape |
|---|---|
| <a id="s-83399b165d"></a>`classification` | "identity" |
| <a id="s-7590734788"></a>`consumers` | ["riverhog-client"] |
| <a id="s-90e44cb41b"></a>`disposition` | "contractual" |
| <a id="s-a600f51a3c"></a>`id` | "riverhog-client:environment:RIVERHOG_BASE_URL" |
| <a id="s-727a81b3ac"></a>`name` | "RIVERHOG_BASE_URL" |
| <a id="s-10a1945249"></a>`owner` | "riverhog-client" |

## Governing policies

- <a id="pa-5fca68700b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_BASE_URL](../../../evidence/sources.md#src-475671fe74) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/4/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `os.getenv('RIVERHOG_BASE_URL')` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `safe_http_base_url(base_url or os.getenv('RIVERHOG_BASE_URL') or 'http://127.0.0.1:8000', setting='RIVERHOG_BASE_URL', allow_insecure_http=self.allow_insecure_http)` |

### Machine authority

- `/external_contract/configuration_environment/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e7d9fbb8711ab74e113c6ec0b7f7caba018648f85f8507e04283c6cf7807d57 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_BASE_URL",
  "name": "RIVERHOG_BASE_URL",
  "owner": "riverhog-client"
}
```
