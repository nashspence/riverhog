# riverhog_client.transform

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform:c62128b3c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-client` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `distribution` | "riverhog-client" |
| `exports` | additional keys=`CancellationCheck`, `CapabilityApiClient`, `ClaimedArtifact`, `ClaimedCollectionApi`, `ClaimedCollectionReader`, `ClaimedCollectionRuntime`, `ClaimedCollectionRuntimeRegistry`, `ClaimedRetrieval`, `CollectionTransformRuntime`, `DerivedCollectionReceipt`, `DerivedCollectionSpec`, `DerivedCollectionWriter`, `Heartbeat`, `IncrementalDerivedCollectionWriter`, `TransformWorkspace`, `WorkspaceAssurance` |
| `module` | "riverhog_client.transform" |

## Governing policies

- `compatibility/python-api/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-client:riverhog_client.transform` — `packages/riverhog-client/src/riverhog_client/transform/__init__.py::<module>`

### Machine authority

- `/external_contract/python/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f0dff8be09ab7302b2f39d0598220ac3c7e773b633536b158b79dffc127b985 -->

```json
{
  "distribution": "riverhog-client",
  "exports": {
    "CancellationCheck": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "CapabilityApiClient": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "current": {
          "kind": "property",
          "signature": "(self) -> 'Any'"
        },
        "replace": {
          "kind": "method",
          "signature": "(self, client: 'Any', *, owns_client: 'bool' = True) -> 'None'"
        },
        "spawn": {
          "kind": "method",
          "signature": "(self) -> 'CapabilityApiClient'"
        }
      },
      "signature": "(client: 'Any', *, owns_client: 'bool' = False, _state: '_CapabilityClientState | None' = None, _root: 'bool' = True) -> 'None'"
    },
    "ClaimedArtifact": {
      "fields": [
        {
          "default": "required",
          "name": "root",
          "type": "'CollectionRootIdentity'"
        },
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        },
        {
          "default": "False",
          "name": "control",
          "type": "'bool'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        },
        "key": {
          "kind": "property",
          "signature": "(self) -> 'tuple[int, str]'"
        }
      },
      "signature": "(root: 'CollectionRootIdentity', path: 'str', bytes: 'int', sha256: 'str', control: 'bool' = False) -> None"
    },
    "ClaimedCollectionApi": {
      "kind": "class",
      "members": {
        "acknowledge_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'dict[str, Any]'"
        },
        "cancel_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'dict[str, Any]'"
        },
        "create_retrieval_job": {
          "kind": "method",
          "signature": "(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'"
        },
        "download_retrieval_file": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> 'int'"
        },
        "get_collection": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId') -> 'dict[str, Any]'"
        },
        "get_portable_collection_inventory": {
          "kind": "method",
          "signature": "(self, collection_id: 'CollectionId', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'"
        },
        "get_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'dict[str, Any]'"
        },
        "list_retrieval_plan_files": {
          "kind": "method",
          "signature": "(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'"
        },
        "plan_retrieval": {
          "kind": "method",
          "signature": "(self, files: 'Sequence[tuple[CollectionId, str]]', *, lease_seconds: 'int | None' = None, restore_policy: 'RiverhogRestorePolicy' = 'never') -> 'dict[str, Any]'"
        },
        "renew_retrieval_job": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, lease_seconds: 'int') -> 'dict[str, Any]'"
        },
        "stream_retrieval_file": {
          "kind": "method",
          "signature": "(self, job_id: 'str', *, collection_id: 'CollectionId', path: 'str', expected_bytes: 'int', expected_sha256: 'str', start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'AbstractContextManager[Iterator[bytes]]'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "ClaimedCollectionReader": {
      "kind": "class",
      "members": {
        "close_retrievals": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "iter_inventory": {
          "kind": "method",
          "signature": "(self, *, include_control: 'bool' = False) -> 'Iterator[ClaimedArtifact]'"
        },
        "prepare": {
          "kind": "method",
          "signature": "(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, *, lease_seconds: 'int' = 1800, restore_policy: 'RetrievalPolicy' = 'available-only', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ClaimedRetrieval'"
        },
        "replace_api": {
          "kind": "method",
          "signature": "(self, api: 'ClaimedCollectionApi') -> 'None'"
        }
      },
      "signature": "(api: 'ClaimedCollectionApi', *, inputs: 'Sequence[CollectionRootIdentity]', work_id: 'str', claim_id: 'str', fence: 'int', heartbeat: 'Heartbeat | None' = None) -> 'None'"
    },
    "ClaimedCollectionRuntime": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "from_capability": {
          "kind": "classmethod",
          "signature": "(cls, *, base_url: 'str', capability_token: 'str', inputs: 'Sequence[CollectionRootIdentity]', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'ClaimedCollectionRuntime'"
        },
        "heartbeat": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "iter_inventory": {
          "kind": "method",
          "signature": "(self)"
        },
        "open_workspace": {
          "kind": "method",
          "signature": "(self, root: 'Path', *, assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'"
        },
        "prepare_inputs": {
          "kind": "method",
          "signature": "(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'"
        },
        "refresh_capability": {
          "kind": "method",
          "signature": "(self, capability_token: 'str') -> 'None'"
        }
      },
      "signature": "(api: 'Any', *, inputs: 'Sequence[CollectionRootIdentity]', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', cancellation_check: 'CancellationCheck | None' = None, input_retrieval_policy: \"Literal['available-only', 'allow']\" = 'available-only', owned_api: 'bool' = False) -> 'None'"
    },
    "ClaimedCollectionRuntimeRegistry": {
      "kind": "class",
      "members": {
        "bind": {
          "kind": "method",
          "signature": "(self, job_id: 'str', runtime: 'RefreshableClaimedCollectionRuntime') -> 'Iterator[RefreshableClaimedCollectionRuntime]'"
        },
        "discard": {
          "kind": "method",
          "signature": "(self, job_id: 'str') -> 'None'"
        },
        "refresh": {
          "kind": "method",
          "signature": "(self, job_id: 'str', capability_token: 'str') -> 'None'"
        }
      },
      "signature": "() -> 'None'"
    },
    "ClaimedRetrieval": {
      "kind": "class",
      "members": {
        "cleanup_pending": {
          "kind": "property",
          "signature": "(self) -> 'bool'"
        },
        "close": {
          "kind": "method",
          "signature": "(self, *, success: 'bool' = True) -> 'None'"
        },
        "closed": {
          "kind": "property",
          "signature": "(self) -> 'bool'"
        },
        "download": {
          "kind": "method",
          "signature": "(self, artifact: 'ClaimedArtifact', output: 'Path') -> 'int'"
        },
        "read_bytes": {
          "kind": "method",
          "signature": "(self, artifact: 'ClaimedArtifact', *, maximum_bytes: 'int') -> 'bytes'"
        },
        "renew": {
          "kind": "method",
          "signature": "(self, *, lease_seconds: 'int') -> 'dict[str, Any]'"
        },
        "replace_api": {
          "kind": "method",
          "signature": "(self, api: 'ClaimedCollectionApi') -> 'None'"
        },
        "retry_close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "stream": {
          "kind": "method",
          "signature": "(self, artifact: 'ClaimedArtifact', *, start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'"
        }
      },
      "signature": "(api: 'ClaimedCollectionApi', *, job: 'Mapping[str, Any]', artifacts: 'Sequence[ClaimedArtifact]', heartbeat: 'Heartbeat | None' = None) -> 'None'"
    },
    "CollectionTransformRuntime": {
      "kind": "class",
      "members": {
        "append_incremental_output": {
          "kind": "method",
          "signature": "(self, writer: 'IncrementalDerivedCollectionWriter', source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'"
        },
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "finish_incremental_publication": {
          "kind": "method",
          "signature": "(self, writer: 'IncrementalDerivedCollectionWriter', *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', **kwargs: 'Any') -> 'DerivedCollectionReceipt'"
        },
        "from_capability": {
          "kind": "classmethod",
          "signature": "(cls, *, base_url: 'str', capability_token: 'str', spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', allow_insecure_http: 'bool' = False, **kwargs: 'Any') -> 'CollectionTransformRuntime'"
        },
        "heartbeat": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "iter_inventory": {
          "kind": "method",
          "signature": "(self)"
        },
        "open_incremental_publication": {
          "kind": "method",
          "signature": "(self, *, execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] | None' = None) -> 'IncrementalDerivedCollectionWriter'"
        },
        "open_workspace": {
          "kind": "method",
          "signature": "(self, root: 'Path', *, assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'"
        },
        "prepare_inputs": {
          "kind": "method",
          "signature": "(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'"
        },
        "publish": {
          "kind": "method",
          "signature": "(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] | None' = None, **kwargs: 'Any') -> 'DerivedCollectionReceipt'"
        },
        "refresh_capability": {
          "kind": "method",
          "signature": "(self, capability_token: 'str') -> 'None'"
        }
      },
      "signature": "(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str' = 'development', cancellation_check: 'CancellationCheck | None' = None, input_retrieval_policy: \"Literal['available-only', 'allow']\" = 'available-only', owned_api: 'bool' = False) -> 'None'"
    },
    "DerivedCollectionReceipt": {
      "fields": [
        {
          "default": "required",
          "name": "collection_id",
          "type": "'CollectionId'"
        },
        {
          "default": "required",
          "name": "archive_root_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "content_identity",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "derivation",
          "type": "'CollectionDerivation'"
        }
      ],
      "kind": "class",
      "members": {
        "as_dict": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        },
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'Mapping[str, object]') -> 'DerivedCollectionReceipt'"
        }
      },
      "signature": "(collection_id: 'CollectionId', archive_root_sha256: 'str', content_identity: 'str', derivation: 'CollectionDerivation') -> None"
    },
    "DerivedCollectionSpec": {
      "fields": [
        {
          "default": "required",
          "name": "inputs",
          "type": "'tuple[CollectionRootIdentity, ...]'"
        },
        {
          "default": "required",
          "name": "recipe",
          "type": "'RecipeIdentity'"
        },
        {
          "default": "required",
          "name": "operation",
          "type": "'OperationIdentity'"
        }
      ],
      "kind": "class",
      "signature": "(inputs: 'tuple[CollectionRootIdentity, ...]', recipe: 'RecipeIdentity', operation: 'OperationIdentity') -> None"
    },
    "DerivedCollectionWriter": {
      "kind": "class",
      "members": {
        "publish": {
          "kind": "method",
          "signature": "(self, outputs: 'Sequence[ProducerInput]', *, execution_envelope_sha256: 'str', execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', source_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'"
        },
        "replace_api": {
          "kind": "method",
          "signature": "(self, api: 'Any') -> 'None'"
        }
      },
      "signature": "(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str' = 'development') -> 'None'"
    },
    "Heartbeat": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "IncrementalDerivedCollectionWriter": {
      "kind": "class",
      "members": {
        "append": {
          "kind": "method",
          "signature": "(self, source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'"
        },
        "finish": {
          "kind": "method",
          "signature": "(self, *, execution_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'DerivedCollectionReceipt'"
        },
        "heartbeat": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "stop": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        }
      },
      "signature": "(api: 'Any', *, spec: 'DerivedCollectionSpec', claim_id: 'str', fence: 'int', work_id: 'str', execution_id: 'str', controller_evidence: 'Mapping[str, object]', producer_app: 'str', producer_version: 'str', execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] | None' = None) -> 'None'"
    },
    "TransformWorkspace": {
      "fields": [
        {
          "default": "required",
          "name": "root",
          "type": "'Path'"
        },
        {
          "default": "required",
          "name": "execution_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "assurance",
          "type": "'WorkspaceAssurance'"
        }
      ],
      "kind": "class",
      "members": {
        "open": {
          "kind": "classmethod",
          "signature": "(cls, root: 'Path', *, execution_id: 'str', assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'"
        },
        "release": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "resolve": {
          "kind": "method",
          "signature": "(self, relative_path: 'str') -> 'Path'"
        }
      },
      "signature": "(root: 'Path', execution_id: 'str', assurance: 'WorkspaceAssurance') -> None"
    },
    "WorkspaceAssurance": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    }
  },
  "module": "riverhog_client.transform"
}
```
