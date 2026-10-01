"""Target-facing Riverhog data-plane runtime for the stove0 protocol."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Self, cast

from a_riverhog_direct_relations_contract_lib import VOCABULARY_SHA256
from pydantic import JsonValue
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.completion_records import CompletionRecords
from riverhog_client.processing import (
    ClaimedArtifact,
    ClaimedCollectionRuntime,
    ClaimedRetrieval,
    CollectionTransformRuntime,
    DerivedCollectionSpec,
    IncrementalDerivedCollectionWriter,
    ProcessingWorkspace,
)
from riverhog_client.producer import (
    ProducerArtifactCustody,
    ProducerArtifactIdentity,
    ProducerFile,
    ProducerInput,
)
from riverhog_protocol import ArtifactId
from riverhog_protocol.collection_record_preimages import canonical_record_sequence
from riverhog_protocol.collection_workflows import (
    OperationIdentity,
    RecipeIdentity,
)
from stove0_target_client import TargetCallbackClient
from stove0_target_protocol import (
    EFFECT_TARGET_PROTOCOL,
    ExternalEffectReceipt,
    ExternalEffectReceiptPayload,
    InputArtifact,
    InputDisposition,
    InputDispositionDeclaration,
    OperationContract,
    OutputArtifact,
    OutputCollectionRef,
    OutputSourceEdge,
    TargetDescriptor,
    TargetExecutionEvidence,
    TargetJobRequest,
    TargetJobStatus,
    TargetPreRootResult,
    TargetProgress,
    validate_status_against_request,
)

from stove0_target_support.execution import TargetExecutionSession
from stove0_target_support.output_checkpoint import TargetOutputCheckpoint
from stove0_target_support.relationships import completion_output_relationships

CancellationCheck = Callable[[], None]


class TargetCollectionPublication:
    """Target-protocol view of one generic Riverhog incremental construction."""

    def __init__(
        self,
        execution: TargetExecutionRuntime,
        writer: IncrementalDerivedCollectionWriter,
        implementation: TargetDescriptor,
        *,
        source_context: Mapping[str, Any] | None = None,
    ) -> None:
        self.implementation = implementation
        self.source_context = dict(source_context or {})
        self.execution = execution
        self.writer = writer
        self._local_files: dict[str, Path] = {}
        execution._publications.append(self)
        session = execution.session
        self.has_restart_state = bool(
            session is not None
            and session.state_root is not None
            and TargetOutputCheckpoint.has_records(session.state_root, execution.request)
        )
        if session is not None and session.state_root is not None:
            writer.producer.set_pending_source_resolver(self._pending_source)

    def _pending_source(self, artifact_id: ArtifactId) -> ProducerFile:
        session = self.execution.session
        if session is None or session.state_root is None:
            raise ValueError("pending output has no target-owned restart state")
        checkpoint = TargetOutputCheckpoint.load_member(
            session.state_root, request=self.execution.request, artifact_id=artifact_id
        )
        if checkpoint is None:
            raise ValueError("pending output has no exact target-owned checkpoint")
        workspace = next(
            (
                value
                for value in self.execution._workspaces
                if str(value.root) == checkpoint.workspace_root
            ),
            None,
        )
        if workspace is None:
            raise ValueError("pending output checkpoint lacks its protected workspace")
        source = checkpoint.producer_file(workspace)
        self._local_files[artifact_id] = source.source
        return source

    @staticmethod
    def _source_edges_identity(selected: Iterable[str]) -> str:
        identifiers = tuple(sorted(selected))
        if not identifiers or len(identifiers) != len(set(identifiers)):
            raise ValueError("target output sources must be nonempty and unique")
        return hashlib.sha256(canonical_json_bytes(list(identifiers))).hexdigest()

    def prepare_output(
        self,
        source: ProducerInput,
        artifact: OutputArtifact,
        *,
        derived_from: Iterable[str],
    ) -> None:
        """Retain restart metadata before any of the prepared files can be released."""
        if source.artifact_id != artifact.artifact_id:
            raise ValueError(f"target output artifact ID does not match its source: {artifact.id}")
        if not isinstance(source, ProducerFile):
            return
        session = self.execution.session
        if session is not None and session.state_root is not None:
            workspace = next(
                (
                    value
                    for value in self.execution._workspaces
                    if source.source.is_relative_to(value.root)
                ),
                None,
            )
            if workspace is None:
                raise ValueError("persistent target outputs require their protected workspace")
            TargetOutputCheckpoint.retain(
                session.state_root,
                request=self.execution.request,
                output=artifact,
                source_edges_sha256=self._source_edges_identity(derived_from),
                source=source,
                workspace=workspace,
            )
            self.has_restart_state = True
        self._local_files[artifact.artifact_id] = source.source

    def resume_output(
        self,
        output_id: str,
        *,
        derived_from: Iterable[str],
        materialization_hint: tuple[str, ...] | None,
        allow_missing_materialization_hint: bool,
    ) -> OutputArtifact | None:
        """Resume only the exact recorded output and its currently authorized custody."""
        session = self.execution.session
        if session is None or session.state_root is None:
            return None
        checkpoint = TargetOutputCheckpoint.load(
            session.state_root, request=self.execution.request, output_id=output_id
        )
        if checkpoint is None:
            return None
        selected = tuple(derived_from)
        if self._source_edges_identity(selected) != checkpoint.source_edges_sha256:
            raise ValueError("output checkpoint changed the accepted source edges")
        if (
            checkpoint.materialization_hint != materialization_hint
            or type(allow_missing_materialization_hint) is not bool
            or checkpoint.allow_missing_materialization_hint != allow_missing_materialization_hint
        ):
            raise ValueError("output checkpoint differs from the accepted publication decision")
        artifact = checkpoint.output
        self._declare_output(artifact, selected)
        identity = ProducerArtifactIdentity(
            artifact.artifact_id, int(artifact.bytes), artifact.sha256
        )
        custody = self.writer.producer.resume_artifact_custody(identity)
        if custody is not None:
            return artifact
        workspace = next(
            (
                value
                for value in self.execution._workspaces
                if str(value.root) == checkpoint.workspace_root
            ),
            None,
        )
        if workspace is None:
            raise ValueError("pending output checkpoint lacks its protected workspace")
        self.append(checkpoint.producer_file(workspace), artifact, derived_from=selected)
        return artifact

    def has_output_checkpoint(self, output_id: str) -> bool:
        session = self.execution.session
        return bool(
            session is not None
            and session.state_root is not None
            and TargetOutputCheckpoint.load(
                session.state_root, request=self.execution.request, output_id=output_id
            )
            is not None
        )

    def _declare_output(self, artifact: OutputArtifact, selected: Sequence[str]) -> None:
        self.execution._input_client.declare_target_execution_output(
            self.execution.job_id, artifact
        )
        for input_id in selected:
            self.execution._input_client.declare_target_execution_source_edge(
                self.execution.job_id, OutputSourceEdge(output_id=artifact.id, input_id=input_id)
            )

    def append(
        self,
        source: ProducerInput,
        artifact: OutputArtifact,
        *,
        derived_from: Iterable[str],
    ) -> tuple[ProducerArtifactCustody, ...]:
        selected = tuple(derived_from)
        sources = self.execution.resolve_input_ids(selected)
        self.prepare_output(source, artifact, derived_from=selected)
        # Callback declarations precede custody so restart can finish from
        # accepted keys/edges after the target releases disposable files.
        self._declare_output(artifact, selected)
        runtime = cast(CollectionTransformRuntime, self.execution.runtime)
        receipts = runtime.append_incremental_output(
            self.writer,
            source,
            identity=ProducerArtifactIdentity(
                artifact.artifact_id, artifact.bytes, artifact.sha256
            ),
            output_id=artifact.id,
            inputs=sources,
            history_extent=BOUND_HISTORY_EXTENT,
        )
        self._release_custodied_files(receipts)
        return receipts

    def finish_success(
        self,
        *,
        operation: OperationContract,
        execution_sha256: str,
        execution_preimage: bytes | CompletionRecord,
        attempt: int = 1,
        runtime_evidence: Mapping[str, object] | None = None,
        **kwargs: Any,
    ) -> TargetJobStatus:
        sealed = self.execution._input_client.seal_target_execution_production(
            self.execution.job_id
        )
        while sealed.state == "sealing":
            time.sleep(0.1)
            sealed = self.execution._input_client.seal_target_execution_production(
                self.execution.job_id
            )
        production = sealed.production
        if production is None:
            raise RuntimeError("Stove0 sealed no target production authority")
        disposition_set = production.riverhog_disposition_set
        runtime = cast(CollectionTransformRuntime, self.execution.runtime)
        execution_record = (
            CompletionRecord.from_bytes("target-execution", execution_preimage)
            if isinstance(execution_preimage, bytes)
            else execution_preimage
        )
        if (
            execution_record.kind != "target-execution"
            or execution_record.sha256 != execution_sha256
        ):
            raise ValueError("target execution digest differs from its sealed preimage")
        plan = self.execution.request.declaration.plan
        evidence = TargetExecutionEvidence(
            target_descriptor_sha256=plan.target_descriptor_sha256,
            operation_contract_sha256=plan.operation_contract_sha256,
            plan_sha256=plan.plan_sha256,
            execution_sha256=execution_sha256,
            runtime=cast(dict[str, JsonValue], dict(runtime_evidence or {})),
        )
        pre_root = TargetPreRootResult(
            job_id=self.execution.job_id,
            attempt=attempt,
            request_sha256=self.execution.request.request_sha256,
            plan_sha256=plan.plan_sha256,
            production=production,
            execution_evidence=evidence,
        )
        if self.execution.session is not None:
            pre_root = self.execution.session.retain_completion(
                request=self.execution.request,
                implementation=self.implementation,
                operation=operation,
                pre_root=pre_root,
                execution=execution_record,
                source_context=self.source_context,
            )
        evidence = pre_root.execution_evidence
        completion_records = (
            CompletionRecord.from_bytes(
                "invocation",
                canonical_json_bytes(
                    self.execution.request.declaration.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    )
                ),
            ),
            CompletionRecord.from_bytes(
                "implementation",
                canonical_json_bytes(
                    {
                        "descriptor": self.implementation.model_dump(mode="json", by_alias=True),
                        "operation": operation.model_dump(mode="json", by_alias=True),
                        "direct_relations_vocabulary_sha256": VOCABULARY_SHA256,
                    }
                ),
            ),
            execution_record,
            CompletionRecord.from_bytes(
                "target-result",
                canonical_json_bytes(pre_root.model_dump(mode="json", by_alias=True)),
            ),
        )
        with CompletionRecords() as declarations:
            declared = declarations.add(
                "target-output-declarations",
                canonical_record_sequence(
                    product.model_dump(mode="json", exclude_none=True)
                    for product in self.execution._input_client.iter_outputs(production)
                ),
            )
            receipt = runtime.finish_incremental_publication(
                self.writer,
                execution_sha256=execution_sha256,
                disposition_set=disposition_set,
                completion_records=(*completion_records, declared),
                completion_assertions=completion_output_relationships,
                **kwargs,
            )
        self._release_all_files()
        output_collection = OutputCollectionRef.model_validate(
            {
                "collection_id": str(receipt.collection_id),
                "archive_root_sha256": receipt.archive_root_sha256,
                "artifact_set_identity": receipt.artifact_set_identity,
                "derivation_sha256": receipt.derivation.sha256,
            }
        )
        plan = self.execution.request.declaration.plan
        status = TargetJobStatus(
            protocol=plan.protocol,
            job_id=self.execution.request.declaration.job_id,
            state="succeeded",
            attempt=attempt,
            request_sha256=self.execution.request.request_sha256,
            plan_sha256=plan.plan_sha256,
            progress=TargetProgress(
                phase="done",
                completed=production.outputs.artifact_count,
                total=production.outputs.artifact_count,
                unit="artifacts",
            ),
            production=production,
            output_collection=output_collection,
            execution_evidence=evidence,
            derivation=receipt.derivation.as_dict(),
        )
        validate_status_against_request(status, self.execution.request, operation)
        self.execution._completed = True
        if self.execution.session is not None:
            self.execution.session.record_completed(status)
        return status

    def _release_custodied_files(self, receipts: Iterable[ProducerArtifactCustody]) -> None:
        for receipt in receipts:
            local = self._local_files.pop(receipt.artifact.artifact_id, None)
            if local is None:
                continue
            try:
                local.unlink()
            except FileNotFoundError:
                pass

    def _release_all_files(self) -> None:
        for local in self._local_files.values():
            try:
                local.unlink()
            except FileNotFoundError:
                pass
        self._local_files.clear()


class TargetExecutionRuntime:
    """One target job bound to a sealed stove0 execution envelope."""

    def __init__(
        self,
        request: TargetJobRequest,
        runtime: ClaimedCollectionRuntime | CollectionTransformRuntime,
        *,
        session: TargetExecutionSession | None = None,
    ) -> None:
        self.request = request
        self.runtime = runtime
        self.session = session
        self._runtime_binding: Any = None
        self._workspaces: list[ProcessingWorkspace] = []
        self._publications: list[TargetCollectionPublication] = []
        self._input_client = (
            TargetCallbackClient(request.callback_access)
            if session is None
            else session.callback_client()
        )
        self._completed = False

    @classmethod
    def from_request(
        cls,
        request: TargetJobRequest,
        *,
        cancellation_check: CancellationCheck | None = None,
        producer_version: str = "development",
        session: TargetExecutionSession | None = None,
    ) -> TargetExecutionRuntime:
        declaration = request.declaration
        evidence = declaration.controller_evidence
        workflow = evidence.execution_envelope.workflow_plan
        work = workflow.work
        authority = request.runtime
        capability_token = authority.capability_token
        if session is not None:
            # A queued job's original bearer may expire before its runtime is
            # constructed. Constructor claim checks precede registry binding,
            # so they must already use the latest in-memory refresh.
            capability_token = session.runtime_registry.capability_token(
                declaration.job_id, fallback=capability_token
            )
        runtime: ClaimedCollectionRuntime | CollectionTransformRuntime
        if workflow.result_kind == "external-effect":
            runtime = ClaimedCollectionRuntime.from_capability(
                base_url=authority.riverhog_base_url,
                capability_token=capability_token,
                allow_insecure_http=authority.allow_insecure_http,
                inputs=work.root_identities(),
                claim_id=declaration.claim_id,
                fence=declaration.fence,
                execution_id=declaration.job_id,
                work_id=work.work_id,
                cancellation_check=cancellation_check,
                input_retrieval_policy=workflow.input_retrieval_policy,
            )
        else:
            spec = DerivedCollectionSpec(
                inputs=work.root_identities(),
                recipe=RecipeIdentity(
                    work.recipe.id,
                    work.recipe.revision,
                    work.recipe.sha256,
                ),
                operation=OperationIdentity(
                    workflow.operation.id,
                    workflow.operation.sha256,
                ),
                output_policy=workflow.output_policy,
            )
            runtime = CollectionTransformRuntime.from_capability(
                base_url=authority.riverhog_base_url,
                capability_token=capability_token,
                allow_insecure_http=authority.allow_insecure_http,
                spec=spec,
                claim_id=declaration.claim_id,
                fence=declaration.fence,
                execution_id=declaration.job_id,
                work_id=work.work_id,
                controller_evidence=evidence.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
                producer_app="stove0-worker",
                producer_version=producer_version,
                cancellation_check=cancellation_check,
                input_retrieval_policy=workflow.input_retrieval_policy,
            )
        return cls(request, runtime, session=session)

    def __enter__(self) -> Self:
        self.runtime.__enter__()
        if self.session is not None:
            self._runtime_binding = self.session.runtime_registry.bind(
                self.request.declaration.job_id,
                self.runtime,
            )
            self._runtime_binding.__enter__()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        failures: list[Exception] = []
        for workspace in reversed(self._workspaces):
            if not workspace.root.exists() and not workspace.root.is_symlink():
                continue
            try:
                self.release_workspace(workspace)
            except Exception as cleanup_exc:
                failures.append(cleanup_exc)
        if self._runtime_binding is not None:
            try:
                self._runtime_binding.__exit__(exc_type, exc, tb)
            except Exception as cleanup_exc:
                failures.append(cleanup_exc)
            self._runtime_binding = None
        try:
            self.runtime.__exit__(exc_type, exc, tb)
        except Exception as cleanup_exc:
            failures.append(cleanup_exc)
        try:
            self._input_client.close()
        except Exception as cleanup_exc:
            failures.append(cleanup_exc)
        if exc is None and failures and not self._completed:
            raise RuntimeError("target execution cleanup failed before completion") from failures[0]

    def refresh_capability(self, token: str) -> None:
        self.runtime.refresh_capability(token)

    @property
    def job_id(self) -> str:
        return self.request.declaration.job_id

    @property
    def completed(self) -> bool:
        """Whether this attempt has produced its one exact successful result."""

        return self._completed

    def iter_inputs(self) -> Iterator[tuple[InputArtifact, ClaimedArtifact]]:
        for expected in self._input_client.iter_inputs(self.request.declaration.job_id):
            yield (
                expected,
                ClaimedArtifact(
                    root=expected.collection.to_identity(),
                    artifact_id=expected.artifact_id,
                    bytes=expected.bytes,
                    sha256=expected.sha256,
                ),
            )

    def resolve_input_ids(self, input_ids: Sequence[str]) -> tuple[ClaimedArtifact, ...]:
        wanted = set(input_ids)
        if not wanted or len(wanted) != len(tuple(input_ids)):
            raise ValueError("target input references must be nonempty and unique")
        resolved: dict[str, ClaimedArtifact] = {}
        for expected, claimed in self.iter_inputs():
            if expected.id in wanted:
                resolved[expected.id] = claimed
                if len(resolved) == len(wanted):
                    break
        if set(resolved) != wanted:
            missing = sorted(wanted - set(resolved))[0]
            raise ValueError(f"target output references an unknown input: {missing}")
        return tuple(resolved[item] for item in sorted(resolved))

    def declare_disposition(
        self,
        input_id: str,
        status: InputDisposition,
        *,
        code: str | None = None,
        message: str | None = None,
    ) -> None:
        self._input_client.declare_target_execution_disposition(
            self.job_id,
            InputDispositionDeclaration(
                input_id=input_id, status=status, code=code, message=message
            ),
        )

    def prepare_inputs(
        self,
        inputs: Sequence[InputArtifact] | None = None,
        **kwargs: Any,
    ) -> ClaimedRetrieval:
        if inputs is None:
            raise ValueError("target retrieval requires an explicit bounded input selection")
        artifacts = [
            ClaimedArtifact(
                root=item.collection.to_identity(),
                artifact_id=item.artifact_id,
                bytes=item.bytes,
                sha256=item.sha256,
            )
            for item in inputs
        ]
        return self.runtime.prepare_inputs(artifacts, **kwargs)

    def open_workspace(self, root: Path) -> ProcessingWorkspace:
        workspace = self.runtime.open_workspace(
            root,
            declared_protection=self.request.declaration.declared_workspace_protection,
        )
        self._workspaces.append(workspace)
        return workspace

    def release_workspace(self, workspace: ProcessingWorkspace) -> bool:
        if workspace not in self._workspaces:
            raise ValueError("workspace does not belong to this target execution")
        if not self._completed and any(
            publication.has_restart_state
            or any(
                path.is_relative_to(workspace.root) and path.exists()
                for path in publication._local_files.values()
            )
            for publication in self._publications
        ):
            # Pending bytes remain under the same declared protection and
            # restart-stable marker; only custody receipts authorize release.
            return False
        workspace.release()
        return True

    def open_collection_publication(
        self,
        *,
        implementation: TargetDescriptor,
        source_context: Mapping[str, object] | None = None,
    ) -> TargetCollectionPublication:
        if not isinstance(self.runtime, CollectionTransformRuntime):
            raise RuntimeError("external-effect execution cannot publish a Riverhog collection")
        if (
            implementation.descriptor_sha256
            != self.request.declaration.plan.target_descriptor_sha256
        ):
            raise ValueError("publication implementation differs from the sealed target plan")
        self.runtime.producer_app = implementation.implementation_id
        self.runtime.producer_version = implementation.implementation_version
        writer = self.runtime.open_incremental_publication(
            execution_envelope_sha256=(
                self.request.declaration.controller_evidence.execution_envelope.execution_envelope_sha256
            ),
            source_context={
                **dict(source_context or {}),
                "target_plan_sha256": self.request.declaration.plan.plan_sha256,
                "target_request_sha256": self.request.request_sha256,
            },
        )
        return TargetCollectionPublication(
            self, writer, implementation, source_context=source_context
        )

    def effect_success(
        self,
        result: Mapping[str, JsonValue],
        *,
        operation: OperationContract,
        execution_sha256: str,
        attempt: int = 1,
        runtime_evidence: Mapping[str, object] | None = None,
    ) -> TargetJobStatus:
        """Seal one externally committed effect as a canonical terminal receipt."""

        plan = self.request.declaration.plan
        if plan.protocol != EFFECT_TARGET_PROTOCOL or operation.result_kind != "external-effect":
            raise ValueError("external-effect success requires an effect plan and operation")
        evidence = TargetExecutionEvidence(
            target_descriptor_sha256=plan.target_descriptor_sha256,
            operation_contract_sha256=plan.operation_contract_sha256,
            plan_sha256=plan.plan_sha256,
            execution_sha256=execution_sha256,
            runtime=cast(dict[str, JsonValue], dict(runtime_evidence or {})),
        )
        receipt = ExternalEffectReceipt.seal(
            ExternalEffectReceiptPayload(
                job_id=self.request.declaration.job_id,
                request_sha256=self.request.request_sha256,
                target_descriptor_sha256=plan.target_descriptor_sha256,
                operation_contract_sha256=plan.operation_contract_sha256,
                plan_sha256=plan.plan_sha256,
                execution_sha256=execution_sha256,
                result=dict(result),
            )
        )
        status = TargetJobStatus(
            protocol=plan.protocol,
            job_id=self.request.declaration.job_id,
            state="succeeded",
            attempt=attempt,
            request_sha256=self.request.request_sha256,
            plan_sha256=plan.plan_sha256,
            progress=TargetProgress(phase="done", completed=1, total=1, unit="effect"),
            execution_evidence=evidence,
            effect_receipt=receipt,
        )
        validate_status_against_request(status, self.request, operation)
        self._completed = True
        if self.session is not None:
            self.session.record_completed(status)
        return status


__all__ = ["CancellationCheck", "TargetCollectionPublication", "TargetExecutionRuntime"]
