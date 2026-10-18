"""Drive the one persisted recipe runtime with contract-validated fixture facts."""

from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.recipes import RecipePlanner
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    SemanticValidatorRegistry,
)
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import ArtifactSelection
from stove0_recipe_config.catalog import CompiledRecipeCatalog
from stove0_recipe_config.compiler import compile_recipe
from test_compiled_recipe_runtime import Targets


def run_program(tmp_path, source, dependencies, subjects, *, facts_by_task, owner_kind="work"):
    recipe, closure = compile_recipe(source, dependencies)
    owner = closure.observers[0]
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="fixture.compiled-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(
                    owner.contract, interfaces=(owner.interface.ref,)
                ),
            ),
        )
    )

    class Observers:
        def registration_ids(self):
            return ("facts",)

        def descriptor(self, name):
            assert name == "facts"
            return descriptor

        def semantic_validators(self, name):
            assert name == "facts"
            return SemanticValidatorRegistry()

    url = f"sqlite:///{tmp_path / 'state.db'}"
    planner = RecipePlanner(
        catalog=CompiledRecipeCatalog(recipes=(recipe,), closure=closure),
        state=SqlAlchemyStateStore(url),
        riverhog=object(),
        observers=Observers(),
        targets=Targets(closure.operations[0].contract) if closure.operations else object(),
    )
    inventory = ArtifactSelection.seal(subjects)
    work = planner.create_work(recipe.id, inventory.roots())
    owner_id = work.work_id
    if owner_kind == "preview":
        from stove0_core.preview_state import PreviewRecord
        from stove0_protocol import PlanningJobPayload, PlanningJobRequest

        job = PlanningJobRequest.seal(PlanningJobPayload(work=work, invocation_id="e" * 64))
        planner.state.create_preview(PreviewRecord(job=job))
        owner_id = job.job_id
    planner = planner.for_invocation(owner_kind, owner_id)
    planner.state.retain_selection(inventory)
    row = planner.state.compiled_planning.ensure(work.work_id, recipe)
    planner.state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=row["revision"], scope=inventory.ref()
    )
    for step in range(500):
        progress = planner.step(work)
        if progress.state not in {"pending", "question"}:
            return planner, progress, work
        if progress.state == "question":
            prepared = planner.observation_delivery.request(progress.work, progress.question)
            if prepared is not None:
                request, current = prepared
                facts = {
                    "artifacts": [
                        {"subject_id": subject.id, **facts_by_task[request.task_id][subject.id]}
                        for subject in request.subjects
                    ]
                }
                evidence = ContentObservationEvidence(
                    request=request,
                    result=ContentObservationResultBuilder(current, request).observed(facts),
                )
                planner.observation_delivery.accept(progress.work, evidence)
        if step % 7 == 0:
            planner.state.engine.dispose()
            planner = RecipePlanner(
                catalog=CompiledRecipeCatalog(),
                state=SqlAlchemyStateStore(url).planning_context(owner_kind, owner_id),
                riverhog=planner.riverhog,
                observers=planner.observers,
                targets=planner.targets,
            )
    raise AssertionError("compiled program stopped making bounded progress")
