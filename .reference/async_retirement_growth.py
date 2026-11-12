"""Observe actual asynchronous phase roots and original worker joins."""
from __future__ import annotations

import builtins
import hashlib
import inspect
import threading
import sys
from collections import OrderedDict, defaultdict
from contextlib import contextmanager, ExitStack
from dataclasses import asdict
from functools import wraps
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_core.services import journal_retirement_registry as journal
from riverhog_core.services import workspace_retirement_registry as shared
from riverhog_core.services import physical_execution as physical
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.upload_journal_validation_retirement import UploadJournalValidationRetirement
from tests.unit import test_upload_journal_validation_retirement_catalog_growth as catalog
from tests.unit import test_upload_journal_validation_retirement_growth as units
from tests.unit import test_physical_workspace_execution_growth as execution
from tests.unit import test_workspace_construction_retention_growth as retention

SCHEDULE = UploadJournalValidationRetirement.advance
REGISTRY = {name: getattr(journal.JournalRetirementRegistry, name) for name in
            ("collect", "resume_ready", "submit", "retained", "_scope", "_create", "_run", "_accept_progress")}
TURN = {name: getattr(journal.JournalRetirementTurn, name) for name in ("advance", "close", "_receive")}
FORMAT = journal.format_retirement_blocker


class PhaseEvidence:
    def __init__(self, case, original, *, cold=False, error=None, text=None, fault=None):
        self.case, self.original = case, original
        self.cold, self.error, self.text, self.fault = cold, error, text, fault
        self.local = threading.local()
        self.roots, self.links, self.gates = [], [], []
        self.lock = threading.Lock()
        self.gate_ready = threading.Condition(self.lock)
        self.originals, self.workers, self.events = [], [], []
        self.gate_threads = {}
        self.main_thread = threading.get_ident()
        self.projections = {}

    def token(self, values, original):
        for index, retained in enumerate(values):
            if retained is original:
                return index
        values.append(original)
        return len(values)-1

    def event(self, kind, *, context=None, worker=None, **fields):
        with self.lock:
            if context is not None:
                fields["original"] = self.token(self.originals,context)
                fields["scope_sha256"] = context.reservation.key.scope_sha256
            if worker is not None:
                fields["attempt"] = self.token(self.workers,worker)
            self.events.append({"event":kind,**fields})

    def publish(self, record_property):
        record_property("growth_samples",[s for op,s,_ in self.roots if op is not REGISTRY["_run"]])
        record_property("separate_original_worker_samples",[s for op,s,_ in self.roots if op is REGISTRY["_run"]])
        record_property("retirement_transcripts",[self.transcript()])

    def transcript(self):
        return {"events":self.events,"root_samples":[{"index":index,"declaration":sample["declaration"],
            "sha256":hashlib.sha256(canonical_json_bytes(sample)).hexdigest(),"outcome":outcome}
            for index,(_,sample,outcome) in enumerate(self.roots)]}

    @property
    def stack(self):
        if not hasattr(self.local, "stack"):
            self.local.stack = []
        return self.local.stack

    def active(self, operation):
        return next((frame for frame in reversed(self.stack) if frame.op is operation), None)

    def emit(self, operation, inputs, metrics, children):
        binding = growth_binding(operation)
        if operation not in self.projections:
            self.projections[operation] = project_bindings((binding,))
        projection = self.projections[operation]
        dimensions, values = dict(inputs), dict(metrics)
        for tag, child in binding.delegates:
            rows = children[tag]
            dimensions[tag + "_calls"] = len(rows)
            values[tag + "_calls"] = values[tag + "_calls_limit"] = len(rows)
            child_binding = growth_binding(child)
            for item in child_binding.declaration.dimensions:
                dimensions[tag + "_" + item.name] = max(
                    (row["dimensions"][item.name] for row in rows), default=0)
            for bound in child_binding.declaration.bounds:
                values[tag + "_" + bound.metric] = sum(row["metrics"][bound.metric] for row in rows)
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(canonical_json_bytes(
                projection["declarations"][binding.declaration.id])).hexdigest(),
            "dimensions": dimensions,
            "metrics": {b.metric: values.get(b.metric, 0) for b in binding.declaration.bounds},
            "delegates": children,
        }
        for tag, child in binding.delegates:
            for row in children[tag]:
                assert all(row["dimensions"][k] <= dimensions[tag + "_" + k]
                           for k in row["dimensions"])
        try:
            validate_execution_samples(projection, binding.declaration.id, [sample])
        except ValueError as error:
            raise AssertionError((binding.declaration.id, str(error), dimensions, sample["metrics"])) from error
        return sample

    def append(self, operation, sample, outcome):
        if self.stack:
            parent = self.stack[-1]
            tags = [tag for tag, child in growth_binding(parent.op).delegates if child is operation]
            assert len(tags) == 1, (parent.op, operation)
            parent.children[tags[0]].append(sample)
        else:
            self.roots.append((operation, sample, outcome))
            if operation is SCHEDULE:
                self.event("schedule-return",sample=len(self.roots)-1,outcome=outcome,
                           bookkeeping=sample["metrics"]["bookkeeping_events"])
            elif operation is REGISTRY["_run"]:
                self.event("worker-root",worker=threading.current_thread(),
                           sample=len(self.roots)-1,outcome=outcome)
            else:
                self.event("phase-root",phase=operation.__name__,sample=len(self.roots)-1,outcome=outcome)

    def inputs(self, operation, args, kw):
        obj = args[0] if args else None
        q = kw.get("limit", 1)
        n = OrderedDict.__len__(self.original.registry._contexts)
        if operation is SCHEDULE:
            return {"unit_limit": max(0, q), "octet_budget": kw.get("octet_budget", 4*1024*1024)}
        if operation in (REGISTRY["collect"], REGISTRY["resume_ready"]):
            return {"limit": q, "retained_contexts": n}
        if operation in (REGISTRY["submit"], REGISTRY["retained"]):
            return {"retained_contexts": n}
        if operation is REGISTRY["_scope"]:
            job = args[1]
            controls = {**asdict(job), "root": str(self.original.cleanup.root),
                        "target_incarnation": "native-journal:" + str(self.original.cleanup.root / job.workspace_id)}
            controls = {k: str(v) if type(v) is int else v for k, v in controls.items()}
            return {"control_octets": len(canonical_json_bytes(controls))}
        if operation is REGISTRY["_accept_progress"]:
            return {"outcomes": 1}
        if operation in (REGISTRY["_create"], REGISTRY["_run"], TURN["close"]):
            return {"originals": 1}
        if operation is TURN["advance"]:
            return {"octet_budget": kw["octet_budget"]}
        if operation is TURN["_receive"]:
            return {"callbacks": len(obj._handles._callbacks), "resources": len(self.resources(obj))}
        if operation is FORMAT:
            return {"error_octets": len((self.text or "").encode())}
        raise AssertionError(operation)

    @staticmethod
    def resources(actor):
        result = [actor._files._directory, actor._files._file]
        for method, _ in actor._handles._callbacks:
            obj = getattr(method, "__self__", None)
            if hasattr(obj, "owner"):
                result.append(obj.owner)
        return list({id(obj): obj for obj in result}.values())

    def wrapper(self, actual):
        @wraps(actual)
        def call(*args, **kwargs):
            if actual in (TURN["advance"],TURN["close"],TURN["_receive"]):
                assert threading.get_ident()!=self.main_thread, "coordinator entered original execution"
            inputs = self.inputs(actual, args, kwargs)
            frame = SimpleNamespace(op=actual, metrics=defaultdict(int),
                                    children={tag: [] for tag, _ in growth_binding(actual).delegates})
            self.stack.append(frame)
            result, caught = None, None
            callback_originals = None
            resources, initial = [], []
            if actual is TURN["_receive"]:
                actor = args[0]
                resources = self.resources(actor)
                initial = [resource.metrics for resource in resources]
                callback_originals = {}
                for index, entry in enumerate(actor._handles._callbacks):
                    method, arguments = entry
                    def invoked(_method=method, _arguments=arguments):
                        frame.metrics["callback_invocations"] += 1
                        return _method(*_arguments)
                    callback_originals[invoked] = entry
                    actor._handles._callbacks[index] = (invoked, ())
            before_contexts = OrderedDict.__len__(self.original.registry._contexts)
            try:
                with ExitStack() as receiving_observers:
                    if actual is TURN["_receive"]:
                        import workspace_reclamation
                        from riverhog_core.retained_workspace_handles import RetainedWorkspaceHandles
                        one=receiving_observers.enter_context(pytest.MonkeyPatch.context())
                        for cls,metric in ((workspace_reclamation.FileReclamationOwner,"file_close_entries"),
                                           (RetainedWorkspaceHandles,"namespace_close_entries")):
                            closing=cls.close
                            def counted_close(obj,*a,_actual=closing,_metric=metric,**kw):
                                frame.metrics[_metric]+=1
                                return _actual(obj,*a,**kw)
                            one.setattr(cls,"close",counted_close)
                    result = actual(*args, **kwargs)
                return result
            except BaseException as error:
                caught = error
                raise
            finally:
                if callback_originals is not None:
                    actor._handles._callbacks[:] = [callback_originals.get(op, (op, a))
                                                     for op, a in actor._handles._callbacks]
                    frame.metrics["native_close_calls"] = sum(r.metrics["close_calls"]-v["close_calls"]
                                                              for r,v in zip(resources,initial))
                    frame.metrics["resource_opens"] = sum(r.metrics["open_calls"]-v["open_calls"]
                                                          for r,v in zip(resources,initial))
                if actual in (SCHEDULE, REGISTRY["collect"], REGISTRY["resume_ready"], REGISTRY["submit"]):
                    frame.metrics["bookkeeping_events"] = 0 if result is None else result
                if actual is REGISTRY["collect"]:
                    frame.metrics["completion_visits"] = sum(r["metrics"]["results_received"] for r in frame.children["physical"])
                    withdrawals = sum(r["metrics"]["reservations_released"] for r in frame.children["retire"])
                    assert result is None or result == frame.metrics["completion_visits"] + withdrawals
                if actual is REGISTRY["submit"]:
                    frame.metrics["context_creations"] = max(0, OrderedDict.__len__(self.original.registry._contexts)-before_contexts)
                if actual is REGISTRY["_create"]:
                    frame.metrics["actors_created"] = int(result is not None)
                    if result is not None:
                        frame.metrics["resource_opens"]=sum(r.metrics["open_calls"] for r in self.resources(result))
                if actual is REGISTRY["_accept_progress"]:
                    frame.metrics["type_checks"] = 1
                if actual is FORMAT:
                    frame.metrics["format_calls"] = 1
                    frame.metrics["formatted_octets"] = inputs["error_octets"]
                    frame.metrics["retained_failure_octets"] = 0 if result is None else len(result.encode())
                if actual in (TURN["advance"], TURN["close"]):
                    frame.metrics["work_entries"] = len(frame.children.get("unit", []))
                    frame.metrics["receive_entries"] = len(frame.children["receive"])
                    frame.metrics["blocker_entries"] = len(frame.children.get("blocked", []))
                if actual is REGISTRY["_run"]:
                    frame.metrics["work_or_close_entries"] = len(frame.children["work"])+len(frame.children["close"])
                    frame.metrics["create_entries"] = len(frame.children["create"])
                    assert threading.get_ident() != self.main_thread
                assert self.stack.pop() is frame
                sample = self.emit(actual, inputs, frame.metrics, frame.children)
                self.append(actual, sample, "raised" if caught else "returned")
        return call

    @contextmanager
    def installed(self, *, install_schedule=True):
        original = self.original
        with pytest.MonkeyPatch.context() as patch:
            operations = [
                (UploadJournalValidationRetirement, "advance"),
                *[(journal.JournalRetirementRegistry, name) for name in REGISTRY],
                *[(journal.JournalRetirementTurn, name) for name in TURN],
                (journal, "format_retirement_blocker"),
            ]
            exact = {
                (obj,name): SCHEDULE if obj is UploadJournalValidationRetirement else
                REGISTRY[name] if obj is journal.JournalRetirementRegistry else
                TURN[name] if obj is journal.JournalRetirementTurn else FORMAT
                for obj,name in operations
            }
            for (obj,name), actual in exact.items():
                if actual is SCHEDULE and not install_schedule:
                    continue
                patch.setattr(obj,name,self.wrapper(actual))
            schedule,collect,resume,scope = SCHEDULE,REGISTRY["collect"],REGISTRY["resume_ready"],REGISTRY["_scope"]
            old_range = builtins.range
            def ranges(*args):
                caller = inspect.currentframe().f_back
                frame = None; metric = None
                if caller.f_code is schedule.__code__:
                    frame = self.active(schedule)
                    metric = "candidate_iterations" if args == (3,) else "outer_iterations"
                    if args == (3,) and "attempted" not in caller.f_locals: metric="outer_iterations"
                elif caller.f_code is shared.WorkspaceRetirementRegistry.collect.__code__:
                    frame=self.active(collect); metric="withdrawal_visits"
                elif caller.f_code is shared.WorkspaceRetirementRegistry.resume_ready.__code__:
                    frame=self.active(resume); metric="context_visits"
                if frame is None:return old_range(*args)
                def counted():
                    for item in old_range(*args):
                        frame.metrics[metric]+=1
                        yield item
                return counted()
            import riverhog_core.services.upload_journal_validation_retirement as cleanup_module
            patch.setattr(cleanup_module,"range",ranges,raising=False)
            patch.setattr(shared,"range",ranges,raising=False)
            actual_select = UploadJournalValidationRetirement._select
            def selected(obj, exclusions):
                root=self.active(schedule)
                if root is None:return actual_select(obj,exclusions)
                root.metrics["excluded_jobs_per_call"] = max(root.metrics["excluded_jobs_per_call"],len(exclusions))
                root.metrics["exclusion_copy_references"] += len(exclusions)
                with pytest.MonkeyPatch.context() as one:
                    one.setattr(UploadJournalValidationRetirement,"_select",actual_select)
                    result,sample=catalog.measure(self.case,obj,"_select",exclusions,cold=self.cold)
                self.append(actual_select,sample,"returned")
                return result
            patch.setattr(UploadJournalValidationRetirement,"_select",selected)
            actual_unit=UploadJournalValidationRetirement._advance
            def advanced(obj,job,budget,*,turn):
                assert threading.get_ident()!=self.main_thread
                if not self.stack:return actual_unit(obj,job,budget,turn=turn)
                errors=[]
                result,sample=units.measure(self.case,obj,job,budget,turn=turn,cold=self.cold,
                    error=self.error,fault=self.fault,observed_error=errors)
                self.append(actual_unit,sample,"raised" if errors[0] else "returned")
                if errors[0] is not None:raise errors[0]
                return result
            patch.setattr(UploadJournalValidationRetirement,"_advance",advanced)
            actual_block=UploadJournalValidationRetirement._blocked
            def blocked(obj,job,failure):
                assert threading.get_ident()!=self.main_thread
                if not self.stack:return actual_block(obj,job,failure)
                old=catalog.read(self.case,job.workspace_id)
                changed=int(old==job and old.collection_id is None and old.failure!=failure)
                with pytest.MonkeyPatch.context() as one:
                    one.setattr(UploadJournalValidationRetirement,"_blocked",actual_block)
                    result,sample=catalog.measure(self.case,obj,"_blocked",job,proposed=failure,cold=self.cold,changed=changed)
                self.append(actual_block,sample,"returned")
                return result
            patch.setattr(UploadJournalValidationRetirement,"_blocked",blocked)
            for name in ("collect_workspace","try_submit_workspace"):
                actual=getattr(physical.PhysicalExecutionOwner,name)
                def invoked(pool,*args,_actual=actual,**kw):
                    if not self.stack:return _actual(pool,*args,**kw)
                    count=len(pool._slots)
                    control=execution.width((args, {k:v for k,v in kw.items() if k not in {"work","owner","identity"}}))
                    control=max(control, max((execution.width((s.key,s.target_incarnation,id(s.owner),True)) for s in pool._slots.values()),default=0))
                    result=None
                    try:
                        with execution.observed(pool,patch) as metrics:
                            result=_actual(pool,*args,**kw)
                            if _actual.__name__=="collect_workspace":metrics["results_received"]=len(result)
                    finally:
                        sample=self.emit(_actual,{"retained_slots":count,"quantum":kw.get("limit",1),
                            "control_octets_per_step":control},metrics,{})
                        self.append(_actual,sample,"returned" if result is not None else "raised")
                    if _actual.__name__=="try_submit_workspace" and result=="submitted":
                        context=kw["identity"]
                        self.event("submitted",context=context,worker=pool._slots[args[0]].worker,
                                   close_only=context.closing)
                    elif _actual.__name__=="collect_workspace":
                        for outcome in result:
                            self.event("physical-received",context=outcome.identity)
                    return result
                patch.setattr(physical.PhysicalExecutionOwner,name,invoked)
            for name in ("try_retain_workspace","retire"):
                actual=getattr(ArchiveConstructionRetention,name)
                def retained(ledger,*args,_actual=actual,**kw):
                    if not self.stack:return _actual(ledger,*args,**kw)
                    count=len(ledger._reservations)
                    arg=args[0];key=getattr(arg,"key",arg)
                    target=getattr(arg,"target_incarnation",kw.get("target_incarnation",""))
                    control=execution.width((key,target,id(kw["owner"]),False))
                    result=None
                    try:
                        with retention.measured(ledger) as metrics:
                            result=_actual(ledger,*args,**kw)
                    finally:
                        sample=self.emit(_actual,{"retained_slots":count,"control_octets_per_step":control},metrics,{})
                        self.append(_actual,sample,"returned" if result is not None else "raised")
                    if _actual.__name__=="retire" and result:
                        context=OrderedDict.get(original.registry._contexts,key.workspace_id)
                        assert context is not None and context.reservation is arg
                        self.event("ordinary-withdrawn",context=context)
                    return result
                patch.setattr(ArchiveConstructionRetention,name,retained)
            actual_metadata=journal.metadata_row_bytes
            actual_canonical=journal.canonical_json_bytes
            def metadata(value):
                result=actual_metadata(value);frame=self.active(scope)
                if frame is not None:
                    frame.metrics["canonical_calls"]+=1;frame.metrics["encoded_octets"]+=result
                return result
            def canonical(value):
                result=actual_canonical(value);frame=self.active(scope)
                if frame is not None:
                    frame.metrics["canonical_calls"]+=1;frame.metrics["encoded_octets"]+=len(result)
                return result
            patch.setattr(journal,"metadata_row_bytes",metadata)
            patch.setattr(journal,"canonical_json_bytes",canonical)
            hashing=journal.hashlib.sha256
            def hashed(value=b"",*a,**kw):
                frame=self.active(scope)
                if frame is not None:
                    frame.metrics["hash_calls"]+=1;frame.metrics["hashed_octets"]+=len(value)
                return hashing(value,*a,**kw)
            patch.setattr(journal.hashlib,"sha256",hashed)
            status=original.pool.status
            def checked_status(*a,**kw):
                frame=self.active(REGISTRY["submit"])
                if frame is not None:frame.metrics["status_calls"]+=1
                return status(*a,**kw)
            patch.setattr(original.pool,"status",checked_status)
            evidence=self
            class Contexts(OrderedDict):
                def counted(self,key=None):
                    for name in ("collect","resume_ready","submit","retained"):
                        frame=evidence.active(REGISTRY[name])
                        if frame is not None:
                            frame.metrics["context_mapping_steps"]+=1
                            if key is not None:frame.metrics["context_key_octets"]+=len(key.encode("ascii"))
                            return frame
                    return None
                def __contains__(self,key):
                    frame=self.counted(key)
                    if frame is not None and frame.op is REGISTRY["retained"]:frame.metrics["lookups"]+=1
                    return super().__contains__(key)
                def __len__(self):
                    self.counted();return super().__len__()
                def __getitem__(self,key):
                    self.counted(key);return super().__getitem__(key)
                def get(self,key,default=None):
                    self.counted(key);return super().get(key,default)
                def __setitem__(self,key,value):
                    self.counted(key);return super().__setitem__(key,value)
                def __delitem__(self,key):
                    self.counted(key);return super().__delitem__(key)
                def popitem(self,*args,**kw):
                    result=super().popitem(*args,**kw);self.counted(result[0]);return result
                def __iter__(self):
                    if any(evidence.active(REGISTRY[n]) for n in ("collect","resume_ready","submit","retained")):
                        raise AssertionError("coordinator enumerated original contexts")
                    return super().__iter__()
            original.registry._contexts=Contexts(original.registry._contexts)
            actual_warning=shared._LOG.warning
            def warning(*a,**kw):
                frame=self.active(collect)
                if frame is not None:frame.metrics["error_log_calls"]+=1
                return actual_warning(*a,**kw)
            patch.setattr(shared._LOG,"warning",warning)
            actual_frames=physical._PhysicalExecutionResult.release_exception_frames
            actual_graph=physical.release_exception_graph
            def graph(error):
                if not self.stack:return actual_graph(error)
                assert threading.get_ident()!=self.main_thread
                todo=[error] if error else []; seen=set(); edges=0; frame_count=0
                while todo:
                    exc=todo.pop()
                    if id(exc) in seen:continue
                    seen.add(id(exc)); tb=exc.__traceback__
                    while tb is not None:frame_count+=1;tb=tb.tb_next
                    outgoing=[x for x in (exc.__cause__,exc.__context__) if x is not None]
                    if isinstance(exc,BaseExceptionGroup):outgoing.extend(exc.exceptions)
                    edges+=len(outgoing);todo.extend(outgoing)
                inputs={"exception_nodes":len(seen),"exception_edges":edges,"traceback_frames":frame_count}
                metrics=defaultdict(int)
                clearing=physical.traceback.clear_frames
                def clear(tb):
                    metrics["frame_clear_calls"]+=1
                    cursor=tb
                    while cursor is not None:metrics["traceback_frames"]+=1;cursor=cursor.tb_next
                    return clearing(tb)
                lines,start=inspect.getsourcelines(actual_graph)
                pop_line=start+next(i for i,line in enumerate(lines) if "current = pending.pop()" in line)
                previous=sys.gettrace()
                def traced(frame,event,arg):
                    if frame.f_code is actual_graph.__code__ and event=="line" and frame.f_lineno==pop_line:
                        metrics["exception_visits"]+=1
                    return traced
                try:
                    with pytest.MonkeyPatch.context() as one:
                        one.setattr(physical.traceback,"clear_frames",clear)
                        sys.settrace(traced)
                        return actual_graph(error)
                finally:
                    sys.settrace(previous)
                    sample=self.emit(actual_graph,inputs,metrics,{})
                    self.append(actual_graph,sample,"observed")
            patch.setattr(physical,"release_exception_graph",graph)
            patch.setattr(shared,"release_exception_graph",graph)
            def frames(outcome):
                if not self.stack:return actual_frames(outcome)
                assert threading.get_ident()!=self.main_thread
                frame=SimpleNamespace(op=actual_frames,metrics={},children={"graph":[]})
                self.stack.append(frame)
                try:return actual_frames(outcome)
                finally:
                    assert self.stack.pop() is frame
                    sample=self.emit(actual_frames,{"outcomes":1},{"graph_entries":len(frame.children["graph"])},frame.children)
                    self.append(actual_frames,sample,"observed")
            patch.setattr(physical._PhysicalExecutionResult,"release_exception_frames",frames)
            actual_run=physical._run
            def gated(work,future):
                if getattr(getattr(work,"func",None),"__self__",None) is not original.registry:
                    return actual_run(work,future)
                entered,release=threading.Event(),threading.Event()
                context=work.args[0]
                worker=threading.current_thread()
                with self.gate_ready:
                    self.gates.append((context,entered,release))
                    self.gate_threads[release]=worker
                    self.gate_ready.notify_all()
                self.event("worker-entered",context=context,worker=worker)
                entered.set()
                assert release.wait(30),"witness did not release original physical worker"
                actual_run(work,future)
            patch.setattr(physical,"_run",gated)
            try:
                yield self
            finally:
                for _,_,release in self.gates:release.set()
                for slot in tuple(original.pool._slots.values()):
                    slot.worker.join(timeout=30)
                    assert not slot.worker.is_alive()

    def drain(self):
        # Exact originals, not Future.done. These joins are outside all roots.
        for slot in tuple(self.original.pool._slots.values()):
            if slot.owner is not self.original.registry:
                continue
            context=slot.identity
            with self.gate_ready:
                def candidate():
                    return next(((entered,release) for original,entered,release in self.gates
                        if original is context and self.gate_threads[release] is slot.worker),None)
                assert self.gate_ready.wait_for(lambda:candidate() is not None,timeout=5)
                entered,release=candidate()
            assert entered.wait(5)
            release.set()
            slot.worker.join(timeout=30)
            assert not slot.worker.is_alive()
            if slot.future.exception() is not None:
                import traceback
                traceback.print_exception(slot.future.exception())
            received=bool(context.actor and context.actor.received)
            self.event("original-joined",context=context,worker=slot.worker,
                       actor_received=received,ordinary_charged=context.reservation.key in self.original.ledger._reservations)
            self.links.append((context,slot.worker,received))
        return self.links
