"""Build/install independent normal and fault wheels and record exact execution.

Use the locked environment on native Linux/macOS/Windows. Local override of the
build tool is recorded, not represented as repository qualification. No secrets,
provider credentials, or external services are read.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--native", action="store_true", help="Run the actual Native/Core test bridge in the existing repository environment")
    args.add_argument("--output", required=True)
    opt = args.parse_args()
    ref = Path(__file__).resolve().parent
    package = ref / "retained-resource"
    result = {"python": sys.version, "platform": sys.platform,
              "setuptools": importlib.metadata.version("setuptools"), "compiler": sysconfig.get_config_var("CC"), "steps": []}
    with tempfile.TemporaryDirectory(prefix="constructor-installed-") as td:
        root = Path(td)
        for fault in (False, True):
            variant = "fault" if fault else "normal"
            source = root / variant / "source"
            shutil.copytree(package, source, ignore=shutil.ignore_patterns("build", "dist", "*.so", "*.pyd", "*.egg-info", "__pycache__"))
            env = dict(os.environ)
            env.pop("RETAINED_RESOURCE_TESTING", None)
            if fault:env["RETAINED_RESOURCE_TESTING"] = "1"
            start = time.monotonic()
            subprocess.run([sys.executable, "setup.py", "bdist_wheel"], cwd=source, env=env, check=True)
            wheel, = (source / "dist").glob("*.whl")
            target = root / variant / "installed"
            # uv is the repository-selected installer; local environments with
            # pip can explicitly run the same wheel install. Neither resolves deps.
            if shutil.which("uv"):
                install = ["uv", "pip", "install", "--python", sys.executable, "--no-deps", "--no-index", "--target", str(target), str(wheel)]
            else:
                install = [sys.executable, "-m", "pip", "install", "--no-deps", "--no-index", "--target", str(target), str(wheel)]
            subprocess.run(install, check=True)
            evidence = {"variant":variant,"wheel_sha256":hashlib.sha256(wheel.read_bytes()).hexdigest()}
            extension = next(p for p in (target / "retained_resource").iterdir() if p.suffix in {".so", ".pyd"})
            evidence["installed_extension_sha256"] = hashlib.sha256(extension.read_bytes()).hexdigest()
            tests = root / variant / "tests"
            shutil.copytree(package / "tests", tests, ignore=shutil.ignore_patterns("__pycache__"))
            if fault:
                # -I: no source/PYTHONPATH fallback. Child fault processes receive
                # only this exact installed wheel through PYTHONPATH.
                code = r'''
import sys,os,unittest,json
sys.path.insert(0,sys.argv[1]);os.environ['PYTHONPATH']=sys.argv[1]
import retained_resource
assert retained_resource.__file__.startswith(sys.argv[1])
suite=unittest.defaultTestLoader.discover(sys.argv[2],pattern='test_receiving.py')
r=unittest.TextTestRunner(verbosity=2).run(suite)
print('WITNESS_COUNTS='+json.dumps(dict(tests=r.testsRun,failures=len(r.failures),errors=len(r.errors),skips=len(r.skipped))))
assert r.testsRun==24 and len(r.skipped)==(3 if sys.platform=='win32' else 0)
raise SystemExit(not r.wasSuccessful())
'''
                command=[sys.executable,"-I","-c",code,str(target),str(tests)]
            else:
                code="import sys,runpy;sys.path.insert(0,sys.argv[1]);runpy.run_path(sys.argv[2],run_name='__main__')"
                command=[sys.executable,"-I","-c",code,str(target),str(tests/'installed_smoke.py')]
            run=subprocess.run(command,cwd=root,capture_output=True,text=True)
            print(run.stdout,run.stderr)
            evidence.update(returncode=run.returncode,elapsed_seconds=time.monotonic()-start)
            for line in run.stdout.splitlines():
                if line.startswith('WITNESS_COUNTS='):evidence['counts']=json.loads(line.split('=',1)[1])
            result['steps'].append(evidence)
            if run.returncode:raise RuntimeError(f'{variant} installed witness failed')
            if not fault and opt.native:
                env=dict(os.environ)
                env["PYTHONPATH"]=os.pathsep.join([str(target),str(ref),str(ref.parent),env.get("PYTHONPATH","")])
                run=subprocess.run([sys.executable,str(ref/"growth_probe.py")],cwd=root,env=env,capture_output=True,text=True)
                if run.returncode:raise RuntimeError(run.stdout+run.stderr)
                result["growth"]=json.loads(run.stdout)
            if fault and opt.native:
                env=dict(os.environ)
                env['PYTHONPATH']=os.pathsep.join([str(target),str(ref),str(ref.parent),env.get('PYTHONPATH','')])
                start=time.monotonic()
                code = """
import sys,unittest,json,retained_resource
assert retained_resource.__file__.startswith(sys.argv[2])
suite=unittest.defaultTestLoader.discover(sys.argv[1],pattern='test_native_bridge.py')
r=unittest.TextTestRunner(verbosity=2).run(suite)
print('WITNESS_COUNTS='+json.dumps(dict(tests=r.testsRun,failures=len(r.failures),errors=len(r.errors),skips=len(r.skipped))))
assert r.testsRun==10 and len(r.skipped)==(2 if sys.platform=='win32' else 0)
raise SystemExit(not r.wasSuccessful())
"""
                run=subprocess.run([sys.executable,'-c',code,str(ref),str(target)],cwd=ref.parent,env=env,capture_output=True,text=True)
                print(run.stdout,run.stderr)
                step=dict(variant='actual-native-reference-bridge',returncode=run.returncode,elapsed_seconds=time.monotonic()-start)
                for line in run.stdout.splitlines():
                    if line.startswith('WITNESS_COUNTS='):step['counts']=json.loads(line.split('=',1)[1])
                result['steps'].append(step)
                if run.returncode:raise RuntimeError('actual Native bridge witness failed')
    Path(opt.output).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,sort_keys=True))

if __name__=='__main__':main()
