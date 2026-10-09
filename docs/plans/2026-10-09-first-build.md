# hermes-kubernetes-sandbox — First Build Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A working Hermes Agent terminal-backend plugin (`terminal.backend: kubernetes`) that runs each Hermes session's commands in its own Kubernetes Pod — optionally with a Docker daemon sidecar — plus a cleanup janitor and a validated Kata Containers example deployment.

**Architecture:** The repo root is the Hermes plugin (`plugin.yaml` + `__init__.py`); all code lives in the importable package `kubernetes_sandbox/`. Pure modules (settings, naming, Pod rendering, the exec runner protocol, the v5 websocket framing) have no Hermes dependency and are unit-tested directly; `environment.py` and `provider.py` subclass Hermes's `BaseEnvironment` / `TerminalEnvironmentProvider` and talk to the Kubernetes API through a small `httpx` + `websockets` client (both are pinned Hermes core dependencies, so the plugin adds none). Cluster-side pieces (namespace, RuntimeClass, admission policy, network policy, RBAC, quota, janitor CronJob) ship as `examples/`.

**Tech Stack:** Python 3.11–3.13, httpx 0.28.1, websockets 15.0.1, PyYAML (all from Hermes core), pytest, ruff, uv; Kubernetes ≥ 1.33 (image volumes) with a ≥ 1.30 fallback path; GitHub Actions + kind for example validation.

**Spec:** Design draft `docs/superpowers/specs/2026-10-08-hermes-kubernetes-terminal-backend-design.md` in the operator's homelab repo (decisions K1–K18), and the spike ledger `docs/superpowers/spikes/hermes-sandbox/README.md` there (S1–S6, soak, smoke test). Where the draft is homelab-specific (label domain, gitops ConfigMap template), this plan follows K16–K18: everything lab-specific is configuration or `examples/`.

## Global Constraints

- Provider name `kubernetes`; plugin directory/manifest name `kubernetes-sandbox`; Python package `kubernetes_sandbox` (K17, K1).
- Hermes target: commit `524041b9d0437cd4424a524af4b9e26145e69ab0` (deployed v2026.9.21). Use only `TerminalEnvironmentProvider`, `BaseEnvironment`, `ProcessHandle`, `EnvironmentConnectionError`, `get_activity_callback`, `hermes_constants.get_hermes_home` / `profile_name_for_home`.
- Runtime deps: none beyond Hermes core (`httpx`, `websockets`, `PyYAML`). No `kubernetes` client, no `kubectl` (K2).
- `is_available()` and `probe()` make **no network calls** (Hermes provider contract).
- `runtime_class_name` has **no default**. With `docker: true` and no `runtime_class_name`, the plugin refuses to create sandboxes unless `allow_privileged_host_runtime: true` (K17 interlock).
- Mainstream images only (K16): `work` defaults to Hermes's `DEFAULT_SANDBOX_IMAGE` when importable, else `nousresearch/hermes-sandbox:desktop`; `dind` `docker:29.8.1-dind`; CLI `docker:29.8.1-cli`; formatter `debian:bookworm-slim`. Examples pin every image by digest with `# bump deliberately`.
- Real GNU bash is required in the `work` image (design §1.2.1); never use `sh`/BusyBox images for `work`.
- Docker CLI reaches `work` as a **directory** mount at `/opt/docker-cli/bin` (image volume from the dind image; or an init-copied emptyDir mounted whole) plus `/etc/profile.d/docker-cli.sh` from a ConfigMap. Never a `subPath` into an emptyDir (under Kata it comes up empty).
- `dind` is the only privileged container; `work` runs as uid 1000, `allowPrivilegeEscalation: false`, drops ALL; `automountServiceAccountToken: false` on sandbox Pods.
- Never log or print bearer tokens or Pod exec payloads.
- License MIT; label/annotation prefix `hermes-sandbox/`.

## Review Focus

1. **Arbitrary command text and large stdin** — Hermes wrapper scripts contain every quoting form, heredocs and UTF-8, and file writes pass megabytes on stdin; a person expects byte-exact passthrough. Pinned in Task 5 (`test_runner_exact_script_and_large_stdin`).
2. **Websocket dropped before an exit status** — a person expects an infrastructure error, never a fake exit 0/1. Pinned in Task 6 (`test_handle_raises_connection_error_without_status`) and Task 7 (`test_dropped_stream_raises_environment_connection_error`).
3. **Sandbox Pod deleted between commands** (janitor, eviction, node loss) — a person expects the next command to succeed in a fresh sandbox and the model to be told its workspace was reset. Pinned in Task 7 (`test_recreates_pod_on_404_and_marks_recreated`).
4. **A Pod with the expected name that is not this session's** (hash collision, template change, Failed Pod) — a person expects replacement, never adoption. Pinned in Task 7 (`test_replaces_mismatched_pod`).
5. **Quota exhausted** — a person expects a clear "capacity exhausted, retry after an idle session is reaped" error and no retry storm. Pinned in Task 4 (`test_create_quota_exceeded`) and Task 7 (`test_quota_exhausted_raises_connection_error`).

---

## File Structure

```
plugin.yaml                         Hermes plugin manifest (kind: backend)
__init__.py                         register(ctx): registers KubernetesProvider
kubernetes_sandbox/
  __init__.py                       package marker, __version__
  settings.py                       Settings dataclass; load from YAML + env; validation + K17 interlock
  naming.py                         Pod name, labels, annotations, template hash
  podspec.py                        render_pod(settings, identity) -> dict (pure)
  kube.py                           KubeClient: Pod CRUD over httpx; error types
  runner.py                         exec argv + stdin framing + pgid/kill scripts (pure)
  exec_stream.py                    v5.channel.k8s.io framing; StreamingExecHandle (ProcessHandle)
  environment.py                    KubernetesEnvironment(BaseEnvironment)
  provider.py                       KubernetesProvider(TerminalEnvironmentProvider)
janitor/janitor.py                  stdlib-only reaper run by a CronJob
tests/                              unit tests (one file per module) + conftest.py
examples/kata/                      namespace, RuntimeClass, VAP, NetworkPolicy, RBAC, quota,
                                    profile ConfigMap, janitor CronJob, node config, README
examples/runc-no-docker/            unprivileged sandbox (docker: false), README
scripts/validate-examples.sh        server-side dry-run of every example against a cluster
scripts/e2e.py                      live end-to-end check against a real cluster
.github/workflows/ci.yml            ruff + pytest + kind example validation
pyproject.toml                      dev tooling only (no runtime deps of its own)
```

---

### Task 1: Scaffold, manifest, registration, CI

**Files:**
- Create: `pyproject.toml`, `plugin.yaml`, `__init__.py`, `kubernetes_sandbox/__init__.py`, `tests/conftest.py`, `tests/test_registration.py`, `.github/workflows/ci.yml`
- Modify: `.gitignore` (add `.venv/` already present — no change), `README.md` (Development section)

**Interfaces:**
- Produces: `kubernetes_sandbox.__version__ = "0.1.0"`; root `register(ctx)` calling `ctx.register_terminal_environment_provider(KubernetesProvider())` (provider arrives in Task 8; until then `register` imports lazily so Task 1 tests a stub).

- [ ] **Step 1: Dev environment**

```bash
cd ~/repos/hermes-kubernetes-sandbox
uv venv -p 3.13 .venv
uv pip install --python .venv "hermes-agent @ git+https://github.com/NousResearch/hermes-agent@524041b9d0437cd4424a524af4b9e26145e69ab0" pytest==9.1.1 ruff==0.15.10
.venv/bin/python -c "from tools.environments.base import BaseEnvironment; from agent.terminal_env_provider import TerminalEnvironmentProvider; print('hermes ok')"
```
Expected: `hermes ok`. (If the GitHub install fails because the commit is only in the operator's fork, use `git+https://github.com/dfcowell/hermes-agent@524041b9d0437cd4424a524af4b9e26145e69ab0`.)

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[project]
name = "hermes-kubernetes-sandbox"
version = "0.1.0"
description = "Kubernetes terminal backend plugin for Hermes Agent"
license = "MIT"
requires-python = ">=3.11,<3.14"
dependencies = []  # runtime deps come from Hermes core: httpx, websockets, PyYAML

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]

[tool.ruff]
line-length = 110
target-version = "py311"
extend-exclude = [".venv"]
```

- [ ] **Step 3: Write `plugin.yaml`**

```yaml
name: kubernetes-sandbox
version: 0.1.0
description: Run each Hermes session's commands in its own Kubernetes Pod (optional Docker sidecar; Kata Containers example)
kind: backend
```

- [ ] **Step 4: Write the failing registration test** — `tests/test_registration.py`

```python
import importlib.util
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


class FakeCtx:
    def __init__(self):
        self.providers = []

    def register_terminal_environment_provider(self, provider):
        self.providers.append(provider)


def _load_plugin_root():
    spec = importlib.util.spec_from_file_location(
        "kubernetes_sandbox_plugin", ROOT / "__init__.py", submodule_search_locations=[str(ROOT)])
    mod = importlib.util.module_from_spec(spec)
    import sys
    sys.modules["kubernetes_sandbox_plugin"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_manifest_is_backend_kind():
    import yaml
    meta = yaml.safe_load((ROOT / "plugin.yaml").read_text())
    assert meta["name"] == "kubernetes-sandbox"
    assert meta["kind"] == "backend"


def test_register_adds_one_kubernetes_provider():
    mod = _load_plugin_root()
    ctx = FakeCtx()
    mod.register(ctx)
    assert len(ctx.providers) == 1
    assert ctx.providers[0].name == "kubernetes"
```

- [ ] **Step 5: Run it — expect FAIL** (`.venv/bin/pytest tests/test_registration.py -v` → `FileNotFoundError` / no `register`).

- [ ] **Step 6: Write the root `__init__.py` and package marker**

`__init__.py`:
```python
"""Hermes plugin entry point: registers the `kubernetes` terminal backend."""


def register(ctx):
    from .kubernetes_sandbox.provider import KubernetesProvider
    ctx.register_terminal_environment_provider(KubernetesProvider())
```

`kubernetes_sandbox/__init__.py`:
```python
"""Kubernetes terminal backend for Hermes Agent."""
__version__ = "0.1.0"
```

Temporary `kubernetes_sandbox/provider.py` (replaced in Task 8):
```python
class KubernetesProvider:
    name = "kubernetes"
```

- [ ] **Step 7: `tests/conftest.py`**

```python
import os
import pytest


@pytest.fixture(autouse=True)
def _isolated_hermes_home(tmp_path, monkeypatch):
    home = tmp_path / "hermes-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    for key in list(os.environ):
        if key.startswith("HERMES_K8S_"):
            monkeypatch.delenv(key, raising=False)
    yield home
```

- [ ] **Step 8: Run tests — expect PASS** (`.venv/bin/pytest -v`).

- [ ] **Step 9: CI workflow** — `.github/workflows/ci.yml`

```yaml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6
      - run: uv venv -p 3.13 .venv
      - run: uv pip install --python .venv "hermes-agent @ git+https://github.com/NousResearch/hermes-agent@524041b9d0437cd4424a524af4b9e26145e69ab0" pytest==9.1.1 ruff==0.15.10
      - run: .venv/bin/ruff check .
      - run: .venv/bin/pytest -v
  examples:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: helm/kind-action@v1
        with:
          node_image: kindest/node:v1.33.1
      - run: ./scripts/validate-examples.sh
```
(`scripts/validate-examples.sh` arrives in Task 10; until then the `examples` job is expected to fail — do not merge to `main` before Task 10.)

- [ ] **Step 10: README Development section** — append:

```markdown
## Development

    uv venv -p 3.13 .venv
    uv pip install --python .venv "hermes-agent @ git+https://github.com/NousResearch/hermes-agent@524041b9d0437cd4424a524af4b9e26145e69ab0" pytest==9.1.1 ruff==0.15.10
    .venv/bin/ruff check . && .venv/bin/pytest
```

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml plugin.yaml __init__.py kubernetes_sandbox tests .github README.md
git commit -m "chore: plugin scaffold, manifest, registration test, CI"
```

---

### Task 2: Settings and the privileged-runtime interlock

**Files:**
- Create: `kubernetes_sandbox/settings.py`, `tests/test_settings.py`

**Interfaces:**
- Produces:
  - `class SettingsError(ValueError)`
  - `@dataclass(frozen=True) class Settings` with fields (defaults): `namespace="agent-sandbox"`, `runtime_class_name: str | None = None`, `docker: bool = True`, `allow_privileged_host_runtime: bool = False`, `work_image: str` (Hermes default), `dind_image="docker:29.8.1-dind"`, `docker_cli: str = "image-volume"` (or `"init-copy"`), `cli_image="docker:29.8.1-cli"`, `formatter_image="debian:bookworm-slim"`, `docker_disk_storage_class: str | None = None`, `docker_disk_size="30Gi"`, `workspace_size="10Gi"`, `work_cpu="100m"`, `work_memory="512Mi"`, `dind_cpu_request="500m"`, `dind_cpu_limit="2"`, `dind_memory="1Gi"`, `profile_configmap="hermes-sandbox-profile"`, `max_per_profile=2`, `ready_timeout_seconds=180`, `max_lifetime_seconds=86400`, `api_url="https://kubernetes.default.svc"`, `token_path="/var/run/secrets/kubernetes.io/serviceaccount/token"`, `ca_path: str | None = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"`, `extra_pod_annotations: dict[str, str] = {}`.
  - `def load_settings(home: pathlib.Path) -> Settings` — reads `<home>/kubernetes-sandbox.yaml` (absent → defaults), then env overrides `HERMES_K8S_<FIELD_UPPER>` (bools: `1/true/yes`), then `validate`.
  - `def interlock_error(s: Settings) -> str | None` — the K17 refusal message or `None`.
  - `def validate(s: Settings) -> Settings` — raises `SettingsError` on: unknown `docker_cli`; `docker` true without `docker_disk_storage_class`; non-positive ints.

- [ ] **Step 1: Write failing tests** — `tests/test_settings.py`

```python
import pytest
from kubernetes_sandbox.settings import Settings, SettingsError, interlock_error, load_settings


def test_defaults_without_file(_isolated_hermes_home, monkeypatch):
    monkeypatch.setenv("HERMES_K8S_DOCKER", "false")
    s = load_settings(_isolated_hermes_home)
    assert s.runtime_class_name is None
    assert s.namespace == "agent-sandbox"
    assert s.docker is False


def test_yaml_then_env_override(_isolated_hermes_home, monkeypatch):
    (_isolated_hermes_home / "kubernetes-sandbox.yaml").write_text(
        "runtime_class_name: kata\ndocker_disk_storage_class: fast-block\nmax_per_profile: 3\n")
    monkeypatch.setenv("HERMES_K8S_MAX_PER_PROFILE", "5")
    s = load_settings(_isolated_hermes_home)
    assert s.runtime_class_name == "kata"
    assert s.max_per_profile == 5


def test_docker_requires_block_storage_class(_isolated_hermes_home):
    (_isolated_hermes_home / "kubernetes-sandbox.yaml").write_text("runtime_class_name: kata\n")
    with pytest.raises(SettingsError, match="docker_disk_storage_class"):
        load_settings(_isolated_hermes_home)


def test_unknown_docker_cli_mode_rejected(_isolated_hermes_home):
    (_isolated_hermes_home / "kubernetes-sandbox.yaml").write_text(
        "docker: false\ndocker_cli: copy-please\n")
    with pytest.raises(SettingsError, match="docker_cli"):
        load_settings(_isolated_hermes_home)


def test_unknown_key_rejected(_isolated_hermes_home):
    (_isolated_hermes_home / "kubernetes-sandbox.yaml").write_text("docker: false\nruntimeClass: kata\n")
    with pytest.raises(SettingsError, match="runtimeClass"):
        load_settings(_isolated_hermes_home)


def test_interlock_refuses_privileged_docker_without_runtime_class():
    msg = interlock_error(Settings(docker=True, docker_disk_storage_class="x"))
    assert msg and "root on the node" in msg and "allow_privileged_host_runtime" in msg


def test_interlock_allows_explicit_override_or_runtime_class_or_no_docker():
    assert interlock_error(Settings(docker=True, docker_disk_storage_class="x",
                                    allow_privileged_host_runtime=True)) is None
    assert interlock_error(Settings(docker=True, docker_disk_storage_class="x",
                                    runtime_class_name="kata")) is None
    assert interlock_error(Settings(docker=False)) is None
```

- [ ] **Step 2: Run — expect FAIL** (`ModuleNotFoundError: kubernetes_sandbox.settings`).

- [ ] **Step 3: Implement** — `kubernetes_sandbox/settings.py`

```python
"""Plugin settings: <HERMES_HOME>/kubernetes-sandbox.yaml, overridden by HERMES_K8S_* env vars."""
from __future__ import annotations

import dataclasses
import os
import pathlib

import yaml


def _default_work_image() -> str:
    try:
        from hermes_cli.config_defaults import DEFAULT_SANDBOX_IMAGE  # newer Hermes
        return DEFAULT_SANDBOX_IMAGE
    except Exception:
        return "nousresearch/hermes-sandbox:desktop"


class SettingsError(ValueError):
    pass


@dataclasses.dataclass(frozen=True)
class Settings:
    namespace: str = "agent-sandbox"
    runtime_class_name: str | None = None
    docker: bool = True
    allow_privileged_host_runtime: bool = False
    work_image: str = dataclasses.field(default_factory=_default_work_image)
    dind_image: str = "docker:29.8.1-dind"
    docker_cli: str = "image-volume"
    cli_image: str = "docker:29.8.1-cli"
    formatter_image: str = "debian:bookworm-slim"
    docker_disk_storage_class: str | None = None
    docker_disk_size: str = "30Gi"
    workspace_size: str = "10Gi"
    work_cpu: str = "100m"
    work_memory: str = "512Mi"
    dind_cpu_request: str = "500m"
    dind_cpu_limit: str = "2"
    dind_memory: str = "1Gi"
    profile_configmap: str = "hermes-sandbox-profile"
    max_per_profile: int = 2
    ready_timeout_seconds: int = 180
    max_lifetime_seconds: int = 86400
    api_url: str = "https://kubernetes.default.svc"
    token_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ca_path: str | None = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    extra_pod_annotations: dict = dataclasses.field(default_factory=dict)


_FIELDS = {f.name: f for f in dataclasses.fields(Settings)}
_TRUE = {"1", "true", "yes", "on"}


def _coerce(name: str, raw):
    ftype = _FIELDS[name].type
    if isinstance(raw, str):
        if ftype == "bool":
            return raw.strip().lower() in _TRUE
        if ftype == "int":
            return int(raw)
        if ftype.startswith("str | None") and raw.strip() == "":
            return None
    return raw


def validate(s: Settings) -> Settings:
    if s.docker_cli not in ("image-volume", "init-copy"):
        raise SettingsError(f"docker_cli must be 'image-volume' or 'init-copy', got {s.docker_cli!r}")
    if s.docker and not s.docker_disk_storage_class:
        raise SettingsError("docker: true needs docker_disk_storage_class (a StorageClass that supports "
                            "volumeMode: Block; /var/lib/docker must not live on a shared filesystem)")
    for name in ("max_per_profile", "ready_timeout_seconds", "max_lifetime_seconds"):
        if getattr(s, name) <= 0:
            raise SettingsError(f"{name} must be positive")
    return s


def load_settings(home: pathlib.Path) -> Settings:
    values: dict = {}
    path = pathlib.Path(home) / "kubernetes-sandbox.yaml"
    if path.exists():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        unknown = sorted(set(data) - set(_FIELDS))
        if unknown:
            raise SettingsError(f"unknown setting(s) in {path}: {', '.join(unknown)}")
        values.update(data)
    for name in _FIELDS:
        env = os.environ.get(f"HERMES_K8S_{name.upper()}")
        if env is not None:
            values[name] = env
    values = {k: _coerce(k, v) for k, v in values.items()}
    return validate(Settings(**values))


def interlock_error(s: Settings) -> str | None:
    if s.docker and not s.runtime_class_name and not s.allow_privileged_host_runtime:
        return ("kubernetes sandbox refused: docker: true runs a privileged Docker daemon in every sandbox, "
                "and without a VM-isolated runtime_class_name (e.g. Kata Containers) that daemon is root on "
                "the node — agents could read host disks and other pods' volumes. Set runtime_class_name, or "
                "docker: false, or (only if you accept that) allow_privileged_host_runtime: true.")
    return None
```

- [ ] **Step 4: Run — expect PASS** (`.venv/bin/pytest tests/test_settings.py -v`).

- [ ] **Step 5: Commit** — `git add kubernetes_sandbox/settings.py tests/test_settings.py && git commit -m "feat: settings with K17 privileged-runtime interlock"`

---

### Task 3: Naming, labels, annotations, template hash; Pod rendering

**Files:**
- Create: `kubernetes_sandbox/naming.py`, `kubernetes_sandbox/podspec.py`, `tests/test_naming.py`, `tests/test_podspec.py`

**Interfaces:**
- Consumes: `Settings` (Task 2).
- Produces:
  - `PREFIX = "hermes-sandbox/"`; `MANAGED_BY = {"app.kubernetes.io/managed-by": "hermes-kubernetes-sandbox"}`
  - `@dataclass(frozen=True) class Identity: profile: str; session_key: str` with properties `key_hash` (sha256 of `f"{profile}\0{session_key}"`, first 10 hex) and `pod_name` (`f"hs-{slug(profile)}-{key_hash}"`, DNS-1123, ≤ 63 chars).
  - `def slug(text: str, max_len: int = 40) -> str`
  - `def render_pod(s: Settings, ident: Identity, *, now_iso: str, created_by: str) -> dict` — full Pod manifest; label `hermes-sandbox/template-hash` = `template_hash(s)`.
  - `def template_hash(s: Settings) -> str` — sha256 of the JSON of the Pod spec rendered for a fixed placeholder identity, first 8 hex.
  - Annotation keys: `hermes-sandbox/session-key`, `hermes-sandbox/last-used`, `hermes-sandbox/max-lifetime-seconds`, `hermes-sandbox/created-by`.

- [ ] **Step 1: Failing tests** — `tests/test_naming.py`

```python
import re
from kubernetes_sandbox.naming import Identity, slug

DNS1123 = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def test_pod_name_is_dns1123_deterministic_and_bounded():
    ident = Identity(profile="Mars_Lab.Profile", session_key="telegram:123/thread:9")
    assert ident.pod_name == Identity("Mars_Lab.Profile", "telegram:123/thread:9").pod_name
    assert DNS1123.match(ident.pod_name) and len(ident.pod_name) <= 63
    assert ident.pod_name.startswith("hs-mars-lab-profile-")


def test_different_sessions_differ_and_profile_is_part_of_hash():
    a, b = Identity("p", "s1"), Identity("p", "s2")
    c = Identity("q", "s1")
    assert len({a.pod_name, b.pod_name, c.key_hash, a.key_hash}) == 4


def test_slug_handles_empty_and_long():
    assert slug("") == "default"
    assert len(slug("x" * 200)) <= 40 and DNS1123.match(slug("--A--" + "b" * 100))
```

`tests/test_podspec.py`:
```python
from kubernetes_sandbox.naming import Identity
from kubernetes_sandbox.podspec import render_pod, template_hash
from kubernetes_sandbox.settings import Settings

KATA = Settings(runtime_class_name="kata", docker_disk_storage_class="block-ephemeral")


def _pod(s=KATA):
    return render_pod(s, Identity("mars", "sess-1"), now_iso="2026-10-09T00:00:00Z", created_by="gw-0")


def _containers(pod):
    return {c["name"]: c for c in pod["spec"]["containers"]}


def test_identity_metadata_and_runtime():
    pod = _pod()
    md = pod["metadata"]
    assert md["name"] == Identity("mars", "sess-1").pod_name
    assert md["labels"]["hermes-sandbox/profile"] == "mars"
    assert md["labels"]["hermes-sandbox/template-hash"] == template_hash(KATA)
    assert md["annotations"]["hermes-sandbox/session-key"] == "sess-1"
    assert pod["spec"]["runtimeClassName"] == "kata"
    assert pod["spec"]["automountServiceAccountToken"] is False
    assert pod["spec"]["restartPolicy"] == "Never"


def test_only_dind_is_privileged_and_work_is_uid_1000():
    pod = _pod()
    c = _containers(pod)
    assert c["dind"]["securityContext"]["privileged"] is True
    w = c["work"]["securityContext"]
    assert w["runAsUser"] == 1000 and w["allowPrivilegeEscalation"] is False
    assert w["capabilities"]["drop"] == ["ALL"]
    for init in pod["spec"].get("initContainers", []):
        assert not init.get("securityContext", {}).get("privileged")


def test_docker_cli_is_directory_mount_never_emptydir_subpath():
    for mode in ("image-volume", "init-copy"):
        pod = _pod(Settings(runtime_class_name="kata", docker_disk_storage_class="b", docker_cli=mode))
        vols = {v["name"]: v for v in pod["spec"]["volumes"]}
        mounts = _containers(pod)["work"]["volumeMounts"]
        cli = [m for m in mounts if m["mountPath"].startswith("/opt/docker-cli")]
        assert cli, mode
        for m in mounts:
            if "subPath" in m:
                assert "emptyDir" not in vols[m["name"]], (mode, m)
        assert any(m["mountPath"] == "/etc/profile.d/docker-cli.sh" for m in mounts)


def test_docker_disk_is_generic_ephemeral_block():
    vols = {v["name"]: v for v in _pod()["spec"]["volumes"]}
    tmpl = vols["docker-disk"]["ephemeral"]["volumeClaimTemplate"]["spec"]
    assert tmpl["volumeMode"] == "Block" and tmpl["storageClassName"] == "block-ephemeral"


def test_every_container_declares_requests_and_memory_limit():
    pod = _pod()
    for c in pod["spec"]["containers"] + pod["spec"].get("initContainers", []):
        r = c["resources"]
        assert r["requests"]["cpu"] and r["requests"]["memory"] and r["limits"]["memory"], c["name"]


def test_no_docker_means_single_unprivileged_container():
    pod = _pod(Settings(docker=False))
    assert list(_containers(pod)) == ["work"]
    assert "runtimeClassName" not in pod["spec"]
    assert "initContainers" not in pod["spec"]


def test_template_hash_changes_with_spec_not_identity():
    other = Settings(runtime_class_name="kata", docker_disk_storage_class="block-ephemeral", work_memory="1Gi")
    assert template_hash(KATA) != template_hash(other)
    assert template_hash(KATA) == template_hash(KATA)
```

- [ ] **Step 2: Run — expect FAIL** (modules missing).

- [ ] **Step 3: Implement** — `kubernetes_sandbox/naming.py`

```python
"""Deterministic sandbox identity: Pod name, labels and annotations."""
from __future__ import annotations

import dataclasses
import hashlib
import re

PREFIX = "hermes-sandbox/"
MANAGED_BY = {"app.kubernetes.io/managed-by": "hermes-kubernetes-sandbox"}
ANN_SESSION_KEY = PREFIX + "session-key"
ANN_LAST_USED = PREFIX + "last-used"
ANN_MAX_LIFETIME = PREFIX + "max-lifetime-seconds"
ANN_CREATED_BY = PREFIX + "created-by"
LBL_PROFILE = PREFIX + "profile"
LBL_KEY_HASH = PREFIX + "key-hash"
LBL_TEMPLATE_HASH = PREFIX + "template-hash"


def slug(text: str, max_len: int = 40) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")[:max_len].strip("-")
    return s or "default"


@dataclasses.dataclass(frozen=True)
class Identity:
    profile: str
    session_key: str

    @property
    def key_hash(self) -> str:
        return hashlib.sha256(f"{self.profile}\0{self.session_key}".encode()).hexdigest()[:10]

    @property
    def pod_name(self) -> str:
        return f"hs-{slug(self.profile)}-{self.key_hash}"
```

`kubernetes_sandbox/podspec.py`:
```python
"""Render a sandbox Pod manifest (pure; no cluster access)."""
from __future__ import annotations

import copy
import hashlib
import json

from .naming import (ANN_CREATED_BY, ANN_LAST_USED, ANN_MAX_LIFETIME, ANN_SESSION_KEY, LBL_KEY_HASH,
                     LBL_PROFILE, LBL_TEMPLATE_HASH, MANAGED_BY, Identity, slug)
from .settings import Settings

_WORK_SC = {"runAsUser": 1000, "runAsGroup": 1000, "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]}}
DOCKER_GID = 2375
_CLI_DIR = "/opt/docker-cli"


def _spec(s: Settings) -> dict:
    work = {
        "name": "work", "image": s.work_image, "command": ["sleep", "infinity"],
        "securityContext": copy.deepcopy(_WORK_SC),
        "env": [], "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}],
        "workingDir": "/workspace",
        "resources": {"requests": {"cpu": s.work_cpu, "memory": s.work_memory},
                      "limits": {"memory": s.work_memory}},
    }
    spec: dict = {
        "automountServiceAccountToken": False, "restartPolicy": "Never", "enableServiceLinks": False,
        "securityContext": {"fsGroup": 1000},
        "containers": [work],
        "volumes": [{"name": "workspace", "emptyDir": {"sizeLimit": s.workspace_size}}],
    }
    if s.runtime_class_name:
        spec["runtimeClassName"] = s.runtime_class_name
    if not s.docker:
        return spec

    spec["securityContext"]["supplementalGroups"] = [DOCKER_GID]
    work["env"].append({"name": "DOCKER_HOST", "value": "unix:///run/docker/docker.sock"})
    work["volumeMounts"] += [
        {"name": "docker-sock", "mountPath": "/run/docker"},
        {"name": "profile", "mountPath": "/etc/profile.d/docker-cli.sh", "subPath": "docker-cli.sh",
         "readOnly": True},
    ]
    spec["volumes"] += [
        {"name": "docker-sock", "emptyDir": {}},
        {"name": "profile", "configMap": {"name": s.profile_configmap}},
        {"name": "docker-disk", "ephemeral": {"volumeClaimTemplate": {"spec": {
            "storageClassName": s.docker_disk_storage_class, "volumeMode": "Block",
            "accessModes": ["ReadWriteOncePod"], "resources": {"requests": {"storage": s.docker_disk_size}}}}}},
    ]
    init = [{
        "name": "format", "image": s.formatter_image,
        "command": ["/bin/sh", "-c",
                    "blkid -p /dev/docker-disk >/dev/null 2>&1 || mkfs.ext4 -q -L docker /dev/docker-disk"],
        "volumeDevices": [{"name": "docker-disk", "devicePath": "/dev/docker-disk"}],
        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"memory": "256Mi"}},
    }]
    if s.docker_cli == "image-volume":
        spec["volumes"].append({"name": "dind-image", "image": {"reference": s.dind_image,
                                                                "pullPolicy": "IfNotPresent"}})
        work["volumeMounts"] += [
            {"name": "dind-image", "mountPath": f"{_CLI_DIR}/bin", "subPath": "usr/local/bin", "readOnly": True},
            {"name": "dind-image", "mountPath": "/usr/local/libexec/docker/cli-plugins",
             "subPath": "usr/local/libexec/docker/cli-plugins", "readOnly": True},
        ]
    else:
        spec["volumes"].append({"name": "docker-cli", "emptyDir": {"sizeLimit": "256Mi"}})
        work["volumeMounts"].append({"name": "docker-cli", "mountPath": _CLI_DIR})
        work["env"].append({"name": "DOCKER_CONFIG", "value": _CLI_DIR})
        init.append({
            "name": "docker-cli", "image": s.cli_image,
            "command": ["/bin/sh", "-c",
                        f"mkdir -p {_CLI_DIR}/bin {_CLI_DIR}/cli-plugins && cp /usr/local/bin/docker {_CLI_DIR}/bin/ "
                        f"&& cp /usr/local/libexec/docker/cli-plugins/docker-* {_CLI_DIR}/cli-plugins/"],
            "volumeMounts": [{"name": "docker-cli", "mountPath": _CLI_DIR}],
            "securityContext": copy.deepcopy(_WORK_SC),
            "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}},
        })
    spec["initContainers"] = init
    spec["containers"].insert(0, {
        "name": "dind", "image": s.dind_image, "securityContext": {"privileged": True},
        "command": ["/bin/sh", "-c",
                    "set -eu; mkdir -p /var/lib/docker; mount -t ext4 -o noatime /dev/docker-disk /var/lib/docker; "
                    f"exec dind dockerd --host=unix:///run/docker/docker.sock --group={DOCKER_GID} "
                    "--storage-driver=overlay2 --feature containerd-snapshotter=false"],
        "readinessProbe": {"exec": {"command": ["docker", "-H", "unix:///run/docker/docker.sock", "info"]},
                           "periodSeconds": 2, "failureThreshold": 3},
        "volumeDevices": [{"name": "docker-disk", "devicePath": "/dev/docker-disk"}],
        "volumeMounts": [{"name": "docker-sock", "mountPath": "/run/docker"},
                         {"name": "workspace", "mountPath": "/workspace"}],
        "resources": {"requests": {"cpu": s.dind_cpu_request, "memory": s.dind_memory},
                      "limits": {"cpu": s.dind_cpu_limit, "memory": s.dind_memory}},
    })
    return spec


def template_hash(s: Settings) -> str:
    return hashlib.sha256(json.dumps(_spec(s), sort_keys=True).encode()).hexdigest()[:8]


def render_pod(s: Settings, ident: Identity, *, now_iso: str, created_by: str) -> dict:
    labels = {**MANAGED_BY, LBL_PROFILE: slug(ident.profile, 63), LBL_KEY_HASH: ident.key_hash,
              LBL_TEMPLATE_HASH: template_hash(s)}
    annotations = {**s.extra_pod_annotations, ANN_SESSION_KEY: ident.session_key, ANN_LAST_USED: now_iso,
                   ANN_MAX_LIFETIME: str(s.max_lifetime_seconds), ANN_CREATED_BY: created_by}
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": ident.pod_name, "namespace": s.namespace,
                         "labels": labels, "annotations": annotations},
            "spec": _spec(s)}
```

- [ ] **Step 4: Run — expect PASS** (`.venv/bin/pytest tests/test_naming.py tests/test_podspec.py -v`).

- [ ] **Step 5: Commit** — `git add kubernetes_sandbox/naming.py kubernetes_sandbox/podspec.py tests/test_naming.py tests/test_podspec.py && git commit -m "feat: sandbox identity and Pod rendering"`

---

### Task 4: Kubernetes REST client

**Files:**
- Create: `kubernetes_sandbox/kube.py`, `tests/test_kube.py`

**Interfaces:**
- Consumes: `Settings` (api_url, token_path, ca_path, namespace).
- Produces:
  - Exceptions: `class KubeError(RuntimeError)` (attrs `status: int`, `reason: str`); `class NotFound(KubeError)`; `class QuotaExceeded(KubeError)`; `class Conflict(KubeError)`.
  - `class KubeClient`: `__init__(self, settings, *, transport: httpx.BaseTransport | None = None)`; methods `get_pod(name) -> dict`, `create_pod(manifest) -> dict`, `delete_pod(name, grace_seconds=10) -> None` (404 ignored), `list_pods(label_selector: str) -> list[dict]`, `patch_pod_annotations(name, annotations: dict) -> None` (merge-patch), `exec_url(name, container, argv) -> str` (wss URL), `auth_headers() -> dict`, `ssl_context() -> ssl.SSLContext | bool`.
  - Token is re-read from `token_path` on every request (projected tokens rotate).

- [ ] **Step 1: Failing tests** — `tests/test_kube.py`

```python
import json
import httpx
import pytest
from kubernetes_sandbox.kube import KubeClient, NotFound, QuotaExceeded
from kubernetes_sandbox.settings import Settings


@pytest.fixture
def settings(tmp_path):
    tok = tmp_path / "token"
    tok.write_text("t0k3n\n")
    return Settings(docker=False, api_url="https://api.example:6443", token_path=str(tok), ca_path=None)


def _client(settings, handler):
    return KubeClient(settings, transport=httpx.MockTransport(handler))


def test_get_pod_sends_bearer_and_namespace(settings):
    seen = {}

    def handler(req):
        seen["auth"] = req.headers["authorization"]
        seen["path"] = req.url.path
        return httpx.Response(200, json={"metadata": {"name": "p"}})

    assert _client(settings, handler).get_pod("p")["metadata"]["name"] == "p"
    assert seen == {"auth": "Bearer t0k3n", "path": "/api/v1/namespaces/agent-sandbox/pods/p"}


def test_token_is_reread_each_request(settings, tmp_path):
    auths = []

    def handler(req):
        auths.append(req.headers["authorization"])
        return httpx.Response(200, json={})

    c = _client(settings, handler)
    c.get_pod("a")
    (tmp_path / "token").write_text("rotated")
    c.get_pod("a")
    assert auths == ["Bearer t0k3n", "Bearer rotated"]


def test_get_pod_404_is_notfound(settings):
    with pytest.raises(NotFound):
        _client(settings, lambda r: httpx.Response(404, json={"reason": "NotFound"})).get_pod("x")


def test_create_quota_exceeded(settings):
    body = {"kind": "Status", "reason": "Forbidden",
            "message": 'pods "x" is forbidden: exceeded quota: sandbox-capacity, requested: pods=1'}
    with pytest.raises(QuotaExceeded, match="exceeded quota"):
        _client(settings, lambda r: httpx.Response(403, json=body)).create_pod({"metadata": {"name": "x"}})


def test_delete_ignores_404(settings):
    _client(settings, lambda r: httpx.Response(404, json={})).delete_pod("gone")


def test_patch_annotations_uses_merge_patch(settings):
    seen = {}

    def handler(req):
        seen["ctype"] = req.headers["content-type"]
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={})

    _client(settings, handler).patch_pod_annotations("p", {"a": "1"})
    assert seen == {"ctype": "application/merge-patch+json", "body": {"metadata": {"annotations": {"a": "1"}}}}


def test_list_pods_passes_selector(settings):
    def handler(req):
        assert req.url.params["labelSelector"] == "a=b"
        return httpx.Response(200, json={"items": [{"metadata": {"name": "p1"}}]})

    assert [p["metadata"]["name"] for p in _client(settings, handler).list_pods("a=b")] == ["p1"]


def test_exec_url_encodes_argv(settings):
    url = _client(settings, lambda r: httpx.Response(200)).exec_url("p", "work", ["bash", "-c", "a b&c"])
    assert url.startswith("wss://api.example:6443/api/v1/namespaces/agent-sandbox/pods/p/exec?")
    assert "container=work" in url and "command=bash" in url and "command=a+b%26c" in url
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — `kubernetes_sandbox/kube.py`

```python
"""Minimal Kubernetes API client for sandbox Pods (httpx; no kubernetes package)."""
from __future__ import annotations

import pathlib
import ssl
import urllib.parse

import httpx

from .settings import Settings


class KubeError(RuntimeError):
    def __init__(self, status: int, reason: str, message: str):
        super().__init__(f"{status} {reason}: {message}")
        self.status, self.reason = status, reason


class NotFound(KubeError):
    pass


class Conflict(KubeError):
    pass


class QuotaExceeded(KubeError):
    pass


class KubeClient:
    def __init__(self, settings: Settings, *, transport: httpx.BaseTransport | None = None):
        self._s = settings
        self._base = settings.api_url.rstrip("/")
        self._http = httpx.Client(transport=transport, verify=self.ssl_context(), timeout=30.0)

    def ssl_context(self):
        if self._s.ca_path and pathlib.Path(self._s.ca_path).exists():
            return ssl.create_default_context(cafile=self._s.ca_path)
        return True

    def auth_headers(self) -> dict:
        token = pathlib.Path(self._s.token_path).read_text(encoding="utf-8").strip()
        return {"Authorization": f"Bearer {token}"}

    def _pods(self, name: str = "") -> str:
        path = f"{self._base}/api/v1/namespaces/{self._s.namespace}/pods"
        return f"{path}/{name}" if name else path

    def _request(self, method: str, url: str, **kw) -> httpx.Response:
        headers = {**self.auth_headers(), **kw.pop("headers", {})}
        r = self._http.request(method, url, headers=headers, **kw)
        if r.status_code < 400:
            return r
        try:
            body = r.json()
        except Exception:
            body = {}
        reason, message = body.get("reason", ""), body.get("message", r.text[:300])
        if r.status_code == 404:
            raise NotFound(404, reason, message)
        if r.status_code == 409:
            raise Conflict(409, reason, message)
        if r.status_code == 403 and "exceeded quota" in message:
            raise QuotaExceeded(403, reason, message)
        raise KubeError(r.status_code, reason, message)

    def get_pod(self, name: str) -> dict:
        return self._request("GET", self._pods(name)).json()

    def create_pod(self, manifest: dict) -> dict:
        return self._request("POST", self._pods(), json=manifest).json()

    def delete_pod(self, name: str, grace_seconds: int = 10) -> None:
        try:
            self._request("DELETE", self._pods(name), json={"gracePeriodSeconds": grace_seconds})
        except NotFound:
            pass

    def list_pods(self, label_selector: str) -> list[dict]:
        return self._request("GET", self._pods(), params={"labelSelector": label_selector}).json().get("items", [])

    def patch_pod_annotations(self, name: str, annotations: dict) -> None:
        self._request("PATCH", self._pods(name), content=__import__("json").dumps(
            {"metadata": {"annotations": annotations}}), headers={"Content-Type": "application/merge-patch+json"})

    def exec_url(self, name: str, container: str, argv: list[str]) -> str:
        q = [("container", container), ("stdin", "true"), ("stdout", "true"), ("stderr", "true")]
        q += [("command", a) for a in argv]
        base = self._base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        return f"{base}/api/v1/namespaces/{self._s.namespace}/pods/{name}/exec?{urllib.parse.urlencode(q)}"
```

- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** — `git add kubernetes_sandbox/kube.py tests/test_kube.py && git commit -m "feat: minimal Kubernetes REST client"`

---

### Task 5: Exec runner protocol (argv, stdin framing, process group, kill)

The command must not ride in the URL (size limits, audit logs). The exec argv is a fixed small runner; the real script and the caller's stdin follow on the websocket stdin.

**Files:**
- Create: `kubernetes_sandbox/runner.py`, `tests/test_runner.py`

**Interfaces:**
- Produces:
  - `def exec_argv(exec_id: str, *, login: bool, timeout: int) -> list[str]` — `["bash", "-c", RUNNER, "hermes-runner", exec_id, "1"|"0", str(timeout)]`.
  - `def stdin_payload(script: str, stdin_data: str | None) -> bytes` — `f"{len(script_bytes)}\n".encode() + script_bytes + (stdin_data or "").encode()`.
  - `def kill_argv(exec_id: str) -> list[str]` — TERM the recorded process group, KILL after 2 s.
  - `PGID_DIR = "/tmp"`; pgid file `/tmp/hermes-exec-<exec_id>.pgid`.

- [ ] **Step 1: Failing tests** (run the runner with local bash — this is the real protocol) — `tests/test_runner.py`

```python
import os
import pathlib
import subprocess
import time
import uuid

from kubernetes_sandbox.runner import exec_argv, kill_argv, stdin_payload


def _run(script, stdin_data=None, *, login=False, timeout=30):
    exec_id = uuid.uuid4().hex[:12]
    argv = exec_argv(exec_id, login=login, timeout=timeout)
    p = subprocess.run(argv, input=stdin_payload(script, stdin_data), capture_output=True, timeout=60)
    return p, exec_id


def test_runner_exact_script_and_large_stdin():
    script = r'''printf '%s|' "a'b" 'c"d' "$(echo $'tab\there')" ünïcødé; cat <<'EOF'
heredoc $NOT_EXPANDED
EOF
wc -c'''
    payload = ("x" * 1023 + "\n") * 1024  # 1 MiB
    p, _ = _run(script, payload)
    out = p.stdout.decode()
    assert p.returncode == 0, p.stderr
    assert out.startswith("a'b|c\"d|tab\there|ünïcødé|heredoc $NOT_EXPANDED\n")
    assert out.rstrip().endswith(str(len(payload)))


def test_runner_exit_code_and_stderr_pass_through():
    p, _ = _run("echo oops >&2; exit 7")
    assert p.returncode == 7 and b"oops" in p.stderr


def test_runner_no_stdin_data_gives_eof():
    p, _ = _run("cat; echo done")
    assert p.stdout == b"done\n"


def test_runner_records_pgid_and_kill_stops_process_group():
    exec_id = uuid.uuid4().hex[:12]
    proc = subprocess.Popen(exec_argv(exec_id, login=False, timeout=60), stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.stdin.write(stdin_payload("sleep 300 & sleep 300; echo never", None))
    proc.stdin.close()
    pgid_file = pathlib.Path(f"/tmp/hermes-exec-{exec_id}.pgid")
    for _ in range(50):
        if pgid_file.exists() and pgid_file.read_text().strip():
            break
        time.sleep(0.1)
    pgid = int(pgid_file.read_text())
    subprocess.run(kill_argv(exec_id), check=True, timeout=10)
    assert proc.wait(timeout=10) != 0
    time.sleep(0.3)
    try:
        os.killpg(pgid, 0)
        alive = True
    except ProcessLookupError:
        alive = False
    assert not alive and not pgid_file.exists()


def test_runner_timeout_backstop():
    start = time.time()
    p, _ = _run("sleep 30", timeout=1)
    assert p.returncode in (124, 137) and time.time() - start < 15
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — `kubernetes_sandbox/runner.py`

```python
"""The exec protocol inside the sandbox. The websocket exec runs a fixed RUNNER; the Hermes
script and the caller's stdin arrive on stdin as  <script byte length>\\n<script><stdin...>.
`read -N` under LC_ALL=C reads exactly that many bytes without over-consuming stdin."""
from __future__ import annotations

PGID_DIR = "/tmp"

RUNNER = r'''
export LC_ALL=C
IFS= read -r __hs_n || exit 125
IFS= read -r -d '' -N "$__hs_n" __hs_script || [ "${#__hs_script}" -eq "$__hs_n" ] || exit 125
unset LC_ALL
__hs_pgid_file="/tmp/hermes-exec-$1.pgid"
__hs_login="$2"
__hs_timeout="$3"
if [ "$__hs_login" = 1 ]; then __hs_bash=(bash -l -c); else __hs_bash=(bash -c); fi
setsid --wait timeout --kill-after=10 "$((__hs_timeout + 5))" "${__hs_bash[@]}" '
  read -r _ _ _ _ __pg _ < /proc/$$/stat; echo "$__pg" > "$1"; shift; eval "$1"' \
  hermes-cmd "$__hs_pgid_file" "$__hs_script"
__hs_rc=$?
rm -f "$__hs_pgid_file"
exit "$__hs_rc"
'''


def exec_argv(exec_id: str, *, login: bool, timeout: int) -> list[str]:
    return ["bash", "-c", RUNNER, "hermes-runner", exec_id, "1" if login else "0", str(int(timeout))]


def stdin_payload(script: str, stdin_data: str | None) -> bytes:
    body = script.encode("utf-8")
    return f"{len(body)}\n".encode() + body + (stdin_data or "").encode("utf-8")


def kill_argv(exec_id: str) -> list[str]:
    f = f"{PGID_DIR}/hermes-exec-{exec_id}.pgid"
    return ["bash", "-c",
            f'pg=$(cat {f} 2>/dev/null) || exit 0; kill -TERM -- "-$pg" 2>/dev/null; '
            f'for i in 1 2 3 4 5 6 7 8 9 10; do kill -0 -- "-$pg" 2>/dev/null || break; sleep 0.2; done; '
            f'kill -KILL -- "-$pg" 2>/dev/null; rm -f {f}; exit 0']
```

Notes for the implementer: `eval "$1"` runs the Hermes script in the process-group leader's bash, so the user's commands (and their children, backgrounded or not) share the recorded process group. If `test_runner_exact_script_and_large_stdin` fails on the `read -N` line, verify with `bash --version` ≥ 4.1 and that `LC_ALL=C` is exported before `read`.

- [ ] **Step 4: Run — expect PASS** (`.venv/bin/pytest tests/test_runner.py -v`).
- [ ] **Step 5: Commit** — `git add kubernetes_sandbox/runner.py tests/test_runner.py && git commit -m "feat: exec runner protocol with stdin framing and process-group kill"`

---

### Task 6: v5 websocket framing and the streaming ProcessHandle

**Files:**
- Create: `kubernetes_sandbox/exec_stream.py`, `tests/test_exec_stream.py`

**Interfaces:**
- Consumes: `exec_argv`, `stdin_payload`, `kill_argv` (Task 5); `KubeClient.exec_url`, `auth_headers`, `ssl_context` (Task 4).
- Produces:
  - Constants `STDIN=0, STDOUT=1, STDERR=2, ERROR=3, CLOSE=255`; `SUBPROTOCOL="v5.channel.k8s.io"`.
  - `def encode_frame(channel: int, data: bytes) -> bytes`; `def close_stdin_frame() -> bytes` (= `bytes([255, 0])`).
  - `def parse_status(payload: bytes) -> int` — `Success` → 0; `Failure` with `details.causes[{reason: ExitCode, message: N}]` → N; any other `Failure` → raises `ExecFailed(message)`.
  - `class ExecFailed(RuntimeError)`; `class ExecDisconnected(RuntimeError)`.
  - `class StreamingExecHandle` (satisfies Hermes `ProcessHandle`): `__init__(self, connect: Callable[[], Connection], payload: bytes, on_kill: Callable[[], None])`; `stdout`, `returncode`, `poll()`, `wait(timeout=None)`, `kill()`; property `error: BaseException | None` (set when the stream ended without a status frame or the connection failed).
  - `Connection` is any object with `send(bytes)`, `recv(timeout: float | None) -> bytes`, `close()` — `websockets.sync.client.ClientConnection` satisfies it.
  - `def connect_exec(client: KubeClient, pod: str, container: str, argv: list[str]) -> Connection` — opens `client.exec_url(...)` with `subprotocols=[SUBPROTOCOL]`, auth headers, ssl context, `ping_interval=30`.

- [ ] **Step 1: Failing tests** — `tests/test_exec_stream.py`

```python
import json
import queue
import threading

import pytest
from kubernetes_sandbox.exec_stream import (CLOSE, ERROR, STDERR, STDIN, STDOUT, ExecDisconnected, ExecFailed,
                                            StreamingExecHandle, close_stdin_frame, encode_frame, parse_status)


class FakeConn:
    """Scripted server: yields `frames` (bytes) then optionally raises."""

    def __init__(self, frames, end_with=None):
        self.sent, self._q, self.closed = [], queue.Queue(), False
        for f in frames:
            self._q.put(f)
        self._end = end_with

    def send(self, data):
        self.sent.append(data)

    def recv(self, timeout=None):
        try:
            return self._q.get(timeout=0.05)
        except queue.Empty:
            raise self._end or TimeoutError()

    def close(self):
        self.closed = True


def _status(code):
    if code == 0:
        return encode_frame(ERROR, json.dumps({"status": "Success"}).encode())
    return encode_frame(ERROR, json.dumps({"status": "Failure", "reason": "NonZeroExitCode", "details": {
        "causes": [{"reason": "ExitCode", "message": str(code)}]}}).encode())


def test_frame_codec():
    assert encode_frame(STDIN, b"hi") == b"\x00hi"
    assert close_stdin_frame() == bytes([CLOSE, STDIN])


def test_parse_status():
    assert parse_status(json.dumps({"status": "Success"}).encode()) == 0
    assert parse_status(_status(42)[1:]) == 42
    with pytest.raises(ExecFailed, match="container not found"):
        parse_status(json.dumps({"status": "Failure", "message": "container not found"}).encode())


def test_handle_streams_output_and_sends_payload_then_close():
    conn = FakeConn([encode_frame(STDOUT, b"one\n"), encode_frame(STDERR, b"err\n"),
                     encode_frame(STDOUT, b"two\n"), _status(3)], end_with=ConnectionError("closed"))
    h = StreamingExecHandle(lambda: conn, b"PAYLOAD", on_kill=lambda: None)
    assert h.wait(timeout=5) == 3
    assert h.stdout.read() == "one\nerr\ntwo\n"
    assert conn.sent[0] == encode_frame(STDIN, b"PAYLOAD") and conn.sent[-1] == close_stdin_frame()
    assert h.error is None and conn.closed


def test_handle_raises_connection_error_without_status():
    conn = FakeConn([encode_frame(STDOUT, b"partial")], end_with=ConnectionError("reset"))
    h = StreamingExecHandle(lambda: conn, b"", on_kill=lambda: None)
    h.wait(timeout=5)
    assert isinstance(h.error, ExecDisconnected)
    assert h.returncode is not None and h.returncode != 0


def test_large_payload_is_chunked():
    conn = FakeConn([_status(0)], end_with=ConnectionError())
    StreamingExecHandle(lambda: conn, b"x" * 200_000, on_kill=lambda: None).wait(timeout=5)
    data = b"".join(f[1:] for f in conn.sent if f[:1] == bytes([STDIN]))
    assert data == b"x" * 200_000 and all(len(f) <= 64 * 1024 + 1 for f in conn.sent)


def test_kill_calls_on_kill_and_closes():
    killed = threading.Event()
    conn = FakeConn([], end_with=None)  # never ends on its own
    h = StreamingExecHandle(lambda: conn, b"", on_kill=killed.set)
    h.kill()
    assert killed.is_set()
    assert h.wait(timeout=5) is not None and conn.closed
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — `kubernetes_sandbox/exec_stream.py`

```python
"""Kubernetes exec over websocket (v5.channel.k8s.io) behind Hermes's ProcessHandle protocol."""
from __future__ import annotations

import json
import os
import threading
from typing import Callable

STDIN, STDOUT, STDERR, ERROR, CLOSE = 0, 1, 2, 3, 255
SUBPROTOCOL = "v5.channel.k8s.io"
_CHUNK = 64 * 1024


class ExecFailed(RuntimeError):
    pass


class ExecDisconnected(RuntimeError):
    pass


def encode_frame(channel: int, data: bytes) -> bytes:
    return bytes([channel]) + data


def close_stdin_frame() -> bytes:
    return bytes([CLOSE, STDIN])


def parse_status(payload: bytes) -> int:
    st = json.loads(payload.decode("utf-8", "replace") or "{}")
    if st.get("status") == "Success":
        return 0
    for cause in (st.get("details") or {}).get("causes") or []:
        if cause.get("reason") == "ExitCode":
            return int(cause.get("message", "1"))
    raise ExecFailed(st.get("message") or "exec failed")


class StreamingExecHandle:
    def __init__(self, connect: Callable[[], object], payload: bytes, on_kill: Callable[[], None]):
        self._on_kill = on_kill
        self._done = threading.Event()
        self._killed = threading.Event()
        self._returncode: int | None = None
        self.error: BaseException | None = None
        self._conn = None
        r, w = os.pipe()
        self._stdout = os.fdopen(r, "r", encoding="utf-8", errors="replace")
        self._w = w
        threading.Thread(target=self._run, args=(connect, payload), daemon=True).start()

    @property
    def stdout(self):
        return self._stdout

    @property
    def returncode(self):
        return self._returncode

    def _write(self, data: bytes) -> None:
        try:
            os.write(self._w, data)
        except OSError:
            pass

    def _run(self, connect, payload: bytes) -> None:
        try:
            self._conn = conn = connect()
            for i in range(0, len(payload), _CHUNK):
                conn.send(encode_frame(STDIN, payload[i:i + _CHUNK]))
            conn.send(close_stdin_frame())
            while True:
                try:
                    frame = conn.recv(timeout=1.0)
                except TimeoutError:
                    if self._killed.is_set():
                        self._returncode = 137
                        return
                    continue
                if isinstance(frame, str):
                    frame = frame.encode()
                if not frame:
                    continue
                ch, data = frame[0], frame[1:]
                if ch in (STDOUT, STDERR):
                    self._write(data)
                elif ch == ERROR:
                    self._returncode = parse_status(data)
                    return
        except ExecFailed as e:
            self.error, self._returncode = e, 126
        except Exception as e:  # connection reset, handshake failure, closed without status
            self.error = ExecDisconnected(str(e) or type(e).__name__)
            self._returncode = 137 if self._killed.is_set() else 255
        finally:
            try:
                os.close(self._w)
            except OSError:
                pass
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._done.set()

    def poll(self):
        return self._returncode if self._done.is_set() else None

    def wait(self, timeout=None):
        self._done.wait(timeout=timeout)
        return self._returncode

    def kill(self):
        self._killed.set()
        try:
            self._on_kill()
        finally:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass


def connect_exec(client, pod: str, container: str, argv: list[str]):
    from websockets.sync.client import connect
    ctx = client.ssl_context()
    return connect(client.exec_url(pod, container, argv), subprotocols=[SUBPROTOCOL],
                   additional_headers=client.auth_headers(), ssl=ctx if not isinstance(ctx, bool) else None,
                   ping_interval=30, max_size=None, open_timeout=30)
```

Note: `test_kill_calls_on_kill_and_closes` relies on `recv` raising `TimeoutError` after the kill and the loop returning 137; if the fake's `recv` raises `TimeoutError()` (its `end_with=None` path), the loop notices `_killed` within 1 s.

- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** — `git add kubernetes_sandbox/exec_stream.py tests/test_exec_stream.py && git commit -m "feat: v5 exec websocket framing and streaming ProcessHandle"`

---

### Task 7: KubernetesEnvironment — adopt/create, exec, recreate, cleanup

**Files:**
- Create: `kubernetes_sandbox/environment.py`, `tests/test_environment.py`, `tests/fakes.py`

**Interfaces:**
- Consumes: Tasks 2–6; Hermes `BaseEnvironment`, `EnvironmentConnectionError(reason, retry_hint=...)`, `get_activity_callback()`.
- Produces:
  - `class KubernetesEnvironment(BaseEnvironment)`: `__init__(self, *, settings, identity, cwd, timeout, client=None, exec_connect=None, clock=time.time, sleep=time.sleep)`; `_run_bash(cmd_string, *, login=False, timeout=120, stdin_data=None) -> ProcessHandle`; `cleanup()`; `_before_execute()`.
  - Module flag `_PROCESS_EXITING` set by an `atexit` hook registered at import; `cleanup()` detaches instead of deleting when it is set.
  - `_stdin_mode = "pipe"`, `_snapshot_timeout = 60`.

- [ ] **Step 1: Test fakes** — `tests/fakes.py`

```python
import copy

from kubernetes_sandbox.kube import NotFound, QuotaExceeded


class FakeKube:
    """In-memory Pod store. `ready_after` = get_pod calls before a created Pod reports Ready."""

    def __init__(self, ready_after=1, quota=None):
        self.pods, self.calls, self.ready_after, self.quota = {}, [], ready_after, quota
        self._gets = {}

    def _ready(self, pod):
        pod.setdefault("status", {})
        pod["status"].update({"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]})

    def get_pod(self, name):
        self.calls.append(("get", name))
        if name not in self.pods:
            raise NotFound(404, "NotFound", name)
        self._gets[name] = self._gets.get(name, 0) + 1
        if self._gets[name] >= self.ready_after:
            self._ready(self.pods[name])
        return copy.deepcopy(self.pods[name])

    def create_pod(self, manifest):
        self.calls.append(("create", manifest["metadata"]["name"]))
        if self.quota is not None and len(self.pods) >= self.quota:
            raise QuotaExceeded(403, "Forbidden", "exceeded quota: sandbox-capacity")
        pod = copy.deepcopy(manifest)
        pod["status"] = {"phase": "Pending"}
        self.pods[manifest["metadata"]["name"]] = pod
        self._gets[manifest["metadata"]["name"]] = 0
        return copy.deepcopy(pod)

    def delete_pod(self, name, grace_seconds=10):
        self.calls.append(("delete", name))
        self.pods.pop(name, None)

    def list_pods(self, label_selector):
        k, _, v = label_selector.partition(",")[2].partition("=") if "," in label_selector else ("", "", "")
        return [copy.deepcopy(p) for p in self.pods.values()
                if not k or p["metadata"]["labels"].get(k) == v]

    def patch_pod_annotations(self, name, annotations):
        self.calls.append(("patch", name))
        self.pods[name]["metadata"]["annotations"].update(annotations)


class FakeExec:
    """exec_connect replacement: runs nothing; returns a handle with scripted results."""

    def __init__(self):
        self.runs = []
        self.fail_with_notfound_once = False

    def __call__(self, pod, container, argv, payload, on_kill):
        from kubernetes_sandbox.exec_stream import StreamingExecHandle, encode_frame, ERROR
        import json
        self.runs.append({"pod": pod, "container": container, "argv": argv, "payload": payload})
        if self.fail_with_notfound_once:
            self.fail_with_notfound_once = False
            raise NotFound(404, "NotFound", pod)

        class Conn:
            def __init__(self):
                self.frames = [b"\x01ok\n", encode_frame(ERROR, json.dumps({"status": "Success"}).encode())]

            def send(self, d):
                pass

            def recv(self, timeout=None):
                if self.frames:
                    return self.frames.pop(0)
                raise ConnectionError("closed")

            def close(self):
                pass

        return StreamingExecHandle(lambda: Conn(), payload, on_kill)
```

- [ ] **Step 2: Failing tests** — `tests/test_environment.py`

```python
import pytest
from fakes import FakeExec, FakeKube
from kubernetes_sandbox import environment as envmod
from kubernetes_sandbox.environment import KubernetesEnvironment
from kubernetes_sandbox.naming import ANN_SESSION_KEY, Identity
from kubernetes_sandbox.podspec import render_pod
from kubernetes_sandbox.settings import Settings
from tools.environments.base import EnvironmentConnectionError

S = Settings(runtime_class_name="kata", docker_disk_storage_class="b", ready_timeout_seconds=5)
ID = Identity("mars", "sess-1")


def _env(kube=None, ex=None, settings=S, ident=ID):
    kube, ex = kube or FakeKube(), ex or FakeExec()
    e = KubernetesEnvironment(settings=settings, identity=ident, cwd="/workspace", timeout=30,
                              client=kube, exec_connect=ex, sleep=lambda s: None)
    return e, kube, ex


def test_creates_pod_and_waits_ready():
    e, kube, _ = _env()
    e.ensure_pod()
    assert ("create", ID.pod_name) in kube.calls
    assert kube.pods[ID.pod_name]["status"]["phase"] == "Running"


def test_adopts_matching_running_pod():
    kube = FakeKube(ready_after=0)
    kube.create_pod(render_pod(S, ID, now_iso="2026-01-01T00:00:00Z", created_by="old"))
    kube.calls.clear()
    e, _, _ = _env(kube=kube)
    e.ensure_pod()
    assert not any(c[0] in ("create", "delete") for c in kube.calls)


def test_replaces_mismatched_pod():
    kube = FakeKube(ready_after=0)
    foreign = render_pod(S, ID, now_iso="2026-01-01T00:00:00Z", created_by="old")
    foreign["metadata"]["annotations"][ANN_SESSION_KEY] = "someone-else"
    kube.create_pod(foreign)
    kube.calls.clear()
    e, _, _ = _env(kube=kube)
    e.ensure_pod()
    assert ("delete", ID.pod_name) in kube.calls and ("create", ID.pod_name) in kube.calls
    assert kube.pods[ID.pod_name]["metadata"]["annotations"][ANN_SESSION_KEY] == "sess-1"


def test_interlock_blocks_creation():
    bad = Settings(docker=True, docker_disk_storage_class="b")
    e, kube, _ = _env(settings=bad)
    with pytest.raises(EnvironmentConnectionError, match="root on the node"):
        e.ensure_pod()
    assert not any(c[0] == "create" for c in kube.calls)


def test_per_profile_cap():
    kube = FakeKube(ready_after=0)
    s = Settings(runtime_class_name="kata", docker_disk_storage_class="b", max_per_profile=1)
    kube.create_pod(render_pod(s, Identity("mars", "other"), now_iso="x", created_by="x"))
    e, _, _ = _env(kube=kube, settings=s)
    with pytest.raises(EnvironmentConnectionError, match="mars"):
        e.ensure_pod()


def test_quota_exhausted_raises_connection_error():
    e, _, _ = _env(kube=FakeKube(quota=0))
    with pytest.raises(EnvironmentConnectionError, match="capacity exhausted") as ei:
        e.ensure_pod()
    assert "idle" in ei.value.retry_hint


def test_run_bash_execs_runner_in_work_container():
    e, _, ex = _env()
    h = e._run_bash("echo hi", login=True, timeout=10, stdin_data="IN")
    assert h.wait(timeout=5) == 0
    run = ex.runs[-1]
    assert run["container"] == "work" and run["argv"][:2] == ["bash", "-c"] and run["argv"][-2:] == ["1", "10"]
    assert run["payload"].endswith(b"echo hiIN")


def test_recreates_pod_on_404_and_marks_recreated():
    e, kube, ex = _env()
    e.ensure_pod()
    kube.pods.clear()          # janitor deleted it
    ex.fail_with_notfound_once = True
    h = e._run_bash("true", timeout=10)
    assert h.wait(timeout=5) == 0
    assert kube.calls.count(("create", ID.pod_name)) == 2
    assert getattr(e, "_recreated_notice_pending", False) is True


def test_cleanup_deletes_but_detaches_on_process_exit(monkeypatch):
    e, kube, _ = _env()
    e.ensure_pod()
    monkeypatch.setattr(envmod, "_PROCESS_EXITING", True)
    e.cleanup()
    assert ID.pod_name in kube.pods
    monkeypatch.setattr(envmod, "_PROCESS_EXITING", False)
    e2, _, _ = _env(kube=kube)
    e2.ensure_pod()
    e2.cleanup()
    assert ID.pod_name not in kube.pods


def test_last_used_patch_is_throttled():
    e, kube, _ = _env()
    e.ensure_pod()
    ticks = iter([1000.0, 1001.0, 1070.0])
    e._clock, e._last_touch = (lambda: next(ticks)), 0.0
    for _ in range(3):
        e._touch_last_used()
    assert kube.calls.count(("patch", ID.pod_name)) == 2


def test_dropped_stream_raises_environment_connection_error():
    from kubernetes_sandbox.exec_stream import StreamingExecHandle

    class DropConn:
        def send(self, d):
            pass

        def recv(self, timeout=None):
            raise ConnectionError("reset by peer")

        def close(self):
            pass

    def dropping_exec(pod, container, argv, payload, on_kill):
        return StreamingExecHandle(lambda: DropConn(), payload, on_kill)

    e, _, _ = _env(ex=dropping_exec)
    e.ensure_pod()
    e._snapshot_ready = True
    with pytest.raises(EnvironmentConnectionError, match="exec stream"):
        e.execute("echo hi")
```

Add `tests/__init__.py`? No — `fakes` is imported via `pythonpath`; add `"tests"` to `[tool.pytest.ini_options] pythonpath = [".", "tests"]` in `pyproject.toml`.

- [ ] **Step 3: Run — expect FAIL.**

- [ ] **Step 4: Implement** — `kubernetes_sandbox/environment.py`

```python
"""Hermes BaseEnvironment backed by one Kubernetes Pod per session."""
from __future__ import annotations

import atexit
import datetime as dt
import os
import socket
import time
import uuid

from tools.environments.base import BaseEnvironment, EnvironmentConnectionError, get_activity_callback

from .exec_stream import ExecDisconnected, ExecFailed, StreamingExecHandle, connect_exec
from .kube import KubeClient, KubeError, NotFound, QuotaExceeded
from .naming import ANN_LAST_USED, ANN_SESSION_KEY, LBL_PROFILE, LBL_TEMPLATE_HASH, MANAGED_BY, Identity, slug
from .podspec import render_pod, template_hash
from .runner import exec_argv, kill_argv, stdin_payload
from .settings import Settings, interlock_error

_PROCESS_EXITING = False


def _mark_exiting():
    global _PROCESS_EXITING
    _PROCESS_EXITING = True


atexit.register(_mark_exiting)  # registered after Hermes's own hooks, so it runs before them (LIFO)

_LAST_USED_EVERY = 60.0


def _now_iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ready(pod: dict) -> bool:
    st = pod.get("status") or {}
    return st.get("phase") == "Running" and any(
        c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions") or [])


def _default_exec_connect(client):
    def run(pod, container, argv, payload, on_kill):
        client.get_pod(pod)  # a deleted Pod surfaces here as NotFound, so _run_bash can recreate it
        return StreamingExecHandle(lambda: connect_exec(client, pod, container, argv), payload, on_kill)
    return run


class KubernetesEnvironment(BaseEnvironment):
    _stdin_mode = "pipe"
    _snapshot_timeout = 60

    def __init__(self, *, settings: Settings, identity: Identity, cwd: str, timeout: int,
                 client=None, exec_connect=None, clock=time.time, sleep=time.sleep):
        super().__init__(cwd=cwd or "/workspace", timeout=timeout)
        self._s, self._id = settings, identity
        self._client = client or KubeClient(settings)
        self._exec = exec_connect or _default_exec_connect(self._client)
        self._clock, self._sleep = clock, sleep
        self._pod_ready = False
        self._last_touch = 0.0
        self._last_handle = None

    # --- Pod lifecycle -------------------------------------------------------
    def _notify(self, msg: str) -> None:
        cb = get_activity_callback()
        if cb:
            try:
                cb(msg)
            except Exception:
                pass

    def _matches(self, pod: dict) -> bool:
        md = pod.get("metadata") or {}
        return ((md.get("annotations") or {}).get(ANN_SESSION_KEY) == self._id.session_key
                and (md.get("labels") or {}).get(LBL_TEMPLATE_HASH) == template_hash(self._s)
                and (pod.get("status") or {}).get("phase") not in ("Failed", "Succeeded")
                and not md.get("deletionTimestamp"))

    def _wait_gone(self, name: str) -> None:
        deadline = self._clock() + 60
        while self._clock() < deadline:
            try:
                self._client.get_pod(name)
            except NotFound:
                return
            self._sleep(1)

    def _check_capacity(self) -> None:
        sel = ",".join(f"{k}={v}" for k, v in MANAGED_BY.items()) + f",{LBL_PROFILE}={slug(self._id.profile, 63)}"
        live = [p["metadata"]["name"] for p in self._client.list_pods(sel)
                if p["metadata"]["name"] != self._id.pod_name]
        if len(live) >= self._s.max_per_profile:
            raise EnvironmentConnectionError(
                f"Sandbox limit reached for profile {self._id.profile!r}: {len(live)} live sandboxes "
                f"({', '.join(sorted(live))}); max_per_profile={self._s.max_per_profile}.",
                retry_hint="Retry after an idle sandbox is reaped, or close another session.")

    def ensure_pod(self) -> None:
        msg = interlock_error(self._s)
        if msg:
            raise EnvironmentConnectionError(msg, retry_hint="Fix kubernetes-sandbox.yaml; retrying will not help.")
        name = self._id.pod_name
        try:
            pod = self._client.get_pod(name)
            if self._matches(pod):
                if not _ready(pod):
                    self._wait_ready(name)
                self._pod_ready = True
                return
            self._client.delete_pod(name, grace_seconds=0)
            self._wait_gone(name)
        except NotFound:
            pass
        self._check_capacity()
        manifest = render_pod(self._s, self._id, now_iso=_now_iso(self._clock()),
                              created_by=f"{socket.gethostname()}:{os.getpid()}")
        try:
            self._client.create_pod(manifest)
        except QuotaExceeded as e:
            raise EnvironmentConnectionError(
                f"Sandbox capacity exhausted in namespace {self._s.namespace}: {e}",
                retry_hint="Retry after an idle session's sandbox is reaped, or ask the operator to raise the quota.")
        except KubeError as e:
            raise EnvironmentConnectionError(f"Could not create sandbox Pod {name}: {e}")
        self._notify("starting sandbox Pod")
        self._wait_ready(name)
        self._pod_ready = True
        self._last_touch = self._clock()

    def _wait_ready(self, name: str) -> None:
        deadline = self._clock() + self._s.ready_timeout_seconds
        last = {}
        while self._clock() < deadline:
            last = self._client.get_pod(name)
            if _ready(last):
                return
            if (last.get("status") or {}).get("phase") in ("Failed", "Succeeded"):
                break
            self._notify("waiting for sandbox Pod")
            self._sleep(2)
        conds = [f"{c.get('type')}={c.get('status')} {c.get('message', '')}".strip()
                 for c in (last.get("status") or {}).get("conditions") or []]
        raise EnvironmentConnectionError(
            f"Sandbox Pod {name} not ready after {self._s.ready_timeout_seconds}s: "
            f"phase={(last.get('status') or {}).get('phase')} {'; '.join(conds)}")

    def _touch_last_used(self) -> None:
        now = self._clock()
        if now - self._last_touch < _LAST_USED_EVERY:
            return
        try:
            self._client.patch_pod_annotations(self._id.pod_name, {ANN_LAST_USED: _now_iso(now)})
            self._last_touch = now
        except KubeError:
            pass

    # --- BaseEnvironment ------------------------------------------------------
    def _before_execute(self) -> None:
        if not self._pod_ready:
            self.ensure_pod()
        self._touch_last_used()

    def _start(self, cmd_string, login, timeout, stdin_data):
        exec_id = uuid.uuid4().hex[:12]
        pod = self._id.pod_name

        def on_kill():
            try:
                self._exec(pod, "work", kill_argv(exec_id), b"", lambda: None).wait(timeout=15)
            except Exception:
                pass

        handle = self._exec(pod, "work", exec_argv(exec_id, login=login, timeout=timeout),
                            stdin_payload(cmd_string, stdin_data), on_kill)
        self._last_handle = handle
        return handle

    def _run_bash(self, cmd_string, *, login=False, timeout=120, stdin_data=None):
        if not self._pod_ready:
            self.ensure_pod()
        try:
            return self._start(cmd_string, login, timeout, stdin_data)
        except NotFound:
            self._pod_ready = False
            self.ensure_pod()
            self._mark_recreated()
            return self._start(cmd_string, login, timeout, stdin_data)

    def execute(self, command, cwd="", **kwargs):
        self._last_handle = None
        result = super().execute(command, cwd, **kwargs)
        h = self._last_handle
        err = getattr(h, "error", None) if h is not None else None
        if isinstance(err, (ExecDisconnected, ExecFailed)) and not getattr(h, "_killed").is_set():
            if isinstance(err, ExecDisconnected):
                self._pod_ready = False  # re-check the Pod (and recreate it if gone) on the next command
            raise EnvironmentConnectionError(
                f"Lost the exec stream to sandbox {self._id.pod_name}: {err}",
                retry_hint="Retry the command; if the sandbox Pod was lost it is recreated automatically.")
        return result

    def cleanup(self):
        if _PROCESS_EXITING:
            return  # detach: the next gateway adopts the Pod, or the janitor reaps it
        try:
            self._client.delete_pod(self._id.pod_name, grace_seconds=10)
        except Exception:
            pass
        self._pod_ready = False
```

- [ ] **Step 5: Run — expect PASS** (`.venv/bin/pytest tests/test_environment.py -v`).
- [ ] **Step 6: Run the whole suite** (`.venv/bin/pytest -v && .venv/bin/ruff check .`).
- [ ] **Step 7: Commit** — `git add kubernetes_sandbox/environment.py tests/test_environment.py tests/fakes.py pyproject.toml && git commit -m "feat: KubernetesEnvironment with adopt/create/recreate and detach-on-exit"`

---

### Task 8: KubernetesProvider and real Hermes registration

**Files:**
- Modify: `kubernetes_sandbox/provider.py` (replace the Task 1 stub)
- Create: `tests/test_provider.py`

**Interfaces:**
- Consumes: Tasks 2, 3, 7; Hermes `TerminalEnvironmentProvider`, `hermes_constants.get_hermes_home`, `profile_name_for_home`.
- Produces: `class KubernetesProvider(TerminalEnvironmentProvider)` with `name="kubernetes"`, `display_name="Kubernetes"`, `is_remote=True`, `is_container=True`, `session_isolated_when_nonpersistent=True`, `cache_path_base=None` (inherited default; no file sync in v1); `skip_container_guards=False` (approvals stay on in v1 — design K12 makes it conditional on credential forwarding, which v1 does not implement); `env_description="a Kubernetes sandbox Pod (Linux)"`.

- [ ] **Step 1: Failing tests** — `tests/test_provider.py`

```python
import pytest
from kubernetes_sandbox.provider import KubernetesProvider


def _write(home, text):
    (home / "kubernetes-sandbox.yaml").write_text(text)


def test_attributes():
    p = KubernetesProvider()
    assert p.name == "kubernetes" and p.is_remote and p.is_container
    assert p.skip_container_guards is False
    assert p.session_isolated_when_nonpersistent is True


def test_is_available_needs_settings_and_token_without_network(_isolated_hermes_home, tmp_path):
    p = KubernetesProvider()
    assert p.is_available() is False  # no settings file
    tok = tmp_path / "tok"
    tok.write_text("x")
    _write(_isolated_hermes_home, f"docker: false\ntoken_path: {tok}\n")
    assert p.is_available() is True


def test_check_requirements_reports_interlock(_isolated_hermes_home, tmp_path, caplog):
    tok = tmp_path / "tok"
    tok.write_text("x")
    _write(_isolated_hermes_home, f"docker_disk_storage_class: b\ntoken_path: {tok}\n")
    assert KubernetesProvider().check_requirements({}) is False
    assert "root on the node" in caplog.text


def test_probe_never_raises_on_bad_settings(_isolated_hermes_home):
    _write(_isolated_hermes_home, "nonsense_key: 1\n")
    status, detail = KubernetesProvider().probe()
    assert status == "needs_setup" and "nonsense_key" in detail


def test_create_environment_uses_profile_and_task_id(_isolated_hermes_home, tmp_path, monkeypatch):
    tok = tmp_path / "tok"
    tok.write_text("x")
    _write(_isolated_hermes_home, f"docker: false\ntoken_path: {tok}\n")
    env = KubernetesProvider().create_environment(cwd="/workspace", timeout=60, task_id="thread-7", image=None,
                                                  container_config={}, unknown_future_kwarg=1)
    assert env._id.session_key == "thread-7"
    assert env._id.profile == "default"


def test_name_is_valid_and_not_a_builtin():
    import re
    from agent.terminal_env_registry import _reject_builtin_collision
    _reject_builtin_collision(KubernetesProvider.name)  # raises on a built-in name
    assert re.fullmatch(r"[a-z0-9_]+", KubernetesProvider.name)
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — `kubernetes_sandbox/provider.py`

```python
"""Hermes terminal backend provider: `terminal.backend: kubernetes`."""
from __future__ import annotations

import logging
import pathlib

from agent.terminal_env_provider import TerminalEnvironmentProvider

from .naming import Identity
from .settings import SettingsError, interlock_error, load_settings

log = logging.getLogger(__name__)


def _home() -> pathlib.Path:
    from hermes_constants import get_hermes_home
    return pathlib.Path(get_hermes_home())


def _profile(home: pathlib.Path) -> str:
    try:
        from hermes_constants import profile_name_for_home
        return profile_name_for_home(home) or "default"
    except Exception:
        return "default"


class KubernetesProvider(TerminalEnvironmentProvider):
    name = "kubernetes"
    display_name = "Kubernetes"
    is_remote = True
    is_container = True
    session_isolated_when_nonpersistent = True

    @property
    def description(self) -> str:
        return "Run each session's commands in its own Kubernetes Pod (optional Docker sidecar)."

    @property
    def skip_container_guards(self) -> bool:
        return False

    @property
    def env_description(self) -> str:
        return "a Kubernetes sandbox Pod (Linux)"

    def _settings(self):
        return load_settings(_home())

    def is_available(self) -> bool:
        try:
            if not (_home() / "kubernetes-sandbox.yaml").exists():
                return False
            s = self._settings()
            return pathlib.Path(s.token_path).exists()
        except Exception:
            return False

    def check_requirements(self, config) -> bool:
        try:
            s = self._settings()
        except SettingsError as e:
            log.error("kubernetes sandbox: %s", e)
            return False
        msg = interlock_error(s)
        if msg:
            log.error(msg)
            return False
        if not pathlib.Path(s.token_path).exists():
            log.error("kubernetes sandbox: no API token at %s", s.token_path)
            return False
        return True

    def probe(self):
        try:
            s = self._settings()
        except Exception as e:
            return ("needs_setup", str(e))
        msg = interlock_error(s)
        if msg:
            return ("needs_setup", msg)
        return ("ready", f"namespace {s.namespace}") if self.is_available() else (
            "needs_setup", f"no API token at {s.token_path}")

    def setup_instructions(self):
        return ["Write $HERMES_HOME/kubernetes-sandbox.yaml (see examples/kata/README.md),",
                "mount a ServiceAccount token for the sandbox namespace, then: hermes config set terminal.backend kubernetes"]

    def create_environment(self, *, cwd, timeout, task_id="default", image=None, container_config=None, **kwargs):
        from .environment import KubernetesEnvironment
        home = _home()
        s = self._settings()
        if image:
            import dataclasses
            s = dataclasses.replace(s, work_image=image)
        return KubernetesEnvironment(settings=s, identity=Identity(_profile(home), task_id or "default"),
                                     cwd=cwd or "/workspace", timeout=timeout)
```

- [ ] **Step 4: Run — expect PASS** (whole suite: `.venv/bin/pytest -v`).
- [ ] **Step 5: Commit** — `git add kubernetes_sandbox/provider.py tests/test_provider.py && git commit -m "feat: KubernetesProvider registered as terminal.backend kubernetes"`

---

### Task 9: Janitor

**Files:**
- Create: `janitor/janitor.py`, `tests/test_janitor.py`

**Interfaces:**
- Produces: `janitor.py` CLI (stdlib only, Python ≥ 3.11): env `NAMESPACE` (default `agent-sandbox`), `IDLE_SECONDS` (default 3600), `DRY_RUN` (`1` = report only); in-cluster token/CA; function `decide(pod: dict, now: float, idle_seconds: int) -> str | None` returning a reason (`"idle"`, `"max-lifetime"`, `"finished"`) or `None`; `main()` lists managed Pods (`app.kubernetes.io/managed-by=hermes-kubernetes-sandbox`), deletes those with a reason, prints one line per decision, exits 0.

- [ ] **Step 1: Failing tests** — `tests/test_janitor.py`

```python
import importlib.util
import pathlib

spec = importlib.util.spec_from_file_location("janitor", pathlib.Path(__file__).parents[1] / "janitor/janitor.py")
janitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(janitor)

NOW = 1_000_000.0


def pod(last_used_age=0, created_age=0, phase="Running", max_life=86400):
    iso = janitor.iso
    return {"metadata": {"name": "p", "creationTimestamp": iso(NOW - created_age),
                         "annotations": {"hermes-sandbox/last-used": iso(NOW - last_used_age),
                                         "hermes-sandbox/max-lifetime-seconds": str(max_life)}},
            "status": {"phase": phase}}


def test_keeps_fresh_pod():
    assert janitor.decide(pod(10, 10), NOW, 3600) is None


def test_idle_pod_reaped():
    assert janitor.decide(pod(4000, 4000), NOW, 3600) == "idle"


def test_max_lifetime_beats_activity():
    assert janitor.decide(pod(5, 90000), NOW, 3600) == "max-lifetime"


def test_finished_pods_reaped():
    assert janitor.decide(pod(phase="Failed"), NOW, 3600) == "finished"


def test_missing_annotations_fall_back_to_creation_time():
    p = pod(created_age=5000)
    p["metadata"]["annotations"] = {}
    assert janitor.decide(p, NOW, 3600) == "idle"
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — `janitor/janitor.py`

```python
#!/usr/bin/env python3
"""Reap Hermes sandbox Pods: idle too long, past max lifetime, or finished. Stdlib only."""
from __future__ import annotations

import datetime as dt
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
SELECTOR = "app.kubernetes.io/managed-by=hermes-kubernetes-sandbox"


def iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(s: str | None) -> float | None:
    if not s:
        return None
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


def decide(pod: dict, now: float, idle_seconds: int) -> str | None:
    md, ann = pod.get("metadata") or {}, (pod.get("metadata") or {}).get("annotations") or {}
    if (pod.get("status") or {}).get("phase") in ("Failed", "Succeeded"):
        return "finished"
    created = parse(md.get("creationTimestamp")) or now
    max_life = int(ann.get("hermes-sandbox/max-lifetime-seconds", "86400"))
    if now - created > max_life:
        return "max-lifetime"
    last = parse(ann.get("hermes-sandbox/last-used")) or created
    if now - last > idle_seconds:
        return "idle"
    return None


def _api(method: str, path: str, ctx, token: str, body: dict | None = None) -> dict:
    url = "https://kubernetes.default.svc" + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.loads(r.read() or b"{}")


def main() -> int:
    ns = os.environ.get("NAMESPACE", "agent-sandbox")
    idle = int(os.environ.get("IDLE_SECONDS", "3600"))
    dry = os.environ.get("DRY_RUN") == "1"
    token = open(f"{SA}/token").read().strip()
    ctx = ssl.create_default_context(cafile=f"{SA}/ca.crt")
    q = urllib.parse.urlencode({"labelSelector": SELECTOR})
    pods = _api("GET", f"/api/v1/namespaces/{ns}/pods?{q}", ctx, token).get("items", [])
    now = time.time()
    for p in pods:
        name, reason = p["metadata"]["name"], decide(p, now, idle)
        if reason is None:
            print(f"keep {name}")
            continue
        print(f"{'would delete' if dry else 'delete'} {name}: {reason}")
        if not dry:
            _api("DELETE", f"/api/v1/namespaces/{ns}/pods/{name}", ctx, token, {"gracePeriodSeconds": 10})
    print(f"checked {len(pods)} sandbox pods")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** — `git add janitor tests/test_janitor.py && git commit -m "feat: janitor reaps idle, expired and finished sandbox Pods"`

---

### Task 10: Examples — Kata happy path and runc-no-docker, validated in CI

**Files:**
- Create: `examples/kata/{namespace,runtimeclass,admission-policy,networkpolicy,rbac,resourcequota,profile-configmap,janitor}.yaml`, `examples/kata/kata-node-config.md`, `examples/kata/kubernetes-sandbox.yaml` (sample plugin settings), `examples/kata/README.md`, `examples/runc-no-docker/{namespace,rbac,networkpolicy,resourcequota}.yaml`, `examples/runc-no-docker/kubernetes-sandbox.yaml`, `examples/runc-no-docker/README.md`, `scripts/validate-examples.sh`

**Interfaces:**
- Consumes: label/annotation names (Task 3), janitor env (Task 9), settings keys (Task 2).
- Produces: manifests that the e2e (Task 11) applies.

Content requirements (copy values exactly):
- `namespace.yaml`: `agent-sandbox`, labels `pod-security.kubernetes.io/enforce: privileged`, `audit: restricted`, `warn: restricted`; header comment explaining why privileged is required and that the VAP is the compensating control.
- `runtimeclass.yaml`: `kata`, handler `kata`, `overhead.podFixed: {memory: 1024Mi, cpu: 250m}`, comment with the derivation (512 MiB `default_memory` + 512 MiB host-side headroom; soak peak 1474/2560 MiB) and "keep in step with default_memory".
- `admission-policy.yaml`: the homelab `agent-sandbox-pods` VAP verbatim (runtimeClassName == kata; no hostPath/hostNetwork/hostPID/hostIPC/hostPort; `automountServiceAccountToken` false; no `io.katacontainers.*` annotations), plus: `privileged` only on container `dind` — expression:
  `object.spec.containers.all(c, c.name == 'dind' || !has(c.securityContext) || !has(c.securityContext.privileged) || !c.securityContext.privileged) && (!has(object.spec.initContainers) || object.spec.initContainers.all(c, !has(c.securityContext) || !has(c.securityContext.privileged) || !c.securityContext.privileged))`.
- `networkpolicy.yaml`: a portable `networking.k8s.io/v1` NetworkPolicy (deny all ingress; egress DNS to kube-system:53 and 0.0.0.0/0 except RFC1918+169.254 on 80/443) and, in comments, the Cilium variant and the mandatory `socketLB.hostNamespaceOnly: true`.
- `rbac.yaml`: SA `hermes-sandbox-driver` in the Hermes namespace (`hermes`; note "change to yours"), Role in `agent-sandbox` (pods create/get/list/watch/patch/delete, pods/exec create+get, pods/log get), RoleBinding; SA `hermes-sandbox-janitor` + Role (pods list/delete) + RoleBinding.
- `resourcequota.yaml`: `pods: "4"`, `requests.cpu: "4"`, `requests.memory: 10Gi`, `limits.memory: 11Gi`, `persistentvolumeclaims: "4"`, `requests.storage: 120Gi`, comment with per-session math (limits 1Gi + 512Mi + 1024Mi overhead = 2560Mi).
- `profile-configmap.yaml`: ConfigMap `hermes-sandbox-profile` in `agent-sandbox`, key `docker-cli.sh` with:
  `case ":$PATH:" in *:/opt/docker-cli/bin:*) ;; *) PATH="/opt/docker-cli/bin:$PATH"; export PATH ;; esac`
- `janitor.yaml`: ConfigMap `hermes-sandbox-janitor` (namespace `hermes`) holding `janitor.py` — generated by `scripts/validate-examples.sh --render-janitor` from `janitor/janitor.py` so it cannot drift (the script fails if the checked-in ConfigMap differs); CronJob `hermes-sandbox-janitor` every 5 minutes, `concurrencyPolicy: Forbid`, image `python:3.13-slim@sha256:<pin>` `# bump deliberately`, restricted securityContext (runAsNonRoot, uid 65534, drop ALL, RuntimeDefault seccomp, readOnlyRootFilesystem), requests 10m/32Mi, limit 64Mi, env `NAMESPACE=agent-sandbox`, `IDLE_SECONDS=3600`.
- `kata-node-config.md`: the two node-level changes with rationale and evidence — `default_memory = 512` and `block_device_cache_set = true` / `block_device_cache_direct = true` (spike soak 1 OOM from host double-caching; soak 2 PASS, cgroup peak 1474/2560 MiB, O_DIRECT fd flags `02140002`); how to apply on Talos (config copy in `/var` + containerd `ConfigPath`) and with `kata-deploy` (drop-in `configuration.toml`); "re-copy on every Kata upgrade".
- `kubernetes-sandbox.yaml` (Kata): `runtime_class_name: kata`, `docker: true`, `docker_disk_storage_class: <your RWO block StorageClass>`, `token_path: /var/run/secrets/hermes-sandbox/token`.
- runc-no-docker: same namespace with `enforce: restricted`, no RuntimeClass, settings `docker: false`, README noting the sandbox is an unprivileged Pod with no Docker.
- `README.md` (kata): prerequisites (K8s ≥ 1.33 for image volumes, else `docker_cli: init-copy`; Kata with Cloud Hypervisor; a Block-capable StorageClass), apply order, how to mount the driver SA token into the Hermes Pod via a projected volume on the gateway container only, first manual janitor run, and the measured numbers (cold start ~27 s, ~1.5× runc on test suites, guest ≈ 2 GiB).

- [ ] **Step 1: Write `scripts/validate-examples.sh`**

```bash
#!/usr/bin/env bash
# Validate every example manifest against a live API server (server-side dry run).
# CI runs it against kind; locally pass KUBECONFIG. --render-janitor regenerates
# examples/kata/janitor.yaml's ConfigMap from janitor/janitor.py.
set -euo pipefail
cd "$(dirname "$0")/.."
render_janitor() {
  python3 - <<'EOF'
import pathlib, yaml
src = pathlib.Path("janitor/janitor.py").read_text()
path = pathlib.Path("examples/kata/janitor.yaml")
docs = list(yaml.safe_load_all(path.read_text()))
for d in docs:
    if d and d.get("kind") == "ConfigMap":
        d["data"]["janitor.py"] = src
path.write_text("---\n".join(yaml.safe_dump(d, sort_keys=False) for d in docs if d))
EOF
}
if [ "${1:-}" = "--render-janitor" ]; then render_janitor; exit 0; fi
cp examples/kata/janitor.yaml /tmp/janitor.before.yaml
render_janitor
diff -q /tmp/janitor.before.yaml examples/kata/janitor.yaml || { echo "janitor ConfigMap drifted: run --render-janitor"; exit 1; }
kubectl create namespace hermes --dry-run=client -o yaml | kubectl apply -f -
for dir in examples/kata examples/runc-no-docker; do
  for f in "$dir"/*.yaml; do
    case "$f" in */kubernetes-sandbox.yaml) continue ;; esac
    kubectl apply --dry-run=server -f "$f" >/dev/null && echo "ok   $f"
  done
done
```

- [ ] **Step 2: Write all example files listed above.**
- [ ] **Step 3: Validate locally** — with the operator's dev kubeconfig (read-only dry run): `KUBECONFIG=<dev kubeconfig> ./scripts/validate-examples.sh`. Expected: every file `ok`. (kind in CI covers clusters without Kata: the RuntimeClass object validates without a handler.)
- [ ] **Step 4: Commit and push; confirm the CI `examples` job passes.**

```bash
git add examples scripts/validate-examples.sh && git commit -m "docs: Kata happy-path and runc-no-docker examples, validated against a live API server"
git push origin main
```

---

### Task 11: End-to-end on a real cluster

**Files:**
- Create: `scripts/e2e.py`

**Interfaces:**
- Consumes: everything above. Runs outside Hermes's gateway but inside its Python (the `.venv`), constructing `KubernetesEnvironment` directly.

- [ ] **Step 1: Write `scripts/e2e.py`**

```python
#!/usr/bin/env python3
"""Live end-to-end check. Usage: e2e.py <settings.yaml> — needs a token file named in the settings
(e.g. `kubectl -n hermes create token hermes-sandbox-driver > /tmp/sbx-token`)."""
import pathlib
import sys
import time

from kubernetes_sandbox.environment import KubernetesEnvironment
from kubernetes_sandbox.naming import Identity
from kubernetes_sandbox.settings import load_settings

home = pathlib.Path(sys.argv[1]).parent
s = load_settings(home)
env = KubernetesEnvironment(settings=s, identity=Identity("e2e", f"run-{int(time.time())}"),
                            cwd="/workspace", timeout=120)
results = []


def check(label, ok, detail=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + label + (f": {detail}" if detail else ""))


try:
    t0 = time.time()
    env.init_session()
    check("pod ready + session snapshot", True, f"{time.time() - t0:.1f}s")
    r = env.execute("mkdir -p sub && cd sub && export E2E=yes")
    r = env.execute('echo "$E2E $(pwd)"; a=(x y); echo ${a[1]} {1..3}')
    check("cd/export persist; real bash", "yes /workspace/sub" in r["output"] and "y 1 2 3" in r["output"], r["output"])
    big = "z" * 2_000_000
    r = env.execute("wc -c", stdin_data=big)
    check("2 MB stdin exact", r["output"].strip().endswith(str(len(big))), r["output"][-40:])
    r = env.execute("exit 9")
    check("exit code", r.get("returncode", r.get("exit_code")) == 9, str(r))
    if s.docker:
        r = env.execute("docker run --rm alpine:3.24.2 echo hello-from-inner", timeout=300)
        check("docker run", "hello-from-inner" in r["output"], r["output"][-200:])
    t0 = time.time()
    r = env.execute("sleep 120", timeout=5)
    check("timeout kills remote process", time.time() - t0 < 30, str(r)[:200])
    r = env.execute("pgrep -f 'sleep 120' || echo none")
    check("no leftover process", "none" in r["output"], r["output"])
    env._client.delete_pod(env._id.pod_name, grace_seconds=0)
    env._wait_gone(env._id.pod_name)
    r = env.execute("echo back")
    check("recreated after deletion", "back" in r["output"] and r.get("environment_recreated"), str(r)[:200])
finally:
    env.cleanup()
print(f"{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
```

- [ ] **Step 2: [LIVE — operator approval required]** Apply `examples/kata/` to the dev cluster (the homelab's `agent-sandbox` already has the RuntimeClass, VAP and network policy; apply only `profile-configmap.yaml`, `rbac.yaml` and `janitor.yaml` there, or a fresh namespace per the README), mint a short-lived token for the driver SA, write a settings file, run `.venv/bin/python scripts/e2e.py <settings.yaml>`. Expected: all checks PASS. Record the output in the PR/commit message.
- [ ] **Step 3: Commit** — `git add scripts/e2e.py && git commit -m "test: live end-to-end script"`

---

## Out of scope for this build (tracked in the design)

- File sync of skills/cache into the sandbox (`FileSyncManager`, design §3.4) — `cache_path_base` is `None` in v1.
- Credential forwarding (`forward_env`, K11) and the conditional `skip_container_guards` (K12) — approvals stay on.
- Size tiers (small/large) — one size from settings.
- Streaming background-process support beyond what `BaseEnvironment` already does via `execute`.
- Upstream catalog entry PR (after the homelab deployment proves it).
- A pinned-image allow-list in the example admission policy (design §4.3): digests are deployment-specific; the Kata README tells operators to add one.
- The homelab deployment itself (seeding the plugin into the Hermes StatefulSet, projected token, `forge-egress`) — a separate homelab plan.
