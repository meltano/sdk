from __future__ import annotations

from pathlib import Path

import yaml


def _test_workflow() -> dict:
    workflow_path = Path(__file__).parents[1] / ".github" / "workflows" / "test.yml"
    return yaml.load(workflow_path.read_text(), Loader=yaml.BaseLoader)


def test_external_calls_require_upstream_identity_and_all_secrets() -> None:
    external = _test_workflow()["jobs"]["tests-external"]
    guard = external["env"]["EXTERNAL_TESTS_ENABLED"]

    assert "github.repository == 'meltano/sdk'" in guard
    for secret in (
        "SAMPLE_TAP_GITLAB_AUTH_TOKEN",
        "SAMPLE_TAP_GITLAB_GROUP_IDS",
        "SAMPLE_TAP_GITLAB_PROJECT_IDS",
    ):
        assert f"secrets.{secret} != ''" in guard

    skip_step = next(
        (
            step
            for step in external["steps"]
            if step.get("name") == "Report external test skip"
        ),
        None,
    )
    assert skip_step is not None
    assert skip_step.get("if") == "env.EXTERNAL_TESTS_ENABLED != 'true'"

    external_steps = [
        step
        for step in external["steps"]
        if step.get("name") != "Report external test skip"
    ]
    assert external_steps
    assert all(
        step.get("if") == "env.EXTERNAL_TESTS_ENABLED == 'true'"
        for step in external_steps
    )


def test_coverage_generation_stays_required_but_upload_is_guarded() -> None:
    coverage = _test_workflow()["jobs"]["coverage"]
    guard = coverage["env"]["CODECOV_UPLOAD_ENABLED"]

    assert "github.repository == 'meltano/sdk'" in guard
    assert "secrets.CODECOV_TOKEN != ''" in guard

    steps = {step.get("name"): step for step in coverage["steps"]}
    assert "if" not in steps["Create coverage report"]
    assert steps["Upload coverage to Codecov"]["if"] == (
        "env.CODECOV_UPLOAD_ENABLED == 'true'"
    )
    assert steps["Upload coverage to Codecov"]["with"]["fail_ci_if_error"] == "true"
    assert steps["Report Codecov upload skip"]["if"] == (
        "env.CODECOV_UPLOAD_ENABLED != 'true'"
    )
