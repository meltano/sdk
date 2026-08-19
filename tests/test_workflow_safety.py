from __future__ import annotations

from pathlib import Path

import yaml

_GITLAB_SECRETS = (
    "TAP_GITLAB_AUTH_TOKEN",
    "TAP_GITLAB_GROUP_IDS",
    "TAP_GITLAB_PROJECT_IDS",
)


def _test_workflow() -> dict:
    workflow_path = Path(__file__).parents[1] / ".github" / "workflows" / "test.yml"
    return yaml.load(workflow_path.read_text(), Loader=yaml.BaseLoader)


def test_external_tests_are_skipped_for_forks_and_only_for_forks() -> None:
    """Only a fork may skip: anything else has to run or fail, never report green."""
    external = _test_workflow()["jobs"]["tests-external"]

    assert external["if"] == "${{ !github.event.pull_request.head.repo.fork }}"
    assert "EXTERNAL_TESTS_ENABLED" not in external.get("env", {})
    assert all("if" not in step for step in external["steps"])


def test_missing_external_secrets_fail_the_job() -> None:
    external = _test_workflow()["jobs"]["tests-external"]
    guard = next(
        step
        for step in external["steps"]
        if step.get("name") == "Require sample GitLab secrets"
    )

    assert external["steps"].index(guard) == 0
    for secret in _GITLAB_SECRETS:
        assert secret in guard["run"]
        assert external["env"][secret] == f"${{{{ secrets.SAMPLE_{secret} }}}}"
    assert "exit" in guard["run"]


def test_codecov_upload_runs_on_forks_without_a_token() -> None:
    """Fork pull requests uploaded coverage tokenlessly before, and still must."""
    coverage = _test_workflow()["jobs"]["coverage"]

    assert "CODECOV_UPLOAD_ENABLED" not in coverage["env"]

    upload = next(
        step
        for step in coverage["steps"]
        if step.get("uses", "").startswith("codecov/codecov-action@")
    )
    assert "if" not in upload
    assert upload["with"]["fail_ci_if_error"] == "true"
    assert "secrets.CODECOV_TOKEN" in upload["with"]["token"]


def test_coverage_report_generation_is_unconditional() -> None:
    coverage = _test_workflow()["jobs"]["coverage"]
    steps = {step.get("name"): step for step in coverage["steps"]}

    assert "if" not in steps["Create coverage report"]
