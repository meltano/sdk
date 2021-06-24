[//]: # (NOTE: This Release template is for Admin-Use only. If you've reached this template in error, please select another template from the list.)


## SDK Release Checklist

- [0] Indicate the `vX.Y.Z` version to be released here in the issue title and here: `v<>`

### SDK Release Flow

1. [ ] Manual steps:
    1. [x] Open this Issue
    2. [ ] Create a corresponding MR on a branch named `<issue>-release-vX-Y-Z` (the default name if your issue is titled correctly)
    3. [ ] The `release-vX.Y.Z` MR is ready when:
        1. [ ] Changelog includes all meaningful user-facing updates since the last release
            - [ ] Compare against `main` branch [commit history](https://gitlab.com/meltano/singer-sdk/-/commits/main)
        2. [ ] Version is bumped:
            - [ ] `pyproject.toml`
            - [ ] `cookiecutter/tap-template/pyproject.toml`
            - [ ] `cookiecutter/target-template/pyproject.toml`
        3. [ ] Changelog is flushed with the appropriate version number
        4. [ ] Changes above are committed as `changelog and version bump`
        5. [ ] Open the Changelog in preview mode, mouse over each link and ensure tooltip descriptions match the resolved issue. Check contributor profile links to make sure they are correct.
        6. [ ] Update `.gitlab/issue_templates_/Release.md` with any missing or changed steps in this checklist.
    4. [ ] Check this box when the CI pipeline status is **green** :white_check_mark:
    5. [ ] Merge to `main` with the merge commit message `Release vX.Y.Z`
2. [ ] Release steps:
   1. [ ] Manual:
      1. [ ] [Cut a tag](https://gitlab.com/meltano/singer-sdk/-/tags/new) from `main` named `vX.Y.Z` with Message=`Release vX.Y.Z`
           - _Note: tag name must exactly match poetry version text_
   2. [ ] Automated [CD pipeline](https://gitlab.com/meltano/singer-sdk/-/pipelines?scope=tags):
       - In response to new tag creation, these steps are performed automatically in Gitlab pipelines:
           - Abort if tag `vX.Y.Z` does not match output from `poetry version --short`
           - Publish to [PyPi](https://pypi.org/project/singer-sdk/#history)
               - [ ] Check this box when confirmed
           - Create a Gitlab 'Release' from the specified tag

### SDK Post-Release Flow

1. [ ] Post-release announcement steps:
    1. [ ] Post announcement to Meltano slack: `#announcements`, cross-post (share) to `#sdk`
    2. [ ] Copy-paste with minor contextual edits to Singer slack (with link to original Meltano slack)
    3. [ ] Blog post
    4. [ ] Tweet the blog post


----------------
