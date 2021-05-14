[//]: # (NOTE: This Release template is for Admin-Use only. If you've reached this template in error, please select another template from the list.)


## SDK Release Checklist

- [ ] Indicate the version to be released here: `vX.Y.Z`

### SDK Release Flow

1. [ ] Manual steps:
    - [x] Open this Issue
    - [ ] Create a corresponding MR on a branch named `release-vX.Y.Z`
    - [ ] The `release-vX.Y.Z` MR is ready when:
        - [ ] Version is bumped:
          - [ ] `pyproject.toml`
          - [ ] `cookiecutter/tap-template/pyproject.toml`
        - [ ] Changelog is flushed with the appropriate version number
        - [ ] CI pipeline status is green
2. [ ] Release steps:
   1. [ ] Manual:
      1. [ ] Cut a tag named `vX.Y.Z`, which must match the release version
   2. [ ] Automated CD pipeline:
       - In response to new tag creation, these steps are performed automatically in Gitlab pipelines:
           - Abort if tag `vX.Y.Z` does not match output from `poetry version --short`
           - Publish to PyPi
           - Create a Gitlab 'Release' from the specified tag

### SDK Post-Release Flow

1. [ ] Post-release announcement steps:
    1. [ ] Post announcement to Meltano slack: `#announcements`, cross-post (share) to `#sdk`
    2. [ ] Copy-paste with minor contextual edits to Singer slack (with link to original Meltano slack)
    3. [ ] Blog post
    4. [ ] Tweet the blog post


----------------
