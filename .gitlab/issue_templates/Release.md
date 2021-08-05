[//]: # (NOTE: This Release template is for Admin-Use only. If you've reached this template in error, please select another template from the list.)

## Evergreen Releases - Prep Steps:

An `Evergreen` release process means we are _always_ releasing. We open a new release ticket as soon as we've completed the prior release. (It's therefore the final step in this checklist.)

## "Evergreen Prep" Checklist

- [x] Open this Issue
- [ ] Indicate the version to be released here in the issue's title and here: `vX.Y.Z`
    - If the release number changes (from minor to major or patch, for instance), update the version here and in the issue description.
- [ ] Provide the _planned_ release date: `yyyy-mm-dd` 

### Release Readiness Checklist:

`Engineering` team, the Monday prior to the release:

1. [ ] Ensure all already-merged commits since the last release have changelog entries (excepting non-user-impacting commits, such as docs fixes).
2. [ ] Create a comment in this issue with pending-but-not-merged MRs potentially shipping.
    - Otherwise a comment that all known merge candidates are already merged.
3. [ ] Link issues to this issue which have already merged, or are expected to merge.

### Release Checklist

`Marketing` and `Product`, on the day prior to the release:

Leveraging the combination of linked issues

1. [ ] Review the changelog for [grokability](https://en.wikipedia.org/wiki/Grok), merging an update for clarity/readability/typos if needed.
2. [ ] Create summary readouts for any planned blog posts, optionally requesting clarification or additional exposition in the `#engineering-team` channel.

Rotating `assignee`, on the morning of the release:

1. [ ] Manual steps:
    1. [ ] Unlink any 'slipped' issues which are not being included in this release.
    2. [ ] Create a corresponding MR on a branch named `<issue>-release-vX-Y-Z` (the default name if your issue is titled correctly)
    3. [ ] The `release-vX.Y.Z` MR is ready when:
        1. [ ] Changelog includes all meaningful user-facing updates since the last release
            - [ ] Compare against `main` branch [commit history](https://gitlab.com/meltano/sdk/-/commits/main)
        2. [ ] Version is bumped:
            - [ ] `pyproject.toml`
            - [ ] `docs/conf.py`
            - [ ] `cookiecutter/tap-template/pyproject.toml`
            - [ ] `cookiecutter/target-template/pyproject.toml`
        3. [ ] Changelog is flushed with the appropriate version number
        4. [ ] Changes above are committed as `changelog and version bump`
        5. [ ] Open the Changelog in preview mode, mouse over each link and ensure tooltip descriptions match the resolved issue. Check contributor profile links to make sure they are correct.
    4. [ ] Check this box when the CI pipeline status is **green** :white_check_mark:
    5. [ ] Merge to `main` with the merge commit message `Release vX.Y.Z`
2. [ ] Release steps:
   1. [ ] Manual:
      1. [ ] [Cut a tag](https://gitlab.com/meltano/sdk/-/tags/new) from `main` named `vX.Y.Z` with Message=`Release vX.Y.Z`
           - _Note: tag name must exactly match poetry version text_
   2. [ ] Automated [CD pipeline](https://gitlab.com/meltano/sdk/-/pipelines?scope=tags):
       - In response to new tag creation, these steps are performed automatically in Gitlab pipelines:
           - Abort if tag `vX.Y.Z` does not match output from `poetry version --short`
           - Publish to [PyPi](https://pypi.org/project/sdk/#history)
               - [ ] Check this box when confirmed
           - Create a Gitlab 'Release' from the specified tag
3. [ ] Open the next `Release` issue, assign as appropriate, and provide that link here: `___`

### Announcements, Marketing, and Promotion

`Marketing` or `Product` team:

1. [ ] Post-release announcement steps:
    1. [ ] Post announcement to Meltano slack: `#announcements`
    2. [ ] Cross-post (share) to `#sdk`
    3. Copy-paste to:
       - [ ] `Singer` slack: `#meltano`, `#singer-sdk`
       - [ ] `dbt` slack: `#tools-meltano`
    4. [ ] Blog post
    5. [ ] Tweet the blog post

----------------
