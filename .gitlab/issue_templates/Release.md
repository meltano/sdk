[//]: # (NOTE: This Release template is for Admin-Use only. If you've reached this template in error, please select another template from the list.)

## Evergreen Releases - Prep Steps:

An `Evergreen` release process means we are _always_ releasing. We open a new release ticket as soon as we've completed the prior release. (It's therefore the final step in this checklist.)

## "Evergreen Prep" Checklist

- [x] Open this Issue
- [ ] Indicate the version to be released here in the issue's title `Release vX.Y.Z`
    - If the release number changes (from minor to major or patch, for instance), update the version here and in the issue description.

### Readiness Checklist:

`Engineering` team, to get ready for the upcoming release:

1. [ ] Ensure any [already-merged commits](https://gitlab.com/meltano/sdk/-/commits/main) since the last release have [Changelog](https://gitlab.com/meltano/sdk/-/blob/main/CHANGELOG.md) entries (excepting non-user-impacting commits, such as docs fixes).
2. [ ] Create a comment in the `#engineering-team` slack channel with pending-but-not-merged MRs, potentially shipping. (Aka, the "burndown" list.)
    - Otherwise a comment that all known merge candidates are already merged.
3. [ ] Create or link to a summary of MRs merged and/or expected in the `#marketing` Slack channel, with an `@channel` mention.

### Release Checklist

Rotating `assignee`, on the morning of the release:

1. [ ] Changelog updates and version bump:
    1. [ ] Create a new branch named `release/vX.Y.Z` and a corresponding MR with the `Release` MR template.
    2. An automated pipeline (linked to the branch prefix `release/v*`) will
    immediately and automatically bump the version and flush the changelog.
        - [ ] Check this box to confirm the automated changelog flush and version bump are correct.
        - You _do not_ need to wait for the CI pipeline. (An identical CI pipeline is already included in the below.)
    3. [ ] Review the changelog, committing an update for clarity/readability/typos if needed.
    4. [ ] Check the [pending MRs](https://gitlab.com/meltano/sdk/-/merge_requests?sort=updated_desc) to make sure nothing is missing from `main` branch.
2. [ ] [Cut a release tag](https://gitlab.com/meltano/sdk/-/tags/new) from your `release/vX.Y.Z` branch named `vX.Y.Z` with Message=`Release vX.Y.Z`
    1. In response to new tag creation, these steps are performed automatically in Gitlab pipelines:
        1. Abort if tag `vX.Y.Z` does not match output from `poetry version --short`
        2. Test _everything_.
        3. Publish to PyPi <!-- Meltano-only: and Docker -->.
    2. While the process is running, you can continue with next steps, such as changelog grooming.
    3. [ ] Check this box when the tag's pipeline has completed (eta 40-60 minutes).
    4. [ ] Check this box when [PyPi publish](https://pypi.org/project/singer-sdk/#history) is confirmed.
    <!-- Meltano-only: 5. [ ] Check this box when [Docker publish]() is confirmed. -->
3. Groom the changelog:
    1. [ ] Compare the [Changelog](https://gitlab.com/meltano/sdk/-/blob/main/CHANGELOG.md) against the `main` branch [commit history](https://gitlab.com/meltano/sdk/-/commits/main) and add any significant user-impacting updates (excluding docs and website updates, for instance).
    2. [ ] Open the [Changelog](https://gitlab.com/meltano/sdk/-/blob/main/CHANGELOG.md) in preview mode, mouse over each link and ensure tooltip descriptions match the resolved issue. Check contributor profile links to make sure they are correct.
    3. [ ] Merge the resulting MR to `main` with the merge commit message `Release vX.Y.Z`
4. [ ] Open the next `Release` issue, assign as appropriate, and provide that link here: `___`

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
