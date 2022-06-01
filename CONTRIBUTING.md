# Contributing

Let's build together! Please see our [Contributor Guide](https://docs.meltano.com/contribute/)
for more information on contributing to Meltano.

We believe that everyone can contribute and we welcome all contributions.
If you're not sure what to work on, here are some [ideas to get you started](https://gitlab.com/groups/meltano/-/issues?label_name%5B%5D=Accepting%20Merge%20Requests).

Chat with us in [#contributing](https://meltano.slack.com/archives/C013Z450LCD) on [Slack](https://meltano.com/slack).

Contributors are expected to follow our [Code of Conduct](https://docs.meltano.com/contribute/#code-of-conduct).

## Semantic Pull Requests

This repo uses the [semantic-prs](https://github.com/Ezard/semantic-prs) GitHub app to check all PRs againts the conventional commit syntax.

Pull requests should be named according to the conventional commit syntax to streamline changelog and release notes management. We encourage (but do not require) the use of conventional commits in commit messages as well.

In general, PR titles should follow the format "<type>: <desc>", where type is any one of these:

- `ci`
- `chore`
- `build`
- `docs`
- `feat`
- `fix`
- `perf`
- `refactor`
- `revert`
- `style`
- `test`

More advanced rules and settings can be found within the file `.github/semantic.yml`.
