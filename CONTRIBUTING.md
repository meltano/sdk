# Contributing

Let's build together! Please see our [Contributor Guide](https://docs.meltano.com/contribute/)
for more information on contributing to Meltano.

We believe that everyone can contribute and we welcome all contributions.
If you're not sure what to work on, here are some [ideas to get you started](https://github.com/meltano/meltano/labels/Accepting%20Merge%20Requests).

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

Optionally, you may use the expanded syntax to specify a scope in the form `<type>(<scope>): <desc>`. Currently scopes are:

 scopes:
  - `taps`       # tap SDK only
  - `targets`    # target SDK only
  - `mappers`    # mappers only
  - `templates`  # cookiecutters

More advanced rules and settings can be found within the file `.github/semantic.yml`.
