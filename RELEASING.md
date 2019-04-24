# Releasing

When releasing, ensure you have write access to the `azavea` 
bintray organization. You can configure your credentials by
following the prompts after running `./sbt bintrayChangeCredentials`

## Publishing a release

1. Create a new release branch from up-to-date master, e.g. `release/x.y.z`
1. Bump version refs in README
1. Bump version ref in `project/Version.scala`
1. Commit these changes as a single commit, with the message "Release vx.y.z"
1. Push branch and make a PR on GitHub
1. Ensure CI succeeds
1. Ensure there are no new commits on master, then merge. If there are new commits, rebase this branch on master and start over at step 2 if you wish to include them.
1. Tag the merge commit: `git tag -a vx.y.z -m "Release x.y.z"`
1. Publish the tagged commit as a new release: `./sbt publish`
1. Push the new tag: `git push --tags`

