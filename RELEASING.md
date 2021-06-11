# Publishing a release

1. Create a new release branch from up-to-date master named `release/x.y.z`
1. Review CHANGELOG.md. Move `[Unreleased]` header to empty section and replace with `[x.y.z]` header plus release date.
1. Update the version numbers in the build.sbt and spark-shell examples in the README's "Getting Started" section.
1. Commit these changes as a single commit, with the message "Release vx.y.z"
1. Push branch and make a PR on GitHub
1. Ensure CI succeeds
1. Ensure there are no new commits on master. If there are new commits, rebase this branch on master and start over at step 2 if you wish to include them. Otherwise, merge.
1. Tag the merge commit on the master branch: `git tag -a vx.y.z -m "Release x.y.z"`
1. Push the new tag: `git push --tags`; if you have multiple remotes, you may need to target the proper upstream repo: `git push <remote> --tags`.
1. Review the CircleCI build status to ensure that the tag was successfully published to SonaType.
