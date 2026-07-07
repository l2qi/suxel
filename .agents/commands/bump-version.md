Bump the workspace crate version. The new version is given in $ARGUMENTS.

**Parse the argument:**
- If `$ARGUMENTS` is empty, tell the user to pass a version string (e.g. `/bump-version 0.2.0`).
- Validate it looks like a semver version (`MAJOR.MINOR.PATCH`, optionally with `- prerelease` suffix). If it doesn't, tell the user and stop.

**Steps:**

1. **Read the current version.** Extract the current `version` from `Cargo.toml` under `[workspace.package]` so we can report the old → new change.

2. **Edit `Cargo.toml`.** Replace the `version = "..."` line under `[workspace.package]` with the new version. Do not touch any other line.

3. **Update the lockfile.** Run:
   ```
   cargo generate-lockfile
   ```
   This re-resolves and writes `Cargo.lock` without compiling anything.

4. **Report.** Print the old and new version, and confirm `Cargo.lock` is updated. Remind the owner that any downstream consumer pinning suxel by `tag = "vX.Y.Z"` only picks up the new version once the matching tag is pushed.

**Hard rules — do NOT:**
- Commit or push the change. The user will review and commit themselves.
- Modify any file other than `Cargo.toml` and the lockfile update.

$ARGUMENTS
