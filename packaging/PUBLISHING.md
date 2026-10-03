# Publish VCA and Paceflow

1. Update the workspace version and Paceflow's exact VCA dependency together; update release notes.
2. Run workspace tests, formatting, and Clippy. Run `python3 packaging/verify_packages.py` to build extracted packages without repository files or Git metadata.
3. Recheck `https://crates.io/api/v1/crates/vibe-coding-analytics` and the owners of `paceflow`. A missing crate name is not reserved until publication. Existing Paceflow owners must authorize the publishing credential.
4. Configure the GitHub Actions secret `CRATES_PUBLISH_TOKEN` for publishing both crates. It must permit creating VCA on the first release and updating both thereafter; the old Paceflow-only token may not suffice. Verify the crates.io account and email. A dry run does not prove upload permission.
5. Push a tag matching both crate versions, for example `v0.3.0`. The coordinated release workflow publishes VCA, waits for registry availability, verifies Paceflow against that published dependency, then publishes Paceflow and creates the GitHub release. Registry errors abort; reruns skip confirmed existing versions.
6. After the first VCA publication, add the appropriate maintainers as owners. Name allocation is first come, first served, and published versions cannot be overwritten.
7. Verify `cargo install --locked vibe-coding-analytics`, `cargo install --locked paceflow`, `cargo binstall vibe-coding-analytics`, and `cargo binstall paceflow` from fresh installation directories; both commands must coexist.

If VCA publishes but Paceflow fails, fix the cause and rerun the same release job. Do not bump VCA just to retry. Publishing an existing version is skipped only when its registry lookup returns a confirmed version.

Publishing documentation: https://doc.rust-lang.org/cargo/reference/publishing.html
