use assert_cmd::Command;

#[test]
fn both_binary_names_expose_the_same_cli() -> anyhow::Result<()> {
    for args in [vec!["--version"], vec!["--help"], vec!["session", "--help"]] {
        let primary = Command::cargo_bin("vba")?.args(&args).output()?;
        let alias = Command::cargo_bin("paceflow")?.args(&args).output()?;
        assert!(primary.status.success());
        assert!(alias.status.success());
        let output = String::from_utf8(primary.stdout)?;
        assert_eq!(output, String::from_utf8(alias.stdout)?);
        assert_eq!(primary.stderr, alias.stderr);
        assert!(output.contains("vba"));
    }
    Ok(())
}
