use xshell::{cmd, Shell};

fn main() -> anyhow::Result<()> {
    let sh = Shell::new()?;

    cmd!(
        sh,
        "cargo build --release --target x86_64-unknown-linux-musl"
    )
    .run()?;

    let _ = sh.remove_path("./build");
    sh.create_dir("./build")?;

    let functions = vec!["email_index_writer", "email_index_reader"];

    for function in &functions {
        let func = *function;
        sh.copy_file(
            format!("./target/x86_64-unknown-linux-musl/release/{func}"),
            format!("./build/{func}"),
        )?;
    }

    sh.change_dir("./build");

    for function in &functions {
        let func = *function;
        sh.copy_file(func, "bootstrap")?;
        cmd!(sh, "zip {func}.zip bootstrap").run()?;
        sh.remove_path(format!("{func}"))?;
        sh.remove_path(format!("bootstrap"))?;
    }

    Ok(())
}
