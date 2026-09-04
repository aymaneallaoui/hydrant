use std::process::ExitCode;

use clap::Parser;
use hydrant_loadgen::{cli::Args, stats::Report};

fn main() -> ExitCode {
    let args = Args::parse();
    let json_path = args.json.clone();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("error: starting the async runtime: {err}");
            if err.raw_os_error() == Some(24) {
                eprintln!("hint: the file-descriptor limit is too low; raise it with `ulimit -n`");
            }
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(hydrant_loadgen::run(args)) {
        Ok(report) => {
            print!("{}", report.render());
            match json_path {
                Some(path) => match write_json(&report, &path) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(err) => {
                        eprintln!("error: writing {}: {err}", path.display());
                        ExitCode::FAILURE
                    }
                },
                None => ExitCode::SUCCESS,
            }
        }
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn write_json(report: &Report, path: &std::path::Path) -> anyhow::Result<()> {
    std::fs::write(path, report.to_json()?)?;
    Ok(())
}
