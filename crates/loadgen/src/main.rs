use std::process::ExitCode;

use clap::Parser;
use hydrant_loadgen::{cli::Args, stats::Report};

fn main() -> ExitCode {
    let args = Args::parse();
    let json_path = args.json.clone();
    let max_loss = args.max_loss_percent;
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
            if let Some(path) = json_path
                && let Err(err) = write_json(&report, &path)
            {
                eprintln!("error: writing {}: {err}", path.display());
                return ExitCode::FAILURE;
            }
            exit_for(&report, max_loss)
        }
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

const EXIT_NO_PUBLISH: u8 = 2;
const EXIT_LOSS: u8 = 3;

fn exit_for(report: &Report, max_loss: Option<f64>) -> ExitCode {
    if report.publish.ok == 0 {
        let cause = report
            .publish
            .first_error
            .as_deref()
            .map_or(String::new(), |e| format!(" (first error: {e})"));
        eprintln!("error: no publish succeeded{cause}; the report above is not meaningful");
        return ExitCode::from(EXIT_NO_PUBLISH);
    }
    if let Some(limit) = max_loss
        && report.deliveries.loss_percent > limit
    {
        eprintln!(
            "error: loss {:.3}% exceeds --max-loss-percent {limit}",
            report.deliveries.loss_percent
        );
        return ExitCode::from(EXIT_LOSS);
    }
    ExitCode::SUCCESS
}

fn write_json(report: &Report, path: &std::path::Path) -> anyhow::Result<()> {
    std::fs::write(path, report.to_json()?)?;
    Ok(())
}
