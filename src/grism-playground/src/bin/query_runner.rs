//! Query Runner - Interactive query testing
//!
//! A simple utility for running queries against sample data.
//!
//! # Usage
//!
//! ```bash
//! cargo run --package grism-playground --bin query-runner -- --help
//! ```

use std::sync::Arc;

use clap::{Parser, Subcommand};

use common_error::GrismResult;
use grism_engine::{LocalExecutor, LocalPhysicalPlanner, PhysicalPlanner};
use grism_logical::expr::{col, lit};
use grism_logical::ops::{FilterOp, LimitOp, ProjectOp, ScanOp};
use grism_logical::{LogicalOp, LogicalPlan};
use grism_optimizer::Optimizer;
use grism_storage::{MemoryStorage, SnapshotId, Storage};

use grism_playground::{create_social_network, print_header, print_results};

/// Query Runner CLI.
#[derive(Parser, Debug)]
#[command(name = "query-runner")]
#[command(about = "Run queries against sample hypergraph data")]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scan nodes by label
    Scan {
        /// Node label to scan
        #[arg(short, long, default_value = "Person")]
        label: String,

        /// Maximum results
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },

    /// Filter nodes by predicate
    Filter {
        /// Node label
        #[arg(short, long, default_value = "Person")]
        label: String,

        /// Column to filter on
        #[arg(short, long)]
        column: String,

        /// Value to compare (as i64)
        #[arg(short, long)]
        value: i64,

        /// Comparison operator (gt, lt, eq)
        #[arg(short, long, default_value = "gt")]
        op: String,
    },

    /// Project specific columns
    Project {
        /// Node label
        #[arg(short, long, default_value = "Person")]
        label: String,

        /// Columns to project
        #[arg(short, long, num_args = 1..)]
        columns: Vec<String>,
    },

    /// Show storage statistics
    Stats,

    /// Run all demo queries
    Demo,
}

#[tokio::main]
async fn main() -> GrismResult<()> {
    let args = Args::parse();

    // Create storage with sample data
    let storage = create_social_network().await?;

    match args.command {
        Commands::Scan { label, limit } => {
            run_scan(&storage, &label, limit).await?;
        }
        Commands::Filter {
            label,
            column,
            value,
            op,
        } => {
            run_filter(&storage, &label, &column, value, &op).await?;
        }
        Commands::Project { label, columns } => {
            run_project(&storage, &label, &columns).await?;
        }
        Commands::Stats => {
            show_stats(&storage).await?;
        }
        Commands::Demo => {
            run_demo(&storage).await?;
        }
    }

    Ok(())
}

async fn run_scan(
    storage: &Arc<MemoryStorage>,
    label: &str,
    limit: Option<usize>,
) -> GrismResult<()> {
    print_header(&format!("Scanning {} nodes", label));

    let scan = ScanOp::nodes_with_label(label);
    let mut logical = LogicalOp::scan(scan);

    if let Some(n) = limit {
        logical = LogicalOp::limit(logical, LimitOp::new(n));
    }

    let plan = LogicalPlan::new(logical);
    execute_plan(storage, &plan).await
}

async fn run_filter(
    storage: &Arc<MemoryStorage>,
    label: &str,
    column: &str,
    value: i64,
    op: &str,
) -> GrismResult<()> {
    print_header(&format!(
        "Filtering {} where {} {} {}",
        label, column, op, value
    ));

    let scan = ScanOp::nodes_with_label(label);

    let predicate = match op {
        "gt" => col(column).gt(lit(value)),
        "lt" => col(column).lt(lit(value)),
        "eq" => col(column).eq(lit(value)),
        "gte" | "ge" => col(column).gt_eq(lit(value)),
        "lte" | "le" => col(column).lt_eq(lit(value)),
        _ => {
            eprintln!("Unknown operator: {}. Using 'gt'", op);
            col(column).gt(lit(value))
        }
    };

    let filter = FilterOp::new(predicate);
    let logical = LogicalOp::filter(LogicalOp::scan(scan), filter);
    let plan = LogicalPlan::new(logical);

    execute_plan(storage, &plan).await
}

async fn run_project(
    storage: &Arc<MemoryStorage>,
    label: &str,
    columns: &[String],
) -> GrismResult<()> {
    if columns.is_empty() {
        println!("No columns specified. Use -c to specify columns.");
        return Ok(());
    }

    print_header(&format!("Projecting {} from {}", columns.join(", "), label));

    let scan = ScanOp::nodes_with_label(label);
    let exprs: Vec<_> = columns.iter().map(|c| col(c)).collect();
    let project = ProjectOp::new(exprs);

    let logical = LogicalOp::project(LogicalOp::scan(scan), project);
    let plan = LogicalPlan::new(logical);

    execute_plan(storage, &plan).await
}

async fn show_stats(_storage: &Arc<MemoryStorage>) -> GrismResult<()> {
    print_header("Storage Statistics");

    // TODO: Statistics require scanning datasets with RFC-0012 interface
    // For now, display message about using scan operations instead
    println!("Statistics are available via RFC-0012 Storage::scan() operations.");
    println!("Use 'scan' command to query specific datasets.");

    Ok(())
}

async fn run_demo(storage: &Arc<MemoryStorage>) -> GrismResult<()> {
    print_header("Running Demo Queries");

    println!("\n1. Scan all Person nodes:");
    run_scan(storage, "Person", None).await?;

    println!("\n2. Filter age > 30:");
    run_filter(storage, "Person", "age", 30, "gt").await?;

    println!("\n3. Project name and city:");
    run_project(storage, "Person", &["name".to_string(), "city".to_string()]).await?;

    println!("\n4. Scan companies:");
    run_scan(storage, "Company", None).await?;

    println!("\nDemo complete!");
    Ok(())
}

async fn execute_plan(storage: &Arc<MemoryStorage>, plan: &LogicalPlan) -> GrismResult<()> {
    // Optimize (using default optimizer rules)
    let optimizer = Optimizer::default();
    let optimized = optimizer.optimize(plan.clone())?;

    // Convert to physical (use the plan field from OptimizedPlan)
    let planner = LocalPhysicalPlanner::new();
    let physical = planner.plan(&optimized.plan)?;

    // Execute
    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical,
            Arc::clone(storage) as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    print_results(&result);
    Ok(())
}
