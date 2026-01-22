//! Hypergraph Demo - End-to-end example
//!
//! This binary demonstrates the complete Grism workflow:
//! 1. Create a hypergraph with nodes, edges, and hyperedges
//! 2. Store the data in memory
//! 3. Run queries using the Rust API
//! 4. Display results
//!
//! # Usage
//!
//! ```bash
//! cargo run --package grism-playground --bin hypergraph-demo
//! ```

use std::sync::Arc;

use clap::Parser;

use common_error::GrismResult;
use grism_engine::{LocalExecutor, LocalPhysicalPlanner, PhysicalPlanner};
use grism_logical::{LogicalOp, LogicalPlan};
use grism_logical::ops::{FilterOp, LimitOp, ProjectOp, ScanOp};
use grism_logical::expr::{col, lit};
use grism_optimizer::Optimizer;
use grism_storage::{InMemoryStorage, SnapshotId, Storage};

use grism_playground::{create_social_network, print_results, print_header, print_divider};
use grism_playground::data::properties;

/// Hypergraph Demo CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "hypergraph-demo")]
#[command(about = "End-to-end demonstration of Grism hypergraph capabilities")]
struct Args {
    /// Verbose output
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> GrismResult<()> {
    let args = Args::parse();

    print_header("Grism Hypergraph Demo");
    println!();
    println!("This demo shows how to:");
    println!("  1. Create nodes, edges, and hyperedges");
    println!("  2. Store data in memory");
    println!("  3. Run queries with filters, projections, and aggregations");
    println!("  4. Execute using the local engine");

    // Step 1: Create storage with sample data
    print_header("Step 1: Create Social Network Data");
    let storage = create_social_network().await?;
    
    // Print statistics
    let node_count = storage.get_all_nodes().await?.len();
    let edge_count = storage.get_all_edges().await?.len();
    let hyperedge_count = storage.get_all_hyperedges().await?.len();
    
    println!("Created hypergraph with:");
    println!("  - {} nodes", node_count);
    println!("  - {} edges", edge_count);
    println!("  - {} hyperedges", hyperedge_count);

    if args.verbose {
        print_divider();
        println!("Nodes:");
        for node in storage.get_all_nodes().await? {
            println!("  {:?}", node);
        }
    }

    // Step 2: Run basic scan query
    print_header("Step 2: Scan All Person Nodes");
    run_scan_query(&storage).await?;

    // Step 3: Run filtered query
    print_header("Step 3: Filter Persons Over Age 30");
    run_filter_query(&storage).await?;

    // Step 4: Run projection query
    print_header("Step 4: Project Name and City");
    run_projection_query(&storage).await?;

    // Step 5: Run limited query
    print_header("Step 5: Limit Results to 3");
    run_limit_query(&storage).await?;

    // Step 6: Show hyperedges
    print_header("Step 6: Scan Hyperedges");
    run_hyperedge_scan(&storage).await?;

    // Summary
    print_header("Demo Complete!");
    println!();
    println!("The demo showed:");
    println!("  ✓ Creating a social network hypergraph");
    println!("  ✓ Node scans with label filtering");
    println!("  ✓ Predicate filtering");
    println!("  ✓ Column projection");
    println!("  ✓ Result limiting");
    println!("  ✓ Hyperedge queries");
    println!();
    println!("See the grism-playground crate for more examples!");

    Ok(())
}

/// Run a simple scan query.
async fn run_scan_query(storage: &Arc<InMemoryStorage>) -> GrismResult<()> {
    // Build logical plan: SCAN nodes WHERE label = 'Person'
    let scan = ScanOp::nodes_with_label("Person");
    let logical_plan = LogicalPlan::new(LogicalOp::scan(scan));

    println!("Logical Plan:");
    println!("  {}", logical_plan.root().name());

    // Convert to physical plan
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan)?;

    println!("Physical Plan:");
    println!("  {}", physical_plan.root().name());

    // Execute
    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical_plan,
            Arc::clone(storage) as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    print_results(&result);
    Ok(())
}

/// Run a query with filter predicate.
async fn run_filter_query(storage: &Arc<InMemoryStorage>) -> GrismResult<()> {
    // Build logical plan: SCAN Person WHERE age > 30
    let scan = ScanOp::nodes_with_label("Person");
    let filter = FilterOp::new(col("age").gt(lit(30i64)));
    
    let logical_plan = LogicalPlan::new(LogicalOp::filter(
        LogicalOp::scan(scan),
        filter,
    ));

    println!("Logical Plan:");
    println!("  Filter(age > 30)");
    println!("    └── Scan(Person)");

    // Optimize (using default optimizer rules)
    let optimizer = Optimizer::default();
    let optimized = optimizer.optimize(logical_plan)?;

    // Convert to physical (use the plan field from OptimizedPlan)
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&optimized.plan)?;

    // Execute
    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical_plan,
            Arc::clone(storage) as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    print_results(&result);
    Ok(())
}

/// Run a query with projection.
async fn run_projection_query(storage: &Arc<InMemoryStorage>) -> GrismResult<()> {
    // Build logical plan: SELECT name, city FROM Person
    let scan = ScanOp::nodes_with_label("Person");
    let project = ProjectOp::new(vec![col("name"), col("city")]);
    
    let logical_plan = LogicalPlan::new(LogicalOp::project(
        LogicalOp::scan(scan),
        project,
    ));

    println!("Logical Plan:");
    println!("  Project(name, city)");
    println!("    └── Scan(Person)");

    // Convert and execute
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan)?;

    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical_plan,
            Arc::clone(storage) as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    print_results(&result);
    Ok(())
}

/// Run a query with limit.
async fn run_limit_query(storage: &Arc<InMemoryStorage>) -> GrismResult<()> {
    // Build logical plan: SELECT * FROM Person LIMIT 3
    let scan = ScanOp::nodes_with_label("Person");
    let limit = LimitOp::new(3);
    
    let logical_plan = LogicalPlan::new(LogicalOp::limit(
        LogicalOp::scan(scan),
        limit,
    ));

    println!("Logical Plan:");
    println!("  Limit(3)");
    println!("    └── Scan(Person)");

    // Convert and execute
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan)?;

    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical_plan,
            Arc::clone(storage) as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    print_results(&result);
    Ok(())
}

/// Scan hyperedges.
async fn run_hyperedge_scan(storage: &Arc<InMemoryStorage>) -> GrismResult<()> {
    // Build logical plan: SCAN hyperedges WHERE label = 'WORKS_AT'
    let scan = ScanOp::hyperedges_with_label("WORKS_AT");
    let logical_plan = LogicalPlan::new(LogicalOp::scan(scan));

    println!("Logical Plan:");
    println!("  Scan(WORKS_AT hyperedges)");

    // Convert and execute
    let planner = LocalPhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan)?;

    let executor = LocalExecutor::new();
    let result = executor
        .execute(
            physical_plan,
            Arc::clone(storage) as Arc<dyn Storage>,
            SnapshotId::default(),
        )
        .await?;

    print_results(&result);
    Ok(())
}
