//! Aggregate execution operator.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use common_error::{GrismError, GrismResult};
use grism_logical::{AggExpr, AggFunc, LogicalExpr};

use crate::executor::ExecutionContext;
use crate::expr::ExprEvaluator;
use crate::operators::PhysicalOperator;
use crate::physical::{OperatorCaps, PhysicalSchema};

/// Trait for aggregate accumulators.
trait Accumulator: Send + Sync {
    /// Update the accumulator with new values.
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()>;

    /// Merge another accumulator into this one.
    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()>;

    /// Get the final result.
    fn finalize(&self) -> GrismResult<ArrayRef>;

    /// Get the result data type.
    fn result_type(&self) -> DataType;

    /// Clone as boxed.
    fn clone_box(&self) -> Box<dyn Accumulator>;

    /// Get as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Sum accumulator for Int64 values.
#[derive(Debug, Clone, Default)]
struct SumInt64Accumulator {
    sum: i64,
    count: usize,
}

impl Accumulator for SumInt64Accumulator {
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()> {
        let int_arr = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| GrismError::type_error("SUM requires numeric input"))?;

        for i in 0..int_arr.len() {
            if !int_arr.is_null(i) {
                self.sum += int_arr.value(i);
                self.count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()> {
        if let Some(other) = other.as_any().downcast_ref::<SumInt64Accumulator>() {
            self.sum += other.sum;
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> GrismResult<ArrayRef> {
        if self.count == 0 {
            Ok(Arc::new(Int64Array::from(vec![None as Option<i64>])))
        } else {
            Ok(Arc::new(Int64Array::from(vec![self.sum])))
        }
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Sum accumulator for Float64 values.
#[derive(Debug, Clone, Default)]
struct SumFloat64Accumulator {
    sum: f64,
    count: usize,
}

impl Accumulator for SumFloat64Accumulator {
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()> {
        let float_arr = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| GrismError::type_error("SUM requires numeric input"))?;

        for i in 0..float_arr.len() {
            if !float_arr.is_null(i) {
                self.sum += float_arr.value(i);
                self.count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()> {
        if let Some(other) = other.as_any().downcast_ref::<SumFloat64Accumulator>() {
            self.sum += other.sum;
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> GrismResult<ArrayRef> {
        if self.count == 0 {
            Ok(Arc::new(Float64Array::from(vec![None as Option<f64>])))
        } else {
            Ok(Arc::new(Float64Array::from(vec![self.sum])))
        }
    }

    fn result_type(&self) -> DataType {
        DataType::Float64
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Count accumulator.
#[derive(Debug, Clone, Default)]
struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()> {
        // COUNT counts non-null values
        for i in 0..values.len() {
            if !values.is_null(i) {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()> {
        if let Some(other) = other.as_any().downcast_ref::<CountAccumulator>() {
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> GrismResult<ArrayRef> {
        Ok(Arc::new(Int64Array::from(vec![self.count])))
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Min accumulator for Int64.
#[derive(Debug, Clone, Default)]
struct MinInt64Accumulator {
    min: Option<i64>,
}

impl Accumulator for MinInt64Accumulator {
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()> {
        let int_arr = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| GrismError::type_error("MIN requires comparable input"))?;

        for i in 0..int_arr.len() {
            if !int_arr.is_null(i) {
                let val = int_arr.value(i);
                self.min = Some(match self.min {
                    Some(current) => current.min(val),
                    None => val,
                });
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()> {
        if let Some(other) = other.as_any().downcast_ref::<MinInt64Accumulator>() {
            if let Some(other_min) = other.min {
                self.min = Some(match self.min {
                    Some(current) => current.min(other_min),
                    None => other_min,
                });
            }
        }
        Ok(())
    }

    fn finalize(&self) -> GrismResult<ArrayRef> {
        Ok(Arc::new(Int64Array::from(vec![self.min])))
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Max accumulator for Int64.
#[derive(Debug, Clone, Default)]
struct MaxInt64Accumulator {
    max: Option<i64>,
}

impl Accumulator for MaxInt64Accumulator {
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()> {
        let int_arr = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| GrismError::type_error("MAX requires comparable input"))?;

        for i in 0..int_arr.len() {
            if !int_arr.is_null(i) {
                let val = int_arr.value(i);
                self.max = Some(match self.max {
                    Some(current) => current.max(val),
                    None => val,
                });
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()> {
        if let Some(other) = other.as_any().downcast_ref::<MaxInt64Accumulator>() {
            if let Some(other_max) = other.max {
                self.max = Some(match self.max {
                    Some(current) => current.max(other_max),
                    None => other_max,
                });
            }
        }
        Ok(())
    }

    fn finalize(&self) -> GrismResult<ArrayRef> {
        Ok(Arc::new(Int64Array::from(vec![self.max])))
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Avg accumulator.
#[derive(Debug, Clone, Default)]
struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn update(&mut self, values: &ArrayRef) -> GrismResult<()> {
        // Handle Int64 or Float64
        if let Some(int_arr) = values.as_any().downcast_ref::<Int64Array>() {
            for i in 0..int_arr.len() {
                if !int_arr.is_null(i) {
                    self.sum += int_arr.value(i) as f64;
                    self.count += 1;
                }
            }
        } else if let Some(float_arr) = values.as_any().downcast_ref::<Float64Array>() {
            for i in 0..float_arr.len() {
                if !float_arr.is_null(i) {
                    self.sum += float_arr.value(i);
                    self.count += 1;
                }
            }
        } else {
            return Err(GrismError::type_error("AVG requires numeric input"));
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> GrismResult<()> {
        if let Some(other) = other.as_any().downcast_ref::<AvgAccumulator>() {
            self.sum += other.sum;
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> GrismResult<ArrayRef> {
        if self.count == 0 {
            Ok(Arc::new(Float64Array::from(vec![None as Option<f64>])))
        } else {
            Ok(Arc::new(Float64Array::from(vec![
                self.sum / self.count as f64,
            ])))
        }
    }

    fn result_type(&self) -> DataType {
        DataType::Float64
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Create an accumulator for the given aggregate function.
fn create_accumulator(func: &AggFunc, input_type: &DataType) -> Box<dyn Accumulator> {
    match func {
        AggFunc::Sum => match input_type {
            DataType::Int64 => Box::new(SumInt64Accumulator::default()),
            DataType::Float64 => Box::new(SumFloat64Accumulator::default()),
            _ => Box::new(SumFloat64Accumulator::default()), // Fallback
        },
        AggFunc::Count | AggFunc::CountDistinct => Box::new(CountAccumulator::default()),
        AggFunc::Min => Box::new(MinInt64Accumulator::default()), // Simplified: assume Int64
        AggFunc::Max => Box::new(MaxInt64Accumulator::default()), // Simplified: assume Int64
        AggFunc::Avg => Box::new(AvgAccumulator::default()),
        _ => Box::new(CountAccumulator::default()), // Fallback for First, Last, Collect etc
    }
}

/// Hash aggregate execution operator.
///
/// This is a **blocking** operator that must consume all input
/// before producing any output.
#[derive(Debug)]
pub struct HashAggregateExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Group-by expressions.
    group_by: Vec<LogicalExpr>,
    /// Aggregate expressions.
    aggregates: Vec<AggExpr>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Aggregation state.
    state: tokio::sync::Mutex<AggregateState>,
}

#[derive(Debug, Default)]
enum AggregateState {
    #[default]
    Uninitialized,
    /// Accumulating input.
    Accumulating,
    /// All input consumed, ready to emit.
    Ready(Vec<RecordBatch>),
    /// All output emitted.
    Exhausted,
}

impl HashAggregateExec {
    /// Create a new hash aggregate operator.
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggExpr>,
        schema: PhysicalSchema,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            schema,
            state: tokio::sync::Mutex::new(AggregateState::Uninitialized),
        }
    }

    /// Get group-by expressions.
    pub fn group_by(&self) -> &[LogicalExpr] {
        &self.group_by
    }

    /// Get aggregate expressions.
    pub fn aggregates(&self) -> &[AggExpr] {
        &self.aggregates
    }

    /// Perform aggregation on all input batches.
    fn aggregate_batches(&self, batches: &[RecordBatch]) -> GrismResult<RecordBatch> {
        let evaluator = ExprEvaluator::new();

        if self.group_by.is_empty() {
            // Global aggregation (no GROUP BY)
            self.aggregate_global(batches, &evaluator)
        } else {
            // Group-by aggregation
            self.aggregate_grouped(batches, &evaluator)
        }
    }

    /// Perform global aggregation (no GROUP BY).
    fn aggregate_global(
        &self,
        batches: &[RecordBatch],
        evaluator: &ExprEvaluator,
    ) -> GrismResult<RecordBatch> {
        // Create accumulators for each aggregate
        let mut accumulators: Vec<Box<dyn Accumulator>> = self
            .aggregates
            .iter()
            .map(|agg| {
                // Determine input type (simplified: assume Int64 or check first batch)
                let input_type = DataType::Int64;
                create_accumulator(&agg.func, &input_type)
            })
            .collect();

        // Process all batches
        for batch in batches {
            for (i, agg) in self.aggregates.iter().enumerate() {
                // Evaluate the aggregate input expression
                // Handle Wildcard specially for COUNT(*)
                let values = if matches!(*agg.expr, LogicalExpr::Wildcard) {
                    // COUNT(*) - create array of all non-null values
                    Arc::new(Int64Array::from(vec![1i64; batch.num_rows()])) as ArrayRef
                } else {
                    evaluator.evaluate(&agg.expr, batch)?
                };

                accumulators[i].update(&values)?;
            }
        }

        // Build result batch
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.aggregates.len());
        for acc in &accumulators {
            columns.push(acc.finalize()?);
        }

        RecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
            .map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Perform group-by aggregation.
    fn aggregate_grouped(
        &self,
        batches: &[RecordBatch],
        evaluator: &ExprEvaluator,
    ) -> GrismResult<RecordBatch> {
        // Map from group key to accumulators
        // Group key is represented as a string for simplicity
        let mut groups: HashMap<String, Vec<Box<dyn Accumulator>>> = HashMap::new();
        let mut group_values: HashMap<String, Vec<ArrayRef>> = HashMap::new();

        // Process all batches
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Evaluate group-by expressions
            let group_cols: Vec<ArrayRef> = self
                .group_by
                .iter()
                .map(|expr| evaluator.evaluate(expr, batch))
                .collect::<GrismResult<Vec<_>>>()?;

            // Evaluate aggregate input expressions
            let agg_inputs: Vec<ArrayRef> = self
                .aggregates
                .iter()
                .map(|agg| {
                    // Handle Wildcard specially for COUNT(*)
                    if matches!(*agg.expr, LogicalExpr::Wildcard) {
                        Ok(Arc::new(Int64Array::from(vec![1i64; batch.num_rows()])) as ArrayRef)
                    } else {
                        evaluator.evaluate(&agg.expr, batch)
                    }
                })
                .collect::<GrismResult<Vec<_>>>()?;

            // Process each row
            for row in 0..batch.num_rows() {
                // Build group key
                let key = self.build_group_key(&group_cols, row);

                // Get or create accumulators for this group
                let accumulators = groups.entry(key.clone()).or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|agg| {
                            let input_type = DataType::Int64;
                            create_accumulator(&agg.func, &input_type)
                        })
                        .collect()
                });

                // Store group values for output
                if !group_values.contains_key(&key) {
                    let row_group_values: Vec<ArrayRef> = group_cols
                        .iter()
                        .map(|col| self.extract_single_value(col, row))
                        .collect::<GrismResult<Vec<_>>>()?;
                    group_values.insert(key.clone(), row_group_values);
                }

                // Update accumulators with this row's values
                for (i, input) in agg_inputs.iter().enumerate() {
                    let single_value = self.extract_single_value(input, row)?;
                    accumulators[i].update(&single_value)?;
                }
            }
        }

        // Build result batch
        self.build_grouped_result(&groups, &group_values)
    }

    /// Build a string key for a group from column values at a row.
    fn build_group_key(&self, group_cols: &[ArrayRef], row: usize) -> String {
        let mut key = String::new();
        for (i, col) in group_cols.iter().enumerate() {
            if i > 0 {
                key.push('|');
            }
            // Handle different types
            if let Some(int_arr) = col.as_any().downcast_ref::<Int64Array>() {
                if int_arr.is_null(row) {
                    key.push_str("NULL");
                } else {
                    key.push_str(&int_arr.value(row).to_string());
                }
            } else if let Some(str_arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                if str_arr.is_null(row) {
                    key.push_str("NULL");
                } else {
                    key.push_str(str_arr.value(row));
                }
            } else {
                key.push_str(&format!("{:?}", row));
            }
        }
        key
    }

    /// Extract a single value from an array as a new single-element array.
    fn extract_single_value(&self, arr: &ArrayRef, row: usize) -> GrismResult<ArrayRef> {
        use arrow::compute::take;
        let indices = Int64Array::from(vec![row as i64]);
        take(arr.as_ref(), &indices, None).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Build the result batch from grouped accumulators.
    fn build_grouped_result(
        &self,
        groups: &HashMap<String, Vec<Box<dyn Accumulator>>>,
        group_values: &HashMap<String, Vec<ArrayRef>>,
    ) -> GrismResult<RecordBatch> {
        if groups.is_empty() {
            // Return empty batch with correct schema
            return RecordBatch::try_new(self.schema.arrow_schema().clone(), vec![])
                .map_err(|e| GrismError::execution(e.to_string()));
        }

        let num_groups = groups.len();

        // Build columns for group-by values
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Add group-by columns
        for col_idx in 0..self.group_by.len() {
            let field = self.schema.arrow_schema().field(col_idx);
            match field.data_type() {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_groups);
                    for key in groups.keys() {
                        if let Some(vals) = group_values.get(key) {
                            if let Some(int_arr) =
                                vals[col_idx].as_any().downcast_ref::<Int64Array>()
                            {
                                if int_arr.is_null(0) {
                                    builder.append_null();
                                } else {
                                    builder.append_value(int_arr.value(0));
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 10);
                    for key in groups.keys() {
                        if let Some(vals) = group_values.get(key) {
                            if let Some(str_arr) = vals[col_idx]
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                            {
                                if str_arr.is_null(0) {
                                    builder.append_null();
                                } else {
                                    builder.append_value(str_arr.value(0));
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                _ => {
                    // Fallback: create null array
                    columns.push(arrow::array::new_null_array(field.data_type(), num_groups));
                }
            }
        }

        // Add aggregate columns
        for agg_idx in 0..self.aggregates.len() {
            let field = self
                .schema
                .arrow_schema()
                .field(self.group_by.len() + agg_idx);
            match field.data_type() {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_groups);
                    for accs in groups.values() {
                        let result = accs[agg_idx].finalize()?;
                        if let Some(int_arr) = result.as_any().downcast_ref::<Int64Array>() {
                            if int_arr.is_null(0) {
                                builder.append_null();
                            } else {
                                builder.append_value(int_arr.value(0));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_groups);
                    for accs in groups.values() {
                        let result = accs[agg_idx].finalize()?;
                        if let Some(float_arr) = result.as_any().downcast_ref::<Float64Array>() {
                            if float_arr.is_null(0) {
                                builder.append_null();
                            } else {
                                builder.append_value(float_arr.value(0));
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                _ => {
                    columns.push(arrow::array::new_null_array(field.data_type(), num_groups));
                }
            }
        }

        RecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
            .map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for HashAggregateExec {
    fn name(&self) -> &'static str {
        "HashAggregateExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::blocking()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = AggregateState::Accumulating;
        self.input.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        let mut state = self.state.lock().await;

        match &mut *state {
            AggregateState::Uninitialized => Err(GrismError::execution("Operator not opened")),
            AggregateState::Accumulating => {
                // Consume all input
                let mut all_batches = Vec::new();
                while let Some(batch) = self.input.next().await? {
                    all_batches.push(batch);
                }

                // Perform aggregation
                let result = self.aggregate_batches(&all_batches)?;

                if result.num_rows() == 0 {
                    *state = AggregateState::Exhausted;
                    Ok(None)
                } else {
                    *state = AggregateState::Ready(vec![]);
                    Ok(Some(result))
                }
            }
            AggregateState::Ready(batches) => {
                if batches.is_empty() {
                    *state = AggregateState::Exhausted;
                    Ok(None)
                } else {
                    Ok(Some(batches.remove(0)))
                }
            }
            AggregateState::Exhausted => Ok(None),
        }
    }

    async fn close(&self) -> GrismResult<()> {
        let mut state = self.state.lock().await;
        *state = AggregateState::Exhausted;
        self.input.close().await
    }

    fn display(&self) -> String {
        let groups: Vec<_> = self.group_by.iter().map(|e| format!("{}", e)).collect();
        let aggs: Vec<_> = self.aggregates.iter().map(|a| format!("{}", a)).collect();
        format!(
            "HashAggregateExec(group_by=[{}], agg=[{}])",
            groups.join(", "),
            aggs.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::EmptyExec;
    use grism_logical::expr::{col, lit};
    use grism_storage::{InMemoryStorage, SnapshotId};

    /// Helper to create a mock input operator that returns a single batch.
    struct MockInputOp {
        batch: Option<RecordBatch>,
        returned: tokio::sync::Mutex<bool>,
        schema: PhysicalSchema,
    }

    impl MockInputOp {
        fn new(batch: RecordBatch) -> Self {
            let schema = PhysicalSchema::new(batch.schema());
            Self {
                batch: Some(batch),
                returned: tokio::sync::Mutex::new(false),
                schema,
            }
        }
    }

    impl std::fmt::Debug for MockInputOp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockInputOp").finish()
        }
    }

    #[async_trait]
    impl PhysicalOperator for MockInputOp {
        fn name(&self) -> &'static str {
            "MockInputOp"
        }

        fn schema(&self) -> &PhysicalSchema {
            &self.schema
        }

        fn capabilities(&self) -> OperatorCaps {
            OperatorCaps::streaming()
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
            vec![]
        }

        async fn open(&self, _ctx: &ExecutionContext) -> GrismResult<()> {
            Ok(())
        }

        async fn next(&self) -> GrismResult<Option<RecordBatch>> {
            let mut returned = self.returned.lock().await;
            if *returned {
                return Ok(None);
            }
            *returned = true;
            Ok(self.batch.clone())
        }

        async fn close(&self) -> GrismResult<()> {
            Ok(())
        }

        fn display(&self) -> String {
            "MockInputOp".to_string()
        }
    }

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("group_col", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_aggregate_empty() {
        let input = Arc::new(EmptyExec::new());

        // COUNT(*) on empty input should return 0
        let aggregates = vec![AggExpr::new(AggFunc::Count, LogicalExpr::Wildcard)];
        let output_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::Int64,
            false,
        )]));

        let agg = HashAggregateExec::new(
            input,
            vec![],
            aggregates,
            PhysicalSchema::new(output_schema),
        );

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        agg.open(&ctx).await.unwrap();
        let result = agg.next().await.unwrap().unwrap();

        // Global aggregation always returns 1 row, even for empty input
        assert_eq!(result.num_rows(), 1);
        let count_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 0); // COUNT of empty = 0

        agg.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_aggregate_global_sum() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // SUM(value) with no GROUP BY
        let aggregates = vec![AggExpr::new(AggFunc::Sum, col("value").into())];

        let output_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "sum",
            DataType::Int64,
            true,
        )]));

        let agg = HashAggregateExec::new(
            input,
            vec![],
            aggregates,
            PhysicalSchema::new(output_schema),
        );

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        agg.open(&ctx).await.unwrap();
        let result = agg.next().await.unwrap().unwrap();

        assert_eq!(result.num_rows(), 1);
        let sum_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum_col.value(0), 150); // 10+20+30+40+50

        agg.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_aggregate_global_count() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // COUNT(value)
        let aggregates = vec![AggExpr::new(AggFunc::Count, col("value").into())];

        let output_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::Int64,
            false,
        )]));

        let agg = HashAggregateExec::new(
            input,
            vec![],
            aggregates,
            PhysicalSchema::new(output_schema),
        );

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        agg.open(&ctx).await.unwrap();
        let result = agg.next().await.unwrap().unwrap();

        assert_eq!(result.num_rows(), 1);
        let count_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 5);

        agg.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_aggregate_global_avg() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // AVG(value)
        let aggregates = vec![AggExpr::new(AggFunc::Avg, col("value").into())];

        let output_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "avg",
            DataType::Float64,
            true,
        )]));

        let agg = HashAggregateExec::new(
            input,
            vec![],
            aggregates,
            PhysicalSchema::new(output_schema),
        );

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        agg.open(&ctx).await.unwrap();
        let result = agg.next().await.unwrap().unwrap();

        assert_eq!(result.num_rows(), 1);
        let avg_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((avg_col.value(0) - 30.0).abs() < 0.001); // (10+20+30+40+50)/5 = 30

        agg.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_aggregate_grouped() {
        let batch = create_test_batch();
        let input: Arc<dyn PhysicalOperator> = Arc::new(MockInputOp::new(batch));

        // SUM(value) GROUP BY group_col
        let group_by = vec![col("group_col").into()];
        let aggregates = vec![AggExpr::new(AggFunc::Sum, col("value").into())];

        let output_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("group_col", DataType::Int64, false),
            Field::new("sum", DataType::Int64, true),
        ]));

        let agg = HashAggregateExec::new(
            input,
            group_by,
            aggregates,
            PhysicalSchema::new(output_schema),
        );

        let storage = Arc::new(InMemoryStorage::new());
        let ctx = ExecutionContext::new(storage, SnapshotId::default());

        agg.open(&ctx).await.unwrap();
        let result = agg.next().await.unwrap().unwrap();

        assert_eq!(result.num_rows(), 2); // Two groups

        // Groups may be in any order, so verify totals
        let group_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut results: Vec<(i64, i64)> = (0..result.num_rows())
            .map(|i| (group_col.value(i), sum_col.value(i)))
            .collect();
        results.sort_by_key(|r| r.0);

        assert_eq!(results[0], (1, 30)); // Group 1: 10+20=30
        assert_eq!(results[1], (2, 120)); // Group 2: 30+40+50=120

        agg.close().await.unwrap();
    }

    #[test]
    fn test_aggregate_capabilities() {
        let input = Arc::new(EmptyExec::new());
        let agg = HashAggregateExec::new(input, vec![], vec![], PhysicalSchema::empty());

        let caps = agg.capabilities();
        assert!(caps.blocking);
        assert!(caps.requires_global_view);
    }
}
