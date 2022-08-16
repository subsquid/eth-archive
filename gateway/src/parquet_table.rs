use arrow::array::{
    Array, ArrayBuilder, ArrayRef, Date64Array, Date64Builder, StringArray, StringBuilder,
    UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::get_statistics_with_limit;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError as Error;
use datafusion::execution::context::SessionState;
use datafusion::logical_plan::{self, ExprVisitable, ExpressionVisitor, Recursion};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::{project_schema, ExecutionPlan, Statistics};
use datafusion::prelude::{Expr, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion_expr::Volatility;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::any::Any;
use std::path::PathBuf as StdPath;
use std::result::Result as StdResult;
use std::sync::Arc;
use tokio::fs;
use url::Url;

type Result<T> = StdResult<T, Error>;

pub struct ParquetTable {
    mem_table: Arc<MemTable>,
    table_path: Url,
    options: ParquetTableOptions,
    table_schema: SchemaRef,
}

impl ParquetTable {
    pub async fn new(options: ParquetTableOptions) -> Result<Self> {
        use parquet::arrow::ParquetRecordBatchStreamBuilder;
        use std::ffi::OsStr;
        use tokio_stream::wrappers::ReadDirStream;

        let table_path =
            Url::parse(&options.table_path).map_err(|e| Error::External(Box::new(e)))?;

        let dir = fs::read_dir(table_path.path())
            .await
            .map_err(|e| Error::External(Box::new(e)))?;

        let mut dir = ReadDirStream::new(dir);

        let mut table_schema = None;
        let mut cols = None;

        while let Some(entry) = dir.next().await {
            let entry = entry.map_err(|e| Error::External(Box::new(e)))?;

            if entry.path().extension() != Some(OsStr::new("parquet")) {
                continue;
            }

            let file = fs::File::open(&entry.path())
                .await
                .map_err(|e| Error::External(Box::new(e)))?;
            let builder = ParquetRecordBatchStreamBuilder::new(file)
                .await
                .unwrap()
                .with_batch_size(3);
            let schema = Arc::clone(builder.schema());

            cols = Some(
                schema
                    .fields
                    .iter()
                    .enumerate()
                    .filter_map(|(i, field)| {
                        if !options.table_partition_cols.contains(field.name()) {
                            None
                        } else {
                            Some(i)
                        }
                    })
                    .collect::<Vec<_>>(),
            );

            table_schema = Some(schema);

            break;
        }

        let table_schema = table_schema.ok_or_else(|| {
            Error::External(Box::new(
                crate::error::Error::NoParquetFileFoundInDirectory(table_path.to_string()),
            ))
        })?;

        let cols = cols.ok_or_else(|| {
            Error::External(Box::new(
                crate::error::Error::NoParquetFileFoundInDirectory(table_path.to_string()),
            ))
        })?;

        let data = todo!();

        let mut mem_table_schema_fields = vec![
            Field::new(FILE_PATH_COLUMN_NAME, DataType::Utf8, false),
            Field::new(FILE_SIZE_COLUMN_NAME, DataType::UInt64, false),
            Field::new(FILE_MODIFIED_COLUMN_NAME, DataType::Date64, true),
        ];

        let mem_table_schema = Arc::new(Schema::new(mem_table_schema_fields));

        let mem_table = Arc::new(MemTable::try_new(mem_table_schema, data)?);

        Ok(Self {
            mem_table,
            table_path,
            options,
            table_schema,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_list, statistics) =
            self.list_files_for_scan(ctx, filters, limit).await?;

        if partitioned_file_list.is_empty() {
            let schema = self.schema();
            let projected_schema = project_schema(&schema, projection.as_ref())?;
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        self.options
            .format
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::parse(&self.table_path)?,
                    file_schema: self.schema(),
                    file_groups: partitioned_file_list,
                    statistics,
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: self.options.table_partition_cols.clone(),
                },
                filters,
            )
            .await
    }
}

impl ParquetTable {
    async fn list_files_for_scan(
        &self,
        ctx: &SessionState,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        let store = ctx.runtime_env.object_store(WrappedUrl(&self.table_path))?;

        let file_list = self.pruned_partition_list(store.as_ref(), filters).await?;

        let files = file_list.then(|part_file| async {
            let part_file = part_file?;

            Ok((part_file, Statistics::default())) as Result<(PartitionedFile, Statistics)>
        });

        let (files, statistics) = get_statistics_with_limit(files, self.schema(), limit).await?;

        Ok((
            split_files(files, self.options.target_partitions),
            statistics,
        ))
    }

    async fn list_all_files(&self) -> Result<BoxStream<Result<PartitionedFile>>> {
        let ctx = SessionContext::new();
        let data_frame = ctx.read_table(self.mem_table.clone())?;
        let filtered_batches = data_frame.collect().await?;
        let paths = batches_to_paths(&filtered_batches)?;

        Ok(Box::pin(futures::stream::iter(paths.into_iter().map(Ok))))
    }

    async fn pruned_partition_list(
        &self,
        store: &dyn ObjectStore,
        filters: &[Expr],
    ) -> Result<BoxStream<Result<PartitionedFile>>> {
        let table_partition_cols = &self.options.table_partition_cols;

        if table_partition_cols.is_empty() {
            return self.list_all_files().await;
        }

        let applicable_filters = filters
            .iter()
            .filter(|f| expr_applicable_for_cols(table_partition_cols, f))
            .collect::<Vec<_>>();

        if applicable_filters.is_empty() {
            return self.list_all_files().await;
        }

        let ctx = SessionContext::new();
        let mut data_frame = ctx.read_table(self.mem_table.clone())?;
        for filter in applicable_filters {
            data_frame = data_frame.filter(filter.clone())?;
        }
        let filtered_batches = data_frame.collect().await?;
        let paths = batches_to_paths(&filtered_batches)?;

        Ok(Box::pin(futures::stream::iter(paths.into_iter().map(Ok))))
    }
}

pub fn expr_applicable_for_cols(col_names: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.accept(ApplicabilityVisitor {
        col_names,
        is_applicable: &mut is_applicable,
    })
    .unwrap();
    is_applicable
}

fn batches_to_paths(batches: &[RecordBatch]) -> Result<Vec<PartitionedFile>> {
    batches
        .iter()
        .flat_map(|batch| {
            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let length_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let modified_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap();

            (0..batch.num_rows()).map(move |row| {
                Ok(PartitionedFile {
                    object_meta: ObjectMeta {
                        location: Path::parse(key_array.value(row))
                            .map_err(|e| Error::External(Box::new(e)))?,
                        last_modified: Utc.timestamp_millis(modified_array.value(row)),
                        size: length_array.value(row) as usize,
                    },
                    partition_values: (3..batch.columns().len())
                        .map(|col| ScalarValue::try_from_array(batch.column(col), row).unwrap())
                        .collect(),
                    range: None,
                    extensions: None,
                })
            })
        })
        .collect()
}

pub fn split_files(partitioned_files: Vec<PartitionedFile>, n: usize) -> Vec<Vec<PartitionedFile>> {
    if partitioned_files.is_empty() {
        return vec![];
    }
    // effectively this is div with rounding up instead of truncating
    let chunk_size = (partitioned_files.len() + n - 1) / n;
    partitioned_files
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect()
}

const FILE_SIZE_COLUMN_NAME: &str = "_df_part_file_size_";
const FILE_PATH_COLUMN_NAME: &str = "_df_part_file_path_";
const FILE_MODIFIED_COLUMN_NAME: &str = "_df_part_file_modified_";

/// The `ExpressionVisitor` for `expr_applicable_for_cols`. Walks the tree to
/// validate that the given expression is applicable with only the `col_names`
/// set of columns.
struct ApplicabilityVisitor<'a> {
    col_names: &'a [String],
    is_applicable: &'a mut bool,
}

impl ApplicabilityVisitor<'_> {
    fn visit_volatility(self, volatility: Volatility) -> Recursion<Self> {
        match volatility {
            Volatility::Immutable => Recursion::Continue(self),
            // TODO: Stable functions could be `applicable`, but that would require access to the context
            Volatility::Stable | Volatility::Volatile => {
                *self.is_applicable = false;
                Recursion::Stop(self)
            }
        }
    }
}

impl ExpressionVisitor for ApplicabilityVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        let rec = match expr {
            Expr::Column(logical_plan::Column { ref name, .. }) => {
                *self.is_applicable &= self.col_names.contains(name);
                Recursion::Stop(self) // leaf node anyway
            }
            Expr::Literal(_)
            | Expr::Alias(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::BinaryExpr { .. }
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::GetIndexedField { .. }
            | Expr::GroupingSet(_)
            | Expr::Case { .. } => Recursion::Continue(self),

            Expr::ScalarFunction { fun, .. } => self.visit_volatility(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => self.visit_volatility(fun.signature.volatility),

            // TODO other expressions are not handled yet:
            // - AGGREGATE, WINDOW and SORT should not end up in filter conditions, except maybe in some edge cases
            // - Can `Wildcard` be considered as a `Literal`?
            // - ScalarVariable could be `applicable`, but that would require access to the context
            Expr::AggregateUDF { .. }
            | Expr::AggregateFunction { .. }
            | Expr::Sort { .. }
            | Expr::WindowFunction { .. }
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => {
                *self.is_applicable = false;
                Recursion::Stop(self)
            }
        };
        Ok(rec)
    }
}

pub struct ParquetTableOptions {
    target_partitions: usize,
    table_partition_cols: Vec<String>,
    format: Arc<dyn FileFormat>,
    table_path: String,
}

struct WrappedUrl<'a>(&'a Url);

impl<'a> AsRef<Url> for WrappedUrl<'a> {
    fn as_ref(&self) -> &Url {
        self.0
    }
}
