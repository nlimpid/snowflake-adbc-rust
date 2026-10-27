//! `GetObjects` over `INFORMATION_SCHEMA`, nested into the ADBC struct layout.

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::options::ObjectDepth;
use arrow_array::{ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use snowflake_arrow::ArrowOptions;
use snowflake_core::Session;

use crate::error::{self, adbc_error};

#[allow(clippy::too_many_arguments)]
pub fn objects_reader(
    session: &Session,
    handle: &tokio::runtime::Handle,
    opts: &ArrowOptions,
    depth: ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<Vec<&str>>,
    column_name: Option<&str>,
) -> adbc_core::error::Result<Box<dyn arrow_array::RecordBatchReader + Send + 'static>> {
    let want_schemas = !matches!(depth, ObjectDepth::Catalogs);
    let want_tables = matches!(
        depth,
        ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
    );
    let want_columns = matches!(depth, ObjectDepth::Columns | ObjectDepth::All);

    let mut catalogs: BTreeMap<String, BTreeMap<String, BTreeMap<String, Table>>> = BTreeMap::new();
    let catalog_rows = query_rows(
        session,
        handle,
        opts,
        &format!(
            "SELECT database_name::varchar FROM information_schema.databases WHERE 1 = 1{}",
            like_predicate("database_name", catalog)
        ),
    )?;
    for row in catalog_rows {
        if let Some(name) = row.first().and_then(Option::as_ref) {
            catalogs.entry(name.clone()).or_default();
        }
    }
    if want_schemas {
        let rows = query_rows(
            session,
            handle,
            opts,
            &format!(
                "SELECT catalog_name::varchar, schema_name::varchar \
                 FROM information_schema.schemata WHERE 1 = 1{}{}",
                like_predicate("catalog_name", catalog),
                like_predicate("schema_name", db_schema)
            ),
        )?;
        for row in rows {
            let Some(catalog_name) = row.first().and_then(Option::as_ref) else {
                continue;
            };
            let Some(schema_name) = row.get(1).and_then(Option::as_ref) else {
                continue;
            };
            catalogs
                .entry(catalog_name.clone())
                .or_default()
                .entry(schema_name.clone())
                .or_default();
        }
    }
    if want_tables {
        let type_filter = match table_type {
            Some(types) if !types.is_empty() => format!(
                " AND table_type IN ({})",
                types
                    .iter()
                    .map(|value| sql_string(value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => String::new(),
        };
        let rows = query_rows(
            session,
            handle,
            opts,
            &format!(
                "SELECT table_catalog::varchar, table_schema::varchar, table_name::varchar, table_type::varchar \
                 FROM information_schema.tables WHERE 1 = 1{}{}{}{type_filter}",
                like_predicate("table_catalog", catalog),
                like_predicate("table_schema", db_schema),
                like_predicate("table_name", table_name),
            ),
        )?;
        for row in rows {
            let (Some(catalog_name), Some(schema_name), Some(name), Some(ty)) = (
                row.first().and_then(Option::as_ref),
                row.get(1).and_then(Option::as_ref),
                row.get(2).and_then(Option::as_ref),
                row.get(3).and_then(Option::as_ref),
            ) else {
                continue;
            };
            catalogs
                .entry(catalog_name.clone())
                .or_default()
                .entry(schema_name.clone())
                .or_default()
                .insert(
                    name.clone(),
                    Table {
                        ty: ty.clone(),
                        columns: if want_columns { Some(Vec::new()) } else { None },
                    },
                );
        }
    }
    if want_columns {
        let rows = query_rows(
            session,
            handle,
            opts,
            &format!(
                "SELECT table_catalog::varchar, table_schema::varchar, table_name::varchar, \
                        column_name::varchar, ordinal_position::varchar, is_nullable::varchar, \
                        data_type::varchar, comment::varchar \
                 FROM information_schema.columns WHERE 1 = 1{}{}{}{}",
                like_predicate("table_catalog", catalog),
                like_predicate("table_schema", db_schema),
                like_predicate("table_name", table_name),
                like_predicate("column_name", column_name),
            ),
        )?;
        for row in rows {
            let Some(catalog_name) = row.first().and_then(Option::as_ref) else {
                continue;
            };
            let Some(schema_name) = row.get(1).and_then(Option::as_ref) else {
                continue;
            };
            let Some(table) = row.get(2).and_then(Option::as_ref) else {
                continue;
            };
            let Some(column) = row.get(3).and_then(Option::as_ref) else {
                continue;
            };
            let ordinal = row
                .get(4)
                .and_then(Option::as_ref)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let entry = catalogs
                .entry(catalog_name.clone())
                .or_default()
                .entry(schema_name.clone())
                .or_default()
                .entry(table.clone())
                .or_insert_with(|| Table {
                    ty: "TABLE".into(),
                    columns: Some(Vec::new()),
                });
            if let Some(columns) = entry.columns.as_mut() {
                columns.push(Column {
                    name: column.clone(),
                    ordinal,
                    nullable: row.get(5).and_then(Clone::clone),
                    data_type: row.get(6).and_then(Clone::clone),
                    comment: row.get(7).and_then(Clone::clone),
                });
            }
        }
    }

    let batch = build_objects(&catalogs, want_schemas, want_tables, want_columns)?;
    Ok(Box::new(crate::info_reader_once(batch)))
}

struct Table {
    ty: String,
    columns: Option<Vec<Column>>,
}

struct Column {
    name: String,
    ordinal: i32,
    nullable: Option<String>,
    data_type: Option<String>,
    comment: Option<String>,
}

fn build_objects(
    catalogs: &BTreeMap<String, BTreeMap<String, BTreeMap<String, Table>>>,
    want_schemas: bool,
    want_tables: bool,
    want_columns: bool,
) -> adbc_core::error::Result<RecordBatch> {
    let column_field = column_struct_field();
    let table_field = table_struct_field(column_field.clone());
    let schema_field = schema_struct_field(table_field.clone());

    let mut catalog_names = Vec::new();
    let mut schema_names = Vec::new();
    let mut schema_nulls = Vec::new();
    let mut schema_offsets = vec![0i32];
    let mut table_names = Vec::new();
    let mut table_types = Vec::new();
    let mut table_nulls = Vec::new();
    let mut table_offsets = vec![0i32];
    let mut column_names = Vec::new();
    let mut ordinals = Vec::new();
    let mut remarks = Vec::new();
    let mut type_names = Vec::new();
    let mut is_nullable = Vec::new();
    let mut column_nulls = Vec::new();
    let mut column_offsets = vec![0i32];

    for (catalog, schemas) in catalogs {
        catalog_names.push(Some(catalog.as_str()));
        if !want_schemas {
            schema_nulls.push(false);
            schema_offsets.push(*schema_offsets.last().unwrap_or(&0));
            continue;
        }
        schema_nulls.push(true);
        for (schema, tables) in schemas {
            schema_names.push(Some(schema.as_str()));
            if !want_tables {
                table_nulls.push(false);
                table_offsets.push(*table_offsets.last().unwrap_or(&0));
                continue;
            }
            table_nulls.push(true);
            for (table, meta) in tables {
                table_names.push(Some(table.as_str()));
                table_types.push(Some(meta.ty.as_str()));
                match &meta.columns {
                    None => {
                        column_nulls.push(false);
                        column_offsets.push(*column_offsets.last().unwrap_or(&0));
                    }
                    Some(columns) => {
                        column_nulls.push(true);
                        for column in columns {
                            column_names.push(Some(column.name.as_str()));
                            ordinals.push(Some(column.ordinal));
                            remarks.push(column.comment.as_deref());
                            type_names.push(column.data_type.as_deref());
                            is_nullable.push(column.nullable.as_deref());
                        }
                        column_offsets.push(column_names.len() as i32);
                    }
                }
            }
            table_offsets.push(table_names.len() as i32);
        }
        schema_offsets.push(schema_names.len() as i32);
    }

    let columns = if want_columns {
        Some(struct_array(
            column_field.clone(),
            vec![
                utf8(column_names),
                Arc::new(Int32Array::from(ordinals)),
                utf8(remarks),
                nulls(DataType::Int16, column_names_len(&column_offsets)),
                utf8(type_names),
                nulls(DataType::Int32, column_names_len(&column_offsets)),
                nulls(DataType::Int16, column_names_len(&column_offsets)),
                nulls(DataType::Int16, column_names_len(&column_offsets)),
                nulls(DataType::Int16, column_names_len(&column_offsets)),
                nulls(DataType::Utf8, column_names_len(&column_offsets)),
                nulls(DataType::Int16, column_names_len(&column_offsets)),
                nulls(DataType::Int16, column_names_len(&column_offsets)),
                nulls(DataType::Int32, column_names_len(&column_offsets)),
                utf8(is_nullable),
                nulls(DataType::Utf8, column_names_len(&column_offsets)),
                nulls(DataType::Utf8, column_names_len(&column_offsets)),
                nulls(DataType::Utf8, column_names_len(&column_offsets)),
                nulls(DataType::Boolean, column_names_len(&column_offsets)),
                nulls(DataType::Boolean, column_names_len(&column_offsets)),
            ],
        )?)
    } else {
        None
    };
    let column_lists = list_or_null(
        Arc::new(column_field),
        columns,
        &column_offsets,
        &column_nulls,
    )?;
    let _ = want_columns;
    let tables = struct_array(
        table_field.clone(),
        vec![
            utf8(table_names),
            utf8(table_types),
            column_lists,
            list_or_null(
                Arc::new(Field::new(
                    "table_constraints",
                    DataType::List(constraint_field()),
                    true,
                )),
                None,
                &vec![0; table_nulls.len() + 1],
                &vec![false; table_nulls.len()],
            )?,
        ],
    )?;
    let table_lists = list_or_null(
        Arc::new(table_field),
        Some(tables),
        &table_offsets,
        &table_nulls,
    )?;
    let schemas = struct_array(schema_field.clone(), vec![utf8(schema_names), table_lists])?;
    let schema_lists = list_or_null(
        Arc::new(schema_field.clone()),
        Some(schemas),
        &schema_offsets,
        &schema_nulls,
    )?;
    let catalog_struct_values = struct_array(
        Field::new(
            "catalog",
            DataType::Struct(Fields::from(vec![
                Field::new("catalog_name", DataType::Utf8, true),
                Field::new(
                    "catalog_db_schemas",
                    DataType::List(Arc::new(schema_field.clone())),
                    true,
                ),
            ])),
            false,
        ),
        vec![utf8(catalog_names), schema_lists],
    )?;
    // The GetObjects root is not itself a struct column; it is two top-level fields.
    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            DataType::List(Arc::new(schema_field)),
            true,
        ),
    ]));
    let as_struct = catalog_struct_values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| error::invalid("internal: catalog value is not a struct"))?;
    let names = as_struct.column(0).clone();
    let lists = as_struct.column(1).clone();
    RecordBatch::try_new(schema, vec![names, lists]).map_err(|err| error::invalid(err.to_string()))
}

fn column_names_len(offsets: &[i32]) -> usize {
    offsets.last().copied().unwrap_or(0) as usize
}

fn column_struct_field() -> Field {
    Field::new(
        "column",
        DataType::Struct(Fields::from(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("xdbc_data_type", DataType::Int16, true),
            Field::new("xdbc_type_name", DataType::Utf8, true),
            Field::new("xdbc_column_size", DataType::Int32, true),
            Field::new("xdbc_decimal_digits", DataType::Int16, true),
            Field::new("xdbc_num_prec_radix", DataType::Int16, true),
            Field::new("xdbc_nullable", DataType::Int16, true),
            Field::new("xdbc_column_def", DataType::Utf8, true),
            Field::new("xdbc_sql_data_type", DataType::Int16, true),
            Field::new("xdbc_datetime_sub", DataType::Int16, true),
            Field::new("xdbc_char_octet_length", DataType::Int32, true),
            Field::new("xdbc_is_nullable", DataType::Utf8, true),
            Field::new("xdbc_scope_catalog", DataType::Utf8, true),
            Field::new("xdbc_scope_schema", DataType::Utf8, true),
            Field::new("xdbc_scope_table", DataType::Utf8, true),
            Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
            Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
        ])),
        false,
    )
}

fn table_struct_field(column_field: Field) -> Field {
    Field::new(
        "table",
        DataType::Struct(Fields::from(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new(
                "table_columns",
                DataType::List(Arc::new(column_field)),
                true,
            ),
            Field::new(
                "table_constraints",
                DataType::List(constraint_field()),
                true,
            ),
        ])),
        false,
    )
}

fn schema_struct_field(table_field: Field) -> Field {
    Field::new(
        "db_schema",
        DataType::Struct(Fields::from(vec![
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new(
                "db_schema_tables",
                DataType::List(Arc::new(table_field)),
                true,
            ),
        ])),
        false,
    )
}

fn constraint_field() -> FieldRef {
    Arc::new(Field::new(
        "constraint",
        DataType::Struct(Fields::from(vec![
            Field::new("constraint_name", DataType::Utf8, true),
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new(
                "constraint_column_names",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new(
                "constraint_column_usage",
                DataType::List(Arc::new(Field::new(
                    "usage",
                    DataType::Struct(Fields::from(vec![
                        Field::new("fk_catalog", DataType::Utf8, true),
                        Field::new("fk_db_schema", DataType::Utf8, true),
                        Field::new("fk_table", DataType::Utf8, false),
                        Field::new("fk_column_name", DataType::Utf8, false),
                    ])),
                    false,
                ))),
                true,
            ),
        ])),
        false,
    ))
}

fn struct_array(field: Field, columns: Vec<ArrayRef>) -> adbc_core::error::Result<ArrayRef> {
    let DataType::Struct(fields) = field.data_type().clone() else {
        return Err(error::invalid("internal: expected struct field"));
    };
    if fields.len() != columns.len() {
        return Err(error::invalid(format!(
            "struct {} has {} fields and {} columns",
            field.name(),
            fields.len(),
            columns.len()
        )));
    }
    let array = StructArray::try_new(fields, columns, None)
        .map_err(|err| error::invalid(err.to_string()))?;
    Ok(Arc::new(array))
}

fn list_or_null(
    field: FieldRef,
    values: Option<ArrayRef>,
    offsets: &[i32],
    validity: &[bool],
) -> adbc_core::error::Result<ArrayRef> {
    let len = validity.len();
    let offsets = if offsets.len() == len + 1 {
        offsets.to_vec()
    } else {
        vec![0; len + 1]
    };
    let child_type = match field.data_type() {
        DataType::List(child) => child.clone(),
        other => {
            return Err(error::invalid(format!(
                "list field {} has type {other:?}",
                field.name()
            )));
        }
    };
    let values = values.unwrap_or_else(|| arrow_array::new_empty_array(child_type.data_type()));
    let nulls = if validity.iter().all(|valid| *valid) {
        None
    } else {
        Some(NullBuffer::from(validity.to_vec()))
    };
    let list = ListArray::try_new(child_type, OffsetBuffer::new(offsets.into()), values, nulls)
        .map_err(|err| error::invalid(format!("list {}: {err}", field.name())))?;
    Ok(Arc::new(list))
}

fn utf8(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn nulls(data_type: DataType, len: usize) -> ArrayRef {
    arrow_array::new_null_array(&data_type, len)
}

fn query_rows(
    session: &Session,
    handle: &tokio::runtime::Handle,
    opts: &ArrowOptions,
    sql: &str,
) -> adbc_core::error::Result<Vec<Vec<Option<String>>>> {
    let result = handle
        .block_on(session.query(sql).execute())
        .map_err(adbc_error)?;
    let batches = handle
        .block_on(snowflake_arrow::collect(result, opts.clone()))
        .map_err(adbc_error)?;
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for column in 0..batch.num_columns() {
                cells.push(cell_string(batch.column(column).as_ref(), row));
            }
            rows.push(cells);
        }
    }
    Ok(rows)
}

fn cell_string(array: &dyn arrow_array::Array, row: usize) -> Option<String> {
    use arrow_array::cast::AsArray;
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row).to_string()),
        _ => None,
    }
}

fn like_predicate(column: &str, pattern: Option<&str>) -> String {
    match pattern {
        None => String::new(),
        Some("") => format!(" AND {column} IS NULL"),
        Some(pattern) => format!(" AND {column} LIKE {}", sql_string(pattern)),
    }
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
