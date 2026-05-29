use std::{fs::File, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, UInt32Array, UInt64Array,
    builder::FixedSizeBinaryBuilder,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use clap::Parser;

use arrow_flight_s3_mvp::util::{parse_size, pretty_bytes, throughput};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_parser = parse_size)]
    target_size: usize,

    #[arg(long, default_value = "data/test.arrow")]
    output: PathBuf,

    #[arg(long, default_value_t = 65_536)]
    rows_per_batch: usize,

    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.payload_bytes == 0 {
        anyhow::bail!("--payload-bytes must be greater than 0");
    }

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let schema = schema(args.payload_bytes);
    let file = File::create(&args.output)
        .with_context(|| format!("failed to create {:?}", args.output))?;
    let mut writer = StreamWriter::try_new(file, &schema)?;

    let row_width = 8 + 8 + 8 + 4 + args.payload_bytes;
    let target_rows = (args.target_size / row_width).max(1);
    let mut written_rows = 0usize;
    let mut batches = 0usize;
    let started = Instant::now();

    while written_rows < target_rows {
        let rows = args.rows_per_batch.min(target_rows - written_rows);
        let batch = make_batch(
            schema.clone(),
            written_rows as u64,
            rows,
            args.payload_bytes,
        )?;
        writer.write(&batch)?;
        written_rows += rows;
        batches += 1;
    }

    writer.finish()?;
    let bytes = std::fs::metadata(&args.output)?.len();
    let elapsed = started.elapsed();

    println!("generated={}", args.output.display());
    println!("rows={written_rows}");
    println!("batches={batches}");
    println!("file_bytes={bytes}");
    println!("file_size={}", pretty_bytes(bytes));
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("throughput={}", throughput(bytes, elapsed));

    Ok(())
}

fn schema(payload_bytes: usize) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("bucket", DataType::UInt32, false),
        Field::new(
            "payload",
            DataType::FixedSizeBinary(payload_bytes as i32),
            false,
        ),
    ]))
}

fn make_batch(
    schema: SchemaRef,
    start: u64,
    rows: usize,
    payload_bytes: usize,
) -> Result<RecordBatch> {
    let ids = UInt64Array::from_iter_values(start..start + rows as u64);
    let ts = Int64Array::from_iter_values((0..rows).map(|i| (start + i as u64) as i64 * 1_000));
    let values = Float64Array::from_iter_values(
        (0..rows).map(|i| ((start + i as u64) % 10_000) as f64 * 0.001),
    );
    let buckets =
        UInt32Array::from_iter_values((0..rows).map(|i| ((start as usize + i) % 1024) as u32));

    let mut payload = FixedSizeBinaryBuilder::with_capacity(rows, payload_bytes as i32);
    let mut bytes = vec![0u8; payload_bytes];
    for row in 0..rows {
        let value = start + row as u64;
        let width = payload_bytes.min(8);
        bytes[..width].copy_from_slice(&value.to_le_bytes()[..width]);
        if payload_bytes > 8 {
            bytes[8..].fill((value as u8).wrapping_mul(31));
        }
        payload.append_value(&bytes)?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids),
        Arc::new(ts),
        Arc::new(values),
        Arc::new(buckets),
        Arc::new(payload.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}
