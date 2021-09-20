+++
title = "Querying the transformed VCF files from the previous EMR processing step"
chapter = true
weight = 450
pre = "<b>3.2 </b>"
+++

Let's check if the VCF files have been transformed. You can examine the S3 output location if parquet files are generated: 


The data is ready to be queried by Athena. We just need to create table schema for it. you can use Glue crawler to detect schema for you.

```SQL
CREATE EXTERNAL TABLE `var_part_by_sample`(
  `variant_id` string, 
  `chrom` string, 
  `pos` int, 
  `alleles` array<string>, 
  `rsid` string, 
  `qual` double, 
  `filters` array<string>, 
  `info.ac` array<int>, 
  `info.af` array<double>, 
  `info.an` int, 
  `info.db` boolean, 
  `info.dp` int, 
  `info.end` int, 
  `info.fs` double, 
  `info.fractioninformativereads` double, 
  `info.lod` double, 
  `info.mq` double, 
  `info.mqranksum` double, 
  `info.qd` double, 
  `info.r2_5p_bias` double, 
  `info.readposranksum` double, 
  `info.sor` double, 
  `ad` array<int>, 
  `af` array<double>, 
  `dp` int, 
  `fir2` array<int>, 
  `f2r1` array<int>, 
  `gp` array<double>, 
  `gq` int, 
  `gt.alleles` array<int>, 
  `gt.phased` boolean, 
  `mb` array<int>, 
  `pl` array<int>, 
  `pri` array<double>, 
  `ps` int, 
  `sb` array<int>, 
  `sq` double, 
  `sample_id` string, 
  `ref` string, 
  `alt` string)
PARTITIONED BY ( 
  `partition_0` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://aws-roda-hcls-datalake/thousandgenomes_dragen/var_partby_samples/'
;

```


For certain analysis, you may care mostly just variants, not the details of samples. To further optimize query performance for such analysis, we could nested sample data into variants using below query. **Note you need to change the s3 bucket to one of your own**.
This can greatly reduce the record count and speedup analysis. In this data model, the record count is reduced from ~16.9 billion to ~160 million.

```SQL
create  table var_nested
with (
  external_location = 's3://genomics_datalake/var_nested/',
  format = 'ORC',
  partitioned_by = array['chrom'],
  bucketed_by = ARRAY['pos'], 
  bucket_count = 60,
  )
as
select variant_id, pos, ref, alt,  
array_agg(CAST(ROW(sample_id, "gt.alleles") AS ROW(id VARCHAR, gts ARRAY(INTEGER)))) as samples,
chrom
from var_part_by_chrom 
group by 1,2,3,4,6

```

{{% notice note %}}
Replace the value for external location S3 path to a location in your own account. 
{{% /notice %}}

