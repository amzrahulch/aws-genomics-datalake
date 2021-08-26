-- sample use case to demo how to use SQL for genomics analysis
-- http://www.cureffi.org/2014/04/24/converting-genetic-variants-to-their-minimal-representation/
-- https://hbctraining.github.io/In-depth-NGS-Data-Analysis-Course/sessionVI/lessons/03_annotation-snpeff.html

-- Flatten version 1: s3://juayu-sampledata/geno_dataset/var_partby_samples/
-- partitioned by sample name, stored in parquet
-- Please use glue crawler to create the table

/*
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
  's3://juayu-sampledata/geno_dataset/var_partby_samples/'
;
*/


-- Flatten version 2: s3://juayu-sampledata/geno_dataset/var_partby_chrom/var_sortby/
-- partitioned by chromosome, stored in ORC
-- after creating table in Athena, run
-- msck repair table var_part_by_chrom to discover partitions
CREATE EXTERNAL TABLE `var_part_by_chrom`(
  `variant_id` string, 
  `pos` int, 
  `ref` string, 
  `alt` string, 
  `sample_id` string, 
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
  `sq` double)
PARTITIONED BY ( 
  `chrom` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://juayu-sampledata/geno_dataset/var_partby_chrom/var_sortby/'
;

-- Nested version: s3://juayu-sampledata/geno_dataset/var_nested/
-- partitioned by chromosome, stored in ORC
-- after creating table in Athena, run
-- msck repair table var_nested to discover partitions
CREATE EXTERNAL TABLE `var_nested`(
  `variant_id` string COMMENT '', 
  `pos` int COMMENT '', 
  `ref` string COMMENT '', 
  `alt` string COMMENT '', 
  `samples` array<struct<id:string,gts:array<int>>> COMMENT '')
PARTITIONED BY ( 
  `chrom` string COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3://juayu-sampledata/geno_dataset/var_nested/'
;

-- Clinvar dataset
CREATE EXTERNAL TABLE `variant_summary`(
  `alleleid` bigint, 
  `type` string, 
  `name` string, 
  `geneid` bigint, 
  `genesymbol` string, 
  `hgnc_id` string, 
  `clinicalsignificance` string, 
  `clinsigsimple` bigint, 
  `lastevaluated` string, 
  `rcvaccession` string, 
  `phenotypeids` string, 
  `phenotypelist` string, 
  `origin` string, 
  `originsimple` string, 
  `assembly` string, 
  `chromosomeaccession` string, 
  `chromosome` string, 
  `start` bigint, 
  `stop` bigint, 
  `referenceallele` string, 
  `alternateallele` string, 
  `cytogenetic` string, 
  `reviewstatus` string, 
  `numbersubmitters` bigint, 
  `guidelines` string, 
  `testedingtr` string, 
  `otherids` string, 
  `submittercategories` bigint, 
  `variationid` bigint, 
  `positionvcf` bigint, 
  `referenceallelevcf` string, 
  `alternateallelevcf` string, 
  `rsid_dbsnp` bigint, 
  `nsv_esv_dbvar` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://aws-roda-hcls-datalake/clinvar_summary_variants/variant_summary/'
;

-- View to cleanup and build variant_id that will be used to join with variant data.
create or replace view clinvar as
select concat('chr', chromosome, ':', referenceallelevcf, ':', alternateallelevcf, ':', cast(positionvcf as varchar)) as variant_id
       , chromosome as chrom
       , positionvcf as pos
       , referenceallelevcf as ref
       , alternateallelevcf as alt
       , genesymbol as genename 
       , clinicalsignificance as clinvar_clnsig
       , split(phenotypeids, ',') as phenotypeids
from variant_summary where assembly = 'GRCh38'
-- and clinsigsimple <> -1
and referenceallelevcf <> 'na' and alternateallelevcf <> 'na'
;



-- Q3 + chrom filter
select count(1) from var_part_by_chrom as v join clinvar as a on v.variant_id = a.variant_id 
where v.chrom = 'chr1' and v.sample_id ='HG00157';

select count(1) from var_partby_samples as v join clinvar as a on v.variant_id = a.variant_id 
where v.chrom = 'chr1' and v.partition_0 ='HG00157';


-- Q3  Query benefit from sample partition, 3s
select count(1) from var_part_by_chrom as v join clinvar as a on v.variant_id = a.variant_id 
where v.sample_id ='HG00157';

select count(1) from var_partby_samples as v join clinvar as a on v.variant_id = a.variant_id 
where v.partition_0 ='HG00157';


-- Q8 Query benefit from sample partition, 3s
select count(1) from var_part_by_chrom as v join clinvar as a on v.variant_id = a.variant_id 
where (a.clinvar_clnsig='Pathogenic' or clinvar_clnsig='Likely_pathogenic') 
      AND v.sample_id ='HG00157';
          
select count(1) from var_partby_samples as v join clinvar as a on v.variant_id = a.variant_id 
where (a.clinvar_clnsig='Pathogenic' or clinvar_clnsig='Likely_pathogenic') 
      AND v.partition_0 ='HG00157';


-- Q2  Query pattern that benefit from chrom partition, 2.5s
select count(DISTINCT(sample_id)) from var_part_by_chrom 
where chrom='chr1' AND pos between 1434595 and 1435147;

select count(DISTINCT(sample_id)) from var_partby_samples 
where chrom='chr1' AND pos between 1434595 and 1435147;


-- Query benefit from nested structure
-- Q5   nested, 5s
select count(1) from var_nested as v join clinvar as a 
  on v.variant_id = a.variant_id where a.clinvar_clnsig='Pathogenic';

select count(distinct v.variant_id) from var_part_by_chrom as v join clinvar as a 
on v.variant_id = a.variant_id where a.clinvar_clnsig='Pathogenic';

-- Q12
select count(1) from var_nested as v join clinvar as a on v.variant_id = a.variant_id 
where clinvar_clnsig <> 'Likely_benign' and cardinality(filter(a.phenotypeids, pt->pt in ('MONDO:MONDO:0012127'))) >  0;

select count(distinct v.variant_id) from var_part_by_chrom as v join clinvar as a on v.variant_id = a.variant_id 
where clinvar_clnsig <> 'Likely_benign' and cardinality(filter(phenotypeids, pt->pt in ('MONDO:MONDO:0012127'))) >  0;

select DISTINCT(sample.id) from (
  select samples from var_nested 
  where chrom='chr1' 
  and pos between 9033567 and 9142000
  and ref = 'G'
  and alt = 'T') as f, 
unnest(f.samples) as s(sample)
where array_join(sample.gts, '|') in ('0|1') 
order by 1;

select distinct(sample_id) from var_part_by_chrom
where chrom='chr1' 
  and pos between 9033567 and 9142000
  and ref = 'G'
  and alt = 'T'
  and array_join("gt.alleles", '|') in ('0|1') 
order by 1;


-- Q13, sample partition, 4s, chrom partition bucket, 6.5s, nested  85s
with a as (
  select sample.id as sampleID from ( select samples from var_nested as v join clinvar as a on v.variant_id = a.variant_id 
  where cardinality(filter(a.phenotypeids, pt->pt in ('MONDO:MONDO:0012127'))) >  0) as f,unnest(f.samples) as s(sample))
select count(1) from a where sampleID  ='HG00157';

select count(1) from var_part_by_chrom as v join clinvar as a on v.variant_id = a.variant_id 
where v.sample_id = 'HG00157' and cardinality(filter(phenotypeids, pt->pt in ('MONDO:MONDO:0012127'))) >  0;

select count(1) from var_partby_samples as v join clinvar as a on v.variant_id = a.variant_id 
where v.partition_0 ='HG00157' and cardinality(filter(phenotypeids, pt->pt in ('MONDO:MONDO:0012127'))) >  0;


-- Q1 always slow
select count(distinct sample_id) from var_part_by_chrom as v join clinvar as a 
  on v.variant_id = a.variant_id 
where a.genename='BRCA1' AND clinvar_clnsig='Pathogenic';

select count(distinct sample_id) from var_partby_samples as v join clinvar as a 
  on v.variant_id = a.variant_id 
where a.genename='BRCA1' AND clinvar_clnsig='Pathogenic';

select count(distinct sample.id)
from (select samples from var_nested as v join clinvar a 
on v.variant_id = a.variant_id 
where a.genename='BRCA1' AND a.clinvar_clnsig='Pathogenic') as f, 
unnest(f.samples) as s(sample);

-- Q5b, always slow
select count(sample.id)
from (select samples from var_nested as v join clinvar a 
on v.variant_id = a.variant_id 
where a.clinvar_clnsig='Pathogenic') as f, 
unnest(f.samples) as s(sample);

select count(1) from var_part_by_chrom as v join clinvar as a 
on v.variant_id = a.variant_id where a.clinvar_clnsig='Pathogenic';




