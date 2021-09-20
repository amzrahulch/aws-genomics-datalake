+++
title = "Querying 1000 Genomes data"
chapter = true
weight = 450
pre = "<b>3.1 </b>"
+++

We will use the parquet/ORC transformed variant data from the 3502 DRAGEN-reanalyzed 1000 Genomes dataset available in 3 different schemas at s3://aws-roda-hcls-datalake/thousandgenomes_dragen/:

var_partby_samples - Dataset partitioned by sample ID

var_partby_chrom - Dataset partitioned by chromosome and bucketed by samples

var_nested - Nested schema consisting of variant sites with sample IDs and genotypes that contain the variant

We use the transformed annotations from ClinVar that are available at https://registry.opendata.aws/clinvar/ and the transformed population allele frequency data from gnomAD at to demonstrate how to make queries that use the raw variant data with annotations.

Please use the CloudFormation templates for 1000 Genomes DRAGEN, ClinVar and gnomAD available at https://github.com/aws-samples/data-lake-as-code/tree/roda to add these tables to your Glue Data Catalog.


We use Clinvar as Annotation dataset. The dataset is publically available in RODA, you can create an external table in Athena (or use the cloudformation template from above) and start accessing it immediately.

```SQL
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION's3://aws-roda-hcls-datalake/clinvar_summary_variants/variant_summary/'
;
``` 


For the end user who is a researcher performing analyses at a cohort level, the typical types of queries are to identify samples that meet a specific criteria. For example, samples that have variants in a specific gene or region, samples that have a pathogenic or likely pathogenic variant in a specific gene, samples that have rare (minor allele frequency < 0.01) variants in a specific region. The typical mode of operation would be to interactively query using progressively narrower filters to identify a cohort of an appropriate size, and then fetch all the details about the variants and samples themselves. Here is an example set of queries where a researcher is looking for samples that have VUS (variants of uncertain significance) variants in the BRCA1 gene, which is present on chromosome 17 between the loci, 43,044,295 and 43,125,364. We will first query to see how many samples have variants in the BRCA1 gene:

```SQL
select count(DISTINCT(sample.id)) from (
    select samples from var_nested 
    where chrom='chr17' 
    and pos between 43044295 and 43125364
  ) as f, 
  unnest(f.samples) as s(sample); 

```
This query returns 3202, which means all samples meet the criteria. 
We need to define a narrower filter to identify samples of interest. This will involve using the clinical significance annotations from ClinVar variant summary table. First, in order to make the queries simpler, we can define a “view” that creates a “variant_id”, consisting of the chromosome, position, ref and alt fields (can be used to join with the variant table), and includes only the fields of interest to us. 

```SQL
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
and referenceallelevcf <> 'na' and alternateallelevcf <> 'na'
;

```
We can then use this “clinvar” view in subsequent queries, such as to further filter the original query by searching for only those samples that have variants of Uncertain Significance. 

```SQL
select count(DISTINCT(sample.id (http://sample.id/))) from (
  select samples from var_nested as v 
  join clinvar a
  on v.variant_id = a.variant_id
  where *a**.clinvar_clnsig = 'Uncertain significance'* 
    and v.chrom='chr17' 
    and v.pos between 43044295 and 43125364
  ) as f,
unnest(f.samples) as s(sample);

```

This query returns a result of 1550 samples, which has reduced our sample set by approximately 50%. If we want further confidence that these are indeed Variants of Unknown Significance, we can further filter down to only those samples that have variants with more than one submitter.


```SQL
select count(DISTINCT(sample.id)) from (
  select samples from var_nested as v 
  join clinvar a
  on v.variant_id = a.variant_id
  where a.clinvar_clnsig = 'Uncertain significance' 
    and v.chrom='chr17' 
    and v.pos between 43044295 and 43125364
    *and a.num_submitters > 1*
  ) as f,
unnest(f.samples) as s(sample);

```

This query returns a result set of 41 samples, which can then be investigated further. For more example queries and scenarios, we have created a [sample notebook](https://github.com/amzrahulch/genomicsDataLakePoC/blob/main/1000Genomes.ipynb) here you can try.