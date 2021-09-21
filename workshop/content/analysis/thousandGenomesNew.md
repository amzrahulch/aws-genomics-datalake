+++
title = "Querying 1000 Genomes data"
chapter = true
weight = 450
pre = "<b>3.1 </b>"
+++

We will use the parquet/ORC transformed variant data from the 3502 DRAGEN-reanalyzed 1000 Genomes dataset available in 3 different schemas at s3://aws-roda-hcls-datalake/thousandgenomes_dragen/:

1. var_partby_samples - Dataset partitioned by sample ID

2. var_partby_chrom - Dataset partitioned by chromosome and bucketed by samples

3. var_nested - Nested schema consisting of variant sites with sample IDs and genotypes that contain the variant

![](/images/dataModel.png)

We use the transformed annotations from ClinVar that are available at https://registry.opendata.aws/clinvar/ to demonstrate how to make queries that use the raw variant data with annotations.

You can use the CloudFormation templates for 1000 Genomes DRAGEN, ClinVar available at https://tiny.amazon.com/la8hhrg7/consawsamazclouhome and https://tiny.amazon.com/ugl52tx8/consawsamazclouhome to add these tables to your Glue Data Catalog. 

Once the template deploys, go the AWS Glue Crawler Console check the box next to the thousandgenomes_dragen_awsroda_crawler and click 'Run crawler'. Once it finishes (1-3 minutes) you can query all tables in these datasets.


For the end user who is a researcher performing analyses at a cohort level, the typical types of queries are to identify samples that meet a specific criteria. For example, samples that have variants in a specific gene or region, samples that have a pathogenic or likely pathogenic variant in a specific gene, samples that have rare (minor allele frequency < 0.01) variants in a specific region. The typical mode of operation would be to interactively query using progressively narrower filters to identify a cohort of an appropriate size, and then fetch all the details about the variants and samples themselves. Here is an example set of queries where a researcher is looking for samples that have VUS (variants of uncertain significance) variants in the BRCA1 gene, which is present on chromosome 17 between the loci, 43,044,295 and 43,125,364. We will first query to see how many samples have variants in the BRCA1 gene:

```SQL
select count(DISTINCT(sample.id)) from (
    select samples from thousandgenomes_dragen_dl_awsroda.var_nested 
    where chrom='chr17' 
    and pos between 43044295 and 43125364
  ) as f, 
  unnest(f.samples) as s(sample); 

```
This query returns 3202, which means all samples meet the criteria. 
We need to define a narrower filter to identify samples of interest. This will involve using the clinical significance annotations from ClinVar variant summary table. First, in order to make the queries simpler, we can define a “view” that creates a “variant_id”, consisting of the chromosome, position, ref and alt fields (can be used to join with the variant table), and includes only the fields of interest to us. 

```SQL
-- View to cleanup and build variant_id that will be used to join with variant data.
create or replace view "clinvar_summary_variants_dl-awsroda".clinvar as
select concat('chr', chromosome, ':', referenceallelevcf, ':', alternateallelevcf, ':', cast(positionvcf as varchar)) as variant_id
       , chromosome as chrom
       , positionvcf as pos
       , referenceallelevcf as ref
       , alternateallelevcf as alt
       , genesymbol as genename 
       , clinicalsignificance as clinvar_clnsig
       , split(phenotypeids, ',') as phenotypeids
       , numbersubmitters as num_submitters
from "clinvar_summary_variants_dl-awsroda".variant_summary where assembly = 'GRCh38'
and referenceallelevcf <> 'na' and alternateallelevcf <> 'na'
;

```
We can then use this “clinvar” view in subsequent queries, such as to further filter the original query by searching for only those samples that have variants of Uncertain Significance. 

```SQL
select count(DISTINCT(sample.id)) from (
  select samples from "thousandgenomes_dragen_dl_awsroda".var_nested as v 
  join "clinvar_summary_variants_dl-awsroda".clinvar a
  on v.variant_id = a.variant_id
  where a.clinvar_clnsig = 'Uncertain significance'
    and v.chrom='chr17' 
    and v.pos between 43044295 and 43125364
  ) as f,
unnest(f.samples) as s(sample);

```

This query returns a result of 1550 samples, which has reduced our sample set by approximately 50%. If we want further confidence that these are indeed Variants of Unknown Significance, we can further filter down to only those samples that have variants with more than one submitter.


```SQL
select count(DISTINCT(sample.id)) from (
  select samples from "thousandgenomes_dragen_dl_awsroda".var_nested as v 
  join "clinvar_summary_variants_dl-awsroda".clinvar a
  on v.variant_id = a.variant_id
  where a.clinvar_clnsig = 'Uncertain significance' 
    and v.chrom='chr17' 
    and v.pos between 43044295 and 43125364
    and a.num_submitters > 1
  ) as f,
unnest(f.samples) as s(sample);

```

This query returns a result set of 41 samples, which can then be investigated further. 

For more example queries and scenarios, we have created a [sample notebook](https://github.com/aws-samples/aws-genomics-datalake/blob/main/1000Genomes.ipynb) that you can try.

