+++
title = "Manage new samples"
chapter = true
weight = 450
pre = "<b>3.3 </b>"
+++

For clinvar data, the dataset is refreshed whenever the source data is updated.
For variants data, sequenced samples are usually not updated, but new samples will be added over time.
How to integrate the new samples to our dataset so our analysis can get up-to-date result?

Once new vcf files are uploaded to S3, we can run the hail job to convert them to parquet. then add them to various optimized data copies.

```SQL
insert into var_part_by_chrom 
	select * from var_part_by_sample 
	where partition_0 in (<list of new samples>);
```

{{% notice note %}}
Note that for the nested dataset, we need to re-built it to have new samples nested properly with variant sites.
{{% /notice %}}

First drop the table and delete underlying S3 data, then re-run the CTAS query. Below query demonstrate if you only want to update data of a specific chromosome. **Note you need to change the s3 bucket to one of your own**.

```SQL
create  table var_nested_chr21
with (
  external_location = 's3://genomics_datalake/var_nested/chrom=chr21/',
  format = 'ORC'
  )
as
select variant_id, pos, ref, alt,  
array_agg(CAST(ROW(sample_id, "gt.alleles") AS ROW(id VARCHAR, gts ARRAY(INTEGER)))) as samples
from var_part_by_chrom 
where chrom = 'chr21'
group by 1,2,3,4

```
