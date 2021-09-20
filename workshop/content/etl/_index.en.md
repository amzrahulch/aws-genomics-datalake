+++
title = "VCF conversion"
chapter = true
weight = 11
pre = "<b>2. </b>"
+++

## Launch pre processing step using cloudformation

We will use cloudformation template to deploy EMR cluster which has Hail compiled and installed already. The cloudformation template takes inputs as shown below. 

{{< img "emr_cfn_input.png" "Options" >}}

{{% notice note %}}
Replace the value for output S3 path and EMR Logs to a path in your own account. While chosing a VPC pick a public subnet or a private subnet with a NAT gateway for outbound internet traffic. 
{{% /notice %}}

The optional values here can be left unchanged. 

{{< img "emr_cfn_input_2.png" "Options" >}}


Once you hit create stack it takes a few minutes for the EMR cluster to appear as Starting. 

{{< img "emr_pre_process_starting.png" "Options" >}}

Within a few minutes (5-10mins) the VCFs in the input path provided will be processed and written as parquet to the output path. The EMR cluster will be auto terminated after this processing step completes.  

{{< img "emr_pre_process_terminated.png" "Options" >}}


## How hail is used to transform VCF to Parquet


The script that was used to transform is located here: `s3://redshift-demos/genomics/vcfToParquetTransform.py` 
This script takes two input parameters: vcf_s3_location output_location. You can provide your own custom script in the cloudformation template above using the HailScriptPath parameter. 

For a single VCF file, the job takes 1-2 minutes. Larger VCF could take longer time. 