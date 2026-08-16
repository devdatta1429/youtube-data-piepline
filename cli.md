Here's the full consolidated command set to close out both remaining issues, in order.

## 1. Find and stop whatever is triggering the duplicate reference-data job

```powershell
aws glue get-triggers --region ap-south-1 --query "Triggers[?Actions[?JobName=='yt-data-pipeline-reference-to-silver-devdatta']]"
```

**If a trigger is found**, disable it (safer than deleting — keeps history):
```powershell
aws glue stop-trigger --name "<trigger-name>" --region ap-south-1
```

**Once you're confident the Lambda fully replaces it**, remove the job so it can never run again:
```powershell
aws glue delete-job --job-name "yt-data-pipeline-reference-to-silver-devdatta" --region ap-south-1
```

## 2. Check `clean_statistics` for the same stray-file pattern

```powershell
aws s3 ls s3://yt-data-pipeline-silver-ap-south-1-devdatta/youtube/statistics/region=us/ --recursive
aws s3 ls s3://yt-data-pipeline-silver-ap-south-1-devdatta/youtube/statistics/region=gb/ --recursive
aws s3 ls s3://yt-data-pipeline-silver-ap-south-1-devdatta/youtube/statistics/region=ca/ --recursive
aws s3 ls s3://yt-data-pipeline-silver-ap-south-1-devdatta/youtube/statistics/region=in/ --recursive
```

Paste these back before deleting anything — filenames/dates will tell us which files are stale.

