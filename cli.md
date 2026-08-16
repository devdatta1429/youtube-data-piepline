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

## 3. Re-run `bronze_to_silver` cleanly once stragglers are identified/removed

```powershell
aws glue start-job-run --job-name "yt-data-pipeline-bronze-to-silver-devdatta" --region ap-south-1
```
Poll status:
```powershell
aws glue get-job-runs --job-name "yt-data-pipeline-bronze-to-silver-devdatta" --region ap-south-1 --max-results 1 --query "JobRuns[0].[JobRunState,ErrorMessage]"
```

## 4. Re-verify `clean_statistics` schema (region should no longer duplicate)

```powershell
aws glue get-table --database-name yt-pipeline-silver-devdatta --name clean_statistics --region ap-south-1 --query "Table.[StorageDescriptor.Columns[].Name,PartitionKeys[].Name]"
```

## 5. Re-run the Silver→Gold job that originally failed

```powershell
aws glue start-job-run --job-name "silver_to_gold_analytics" --region ap-south-1
```
```powershell
aws glue get-job-runs --job-name "silver_to_gold_analytics" --region ap-south-1 --max-results 1 --query "JobRuns[0].[JobRunState,ErrorMessage]"
```

## 6. Final full DQ check

```powershell
aws lambda invoke --function-name yt-data-pipeline-data-quality-devdatta --region ap-south-1 --payload '{}' dq_response.json
type dq_response.json
```

## 7. Reminder — still outstanding from earlier
Your live YouTube API key was exposed in plaintext in this chat when you pasted `list-functions` output. Rotate it in Google Cloud Console and update the `YOUTUBE_API_KEY` env var on `yt-data-pipeline-youtube-ingestion-dev` once you have a moment — not urgent to today's DQ fix, but worth doing.

Start with step 1 and step 2, paste results, and we'll keep going from there.
