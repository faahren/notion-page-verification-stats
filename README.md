# Notion Page Verification Stats
A small project including code to retrieve notion pages with their verification information and then transform it in SQL


## How to install
* Install dependencies
* Create your env file:  `cp .env_template .env`
* Add your API key and BQ information to your newly created env file. **Don't use an existing table as the script will overwrite it.**
* Start with `yarn start` (or `npm start`)...

## What will hapen
The script will retrieve data from Notion and insert pages in the bigquery table that you defined in your env file.

## Run the query to transform the data
- Take the file in scheduled_query/pages.bqsql
- Replace the table id in line 11 (The only thing you need to change is this table id in the first CTE)
- Run it in BigQuery to make sure you have your results
- Schedule it

