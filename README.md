[![Build Status](https://travis-ci.org/tumugi/tumugi-plugin-bigquery.svg?branch=master)](https://travis-ci.org/tumugi/tumugi-plugin-bigquery) [![Code Climate](https://codeclimate.com/github/tumugi/tumugi-plugin-bigquery/badges/gpa.svg)](https://codeclimate.com/github/tumugi/tumugi-plugin-bigquery) [![Coverage Status](https://coveralls.io/repos/github/tumugi/tumugi-plugin-bigquery/badge.svg?branch=master)](https://coveralls.io/github/tumugi/tumugi-plugin-bigquery)  [![Gem Version](https://badge.fury.io/rb/tumugi-plugin-bigquery.svg)](https://badge.fury.io/rb/tumugi-plugin-bigquery)

# Google BigQuery plugin for [tumugi](https://github.com/tumugi/tumugi)

tumugi-plugin-bigquery is a plugin for integrate [Google BigQuery](https://cloud.google.com/bigquery/) and [tumugi](https://github.com/tumugi/tumugi).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'tumugi-plugin-bigquery'
```

And then execute `bundle install`.

## Target

### Tumugi::Plugin::BigqueryDatasetTarget

`Tumugi::Plugin::BigqueryDatasetTarget` is target for BigQuery dataset.

#### Parameters

| Name       | type   | required? | default | description                                                      |
|------------|--------|-----------|---------|------------------------------------------------------------------|
| dataset_id | string | required  |         | Dataset ID                                                       |
| project_id | string | optional  |         | [Project](https://cloud.google.com/compute/docs/projects) ID     |

#### Examples

```rb
task :task1 do
  output target(:bigquery_dataset, dataset_id: "your_dataset_id")
end
```

```rb
task :task1 do
  output target(:bigquery_dataset, project_id: "project_id", dataset_id: "dataset_id")
end
```

#### Tumugi::Plugin::BigqueryTableTarget

`Tumugi::Plugin::BigqueryDatasetTarget` is target for BigQuery table.

#### Parameters

| name       | type   | required? | default | description                                                      |
|------------|--------|-----------|---------|------------------------------------------------------------------|
| table_id   | string | required  |         | Table ID                                                         |
| dataset_id | string | required  |         | Dataset ID                                                       |
| project_id | string | optional  |         | [Project](https://cloud.google.com/compute/docs/projects) ID     |

#### Examples

```rb
task :task1 do
  output target(:bigquery_table, table_id: "table_id", dataset_id: "your_dataset_id")
end
```

## Task

### Tumugi::Plugin::BigqueryDatasetTask

`Tumugi::Plugin::BigqueryDatasetTask` is task to create a dataset.

#### Parameters

| name       | type   | required? | default | description                                                      |
|------------|--------|-----------|---------|------------------------------------------------------------------|
| dataset_id | string | required  |         | Dataset ID                                                       |
| project_id | string | optional  |         | [Project](https://cloud.google.com/compute/docs/projects) ID     |

#### Examples

```rb
task :task1, type: :bigquery_dataset do
  dataset_id 'test'
end
```

### Tumugi::Plugin::BigqueryQueryTask

`Tumugi::Plugin::BigqueryQueryTask` is task to run `query` and save the result into the table which specified by parameter.

#### Parameters

| name            | type    | required? | default    | description                                                                                                                                   |
|-----------------|---------|-----------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| query           | string  | required  |            | query to execute                                                                                                                              |
| table_id        | string  | required  |            | destination table ID                                                                                                                          |
| dataset_id      | string  | required  |            | destination dataset ID                                                                                                                        |
| project_id      | string  | optional  |            | destination project ID                                                                                  |
| mode            | string  | optional  | "truncate" | specifies the action that occurs if the destination table already exists. [see](#mode)                                             |
| flatten_results | boolean | optional  | true       | when you query nested data, BigQuery automatically flattens the table data or not. [see](https://cloud.google.com/bigquery/docs/data#flatten) |
| use_legacy_sql  | bool    | optional  | true       | use legacy SQL syntanx for BigQuery or not                                                                                                    |
| wait            | integer | optional  | 60         | wait time (seconds) for query execution                                                                                                       |

#### Examples

##### truncate mode (default)

```rb
task :task1, type: :bigquery_query do
  query      "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  table_id   "dest_table#{Time.now.to_i}"
  dataset_id "test"
end
```

##### append mode

If you set `mode` to `append`, query result append to existing table.

```rb
task :task1, type: :bigquery_query do
  query      "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  table_id   "dest_table#{Time.now.to_i}"
  dataset_id "test"
  mode       "append"
end
```

### Tumugi::Plugin::BigqueryCopyTask

`Tumugi::Plugin::BigqueryCopyTask` is task to copy table which specified by parameter.

#### Parameters

| name            | type   | required? | default | description                                             |
|-----------------|--------|-----------|---------|---------------------------------------------------------|
| src_table_id    | string | required  |         | source table ID                                         |
| src_dataset_id  | string | required  |         | source dataset ID                                       |
| src_project_id  | string | optional  |         | source project ID                                       |
| dest_table_id   | string | required  |         | destination table ID                                    |
| dest_dataset_id | string | required  |         | destination dataset ID                                  |
| dest_project_id | string | optional  |         | destination project ID                                  |
| force_copy      | bool   | optional  | false   | force copy when destination table already exists or not |
| wait            | integer| optional  | 60      | wait time (seconds) for query execution                 |

#### Examples

Copy `test.src_table` to `test.dest_table`.

##### Normal usecase

```rb
task :task1, type: :bigquery_copy do
  src_table_id    "src_table"
  src_dataset_id  "test"
  dest_table_id   "dest_table"
  dest_dataset_id "test"
end
```

##### force_copy

If `force_copy` is `true`, copy operation always execute even if destination table exists.
This means data of destination table data is deleted, so be carefull to enable this parameter.

```rb
task :task1, type: :bigquery_copy do
  src_table_id    "src_table"
  src_dataset_id  "test"
  dest_table_id   "dest_table"
  dest_dataset_id "test"
  force_copy      true
end
```

### Tumugi::Plugin::BigqueryLoadTask

`Tumugi::Plugin::BigqueryLoadTask` is task to load structured data from GCS into BigQuery.

#### Parameters

| name                  | type            | required?                          | default             | description                                                                                                                                  |
|-----------------------|-----------------|------------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| bucket                | string          | required                           |                     | source GCS bucket name                                                                                                                       |
| key                   | string          | required                           |                     | source path of file like "/path/to/file.csv"                                                                                                 |
| table_id              | string          | required                           |                     | destination table ID                                                                                                                         |
| dataset_id            | string          | required                           |                     | destination dataset ID                                                                                                                       |
| project_id            | string          | optional                           |                     | destination project ID                                                                                                                       |
| schema                | array of object | required when mode is not "append" |                     | see [schema format](#schema)                                                                                                      |
| mode                  | string          | optional                           | "append"            | specifies the action that occurs if the destination table already exists. [see](#mode)                                            |
| source_format         | string          | optional                           | "CSV"               | source file format. [see](#format)                                                                                                |
| ignore_unknown_values | bool            | optional                           | false               | indicates if BigQuery should allow extra values that are not represented in the table schema                                                 |
| max_bad_records       | integer         | optional                           | 0                   | maximum number of bad records that BigQuery can ignore when running the job                                                                  |
| field_delimiter       | string          | optional                           | ","                 | separator for fields in a CSV file. used only when source_format is "CSV"                                                                    |
| allow_jagged_rows     | bool            | optional                           | false               | accept rows that are missing trailing optional columns. The missing values are treated as null. used only when source_format is "CSV"        |
| allow_quoted_newlines | bool            | optional                           | false               | indicates if BigQuery should allow quoted data sections that contain newline characters in a CSV file. used only when source_format is "CSV" |
| quote                 | string          | optional                           | "\"" (double-quote) | value that is used to quote data sections in a CSV file. used only when source_format is "CSV"                                               |
| skip_leading_rows     | integer         | optional                           | 0                   | .number of rows at the top of a CSV file that BigQuery will skip when loading the data. used only when source_format is "CSV"                |
| wait                  | integer         | optional                           | 60                  | wait time (seconds) for query execution                                                                                                      |

#### Example

Load `gs://test_bucket/load_data.csv` into `dest_project:dest_dataset.dest_table`

```rb
task :task1, type: :bigquery_load do
  bucket     "test_bucket"
  key        "load_data.csv"
  table_id   "dest_table"
  datset_id  "dest_dataset"
  project_id "dest_project"
end
```

### Tumugi::Plugin::BigqueryExportTask

`Tumugi::Plugin::BigqueryExportTask` is task to export BigQuery table.

#### Parameters

| name               | type    | required? | default            | description                                                                         |
|--------------------|---------|-----------|--------------------|-------------------------------------------------------------------------------------|
| project_id         | string  | optional  |                    | source project ID                                                                   |
| job_project_id     | string  | optional  | same as project_id | job running project ID                                                              |
| dataset_id         | string  | required  | true               | source dataset ID                                                                   |
| table_id           | string  | required  | true               | source table ID                                                                     |
| compression        | string  | optional  | "NONE"             | [destination file compression]. "NONE": no compression, "GZIP": compression by gzip |
| destination_format | string  | optional  | "CSV"              | [destination file format](#format)                                                  |
| field_delimiter    | string  | optional  | ","                | separator for fields in a CSV file. used only when destination_format is "CSV"      |
| print_header       | bool    | optional  | true               | print header row in a CSV file. used only when destination_format is "CSV"          |
| page_size          | integer | optional  | 10000              | Fetch number of rows in one request                                                 |
| wait               | integer | optional  | 60                 | wait time (seconds) for query execution                                             |

#### Examples

##### Export `src_dataset.src_table` to local file `data.csv`

```rb
task :task1, type: :bigquery_export do
  table_id   "src_table"
  datset_id  "src_dataset"

  output target(:local_file, "data.csv")
end
```

##### Export `src_dataset.src_table` to Google Cloud Storage

You need [tumugi-plugin-google_cloud_storage](https://github.com/tumugi/tumugi-plugin-google_cloud_storage)

```rb
task :task1, type: :bigquery_export do
  table_id   "src_table"
  datset_id  "src_dataset"

  output target(:google_cloud_storage_file, bucket: "bucket", key: "data.csv")
end
```

##### Export `src_dataset.src_table` to Google Drive

You need [tumugi-plugin-google_drive](https://github.com/tumugi/tumugi-plugin-google_drive)

```rb
task :task1, type: :bigquery_export do
  table_id   "src_table"
  datset_id  "src_dataset"

  output target(:google_drive_file, name: "data.csv")
end
```

## Common parameter value

### mode

| value    | description |
|----------|-------------|
| truncate | If the table already exists, BigQuery overwrites the table data. |
| append   | If the table already exists, BigQuery appends the data to the table. |
| empty    | If the table already exists and contains data, a 'duplicate' error is returned in the job result. |

### format

| value                  | description                                |
|------------------------|--------------------------------------------|
| CSV                    | CSV                                        |
| NEWLINE_DELIMITED_JSON | Each line is JSON + new line               |
| AVRO                   | [see](https://avro.apache.org/docs/1.2.0/) |

### schema

Format of `schema` parameter is array of nested object like below:

```js
[
  {
    "name": "column1",
    "type": "string"
  },
  {
    "name": "column2",
    "type": "integer",
    "mode": "repeated"
  },
  {
    "name": "record1",
    "type": "record",
    "fields": [
      {
        "name": "key1",
        "type": "integer",
      },
      {
        "name": "key2",
        "type": "integer"
      }
    ]
  }
]
```

## Config Section

tumugi-plugin-bigquery provide config section named "bigquery" which can specified BigQuery autenticaion info.

### Authenticate by client_email and private_key

```rb
Tumugi.configure do |config|
  config.section("bigquery") do |section|
    section.project_id = "xxx"
    section.client_email = "yyy@yyy.iam.gserviceaccount.com"
    section.private_key = "zzz"
  end
end
```

### Authenticate by JSON key file

```rb
Tumugi.configure do |config|
  config.section("bigquery") do |section|
    section.private_key_file = "/path/to/key.json"
  end
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies.
Then, export [Google Cloud Platform Service Accounts](https://cloud.google.com/iam/docs/service-accounts) key as following,

```sh
export PROJECT_ID="xxx"
export CLIENT_EMAIL="yyy@yyy.iam.gserviceaccount.com"
export PRIVATE_KEY="zzz"
```

Then run `bundle exec rake test` to run the tests.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/tumugi/tumugi-plugin-bigquery.

## License

The gem is available as open source under the terms of the [Apache License
Version 2.0](http://www.apache.org/licenses/).
