[![Build Status](https://travis-ci.org/tumugi/tumugi-plugin-bigquery.svg?branch=master)](https://travis-ci.org/tumugi/tumugi-plugin-bigquery) [![Code Climate](https://codeclimate.com/github/tumugi/tumugi-plugin-bigquery/badges/gpa.svg)](https://codeclimate.com/github/tumugi/tumugi-plugin-bigquery) [![Coverage Status](https://coveralls.io/repos/github/tumugi/tumugi-plugin-bigquery/badge.svg?branch=master)](https://coveralls.io/github/tumugi/tumugi-plugin-bigquery)  [![Gem Version](https://badge.fury.io/rb/tumugi-plugin-bigquery.svg)](https://badge.fury.io/rb/tumugi-plugin-bigquery)

# tumugi-plugin-bigquery

tumugi-plugin-bigquery is a plugin for integrate [Google BigQuery](https://cloud.google.com/bigquery/) and [Tumugi](https://github.com/tumugi/tumugi).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'tumugi-plugin-bigquery'
```

And then execute:

```sh
$ bundle
```

Or install it yourself as:

```sb
$ gem install tumugi-plugin-bigquery
```

## Target

### Tumugi::Plugin::BigqueryDatasetTarget

`Tumugi::Plugin::BigqueryDatasetTarget` is target for BigQuery dataset.

#### Tumugi::Plugin::BigqueryTableTarget

`Tumugi::Plugin::BigqueryDatasetTarget` is target for BigQuery table.

## Task

### Tumugi::Plugin::BigqueryDatasetTask

`Tumugi::Plugin::BigqueryDatasetTask` is task to create a dataset.

#### Usage

```rb
task :task1, type: :bigquery_dataset do
  param_set :dataset_id, 'test'
end
```

### Tumugi::Plugin::BigqueryQueryTask

`Tumugi::Plugin::BigqueryQueryTask` is task to run `query` and save the result into the table which specified by parameter.

#### Usage

```rb
task :task1, type: :bigquery_query do
  param_set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  param_set :dataset_id, 'test'
  param_set :table_id, "dest_table#{Time.now.to_i}"
end
```

### Tumugi::Plugin::BigqueryCopyTask

`Tumugi::Plugin::BigqueryCopyTask` is task to copy table which specified by parameter.

#### Usage

Copy `test.src_table` to `test.dest_table`.

```rb
task :task1, type: :bigquery_copy do
  param_set :src_dataset_id, 'test'
  param_set :src_table_id, 'src_table'
  param_set :dest_dataset_id, 'test'
  param_set :dest_table_id, 'dest_table'
end
```

### Tumugi::Plugin::BigqueryLoadTask

`Tumugi::Plugin::BigqueryLoadTask` is task to load structured data from GCS into BigQuery.

#### Usage

Load `gs://test_bucket/load_data.csv` into `dest_project:dest_dataset.dest_table`

```rb
task :task1, type: :bigquery_load do
  param_set :bucket, 'test_bucket'
  param_set :key, 'load_data.csv'
  param_set :project_id, 'dest_project'
  param_set :datset_id, 'dest_dataset'
  param_set :table_id, 'dest_table'
end
```

### Config Section

tumugi-plugin-bigquery provide config section named "bigquery" which can specified BigQuery autenticaion info.

#### Authenticate by client_email and private_key

```rb
Tumugi.configure do |config|
  config.section("bigquery") do |section|
    section.project_id = "xxx"
    section.client_email = "yyy@yyy.iam.gserviceaccount.com"
    section.private_key = "zzz"
  end
end
```

#### Authenticate by JSON key file

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
