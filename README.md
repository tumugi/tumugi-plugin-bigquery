[![Build Status](https://travis-ci.org/tumugi/tumugi-plugin-bigquery.svg?branch=master)](https://travis-ci.org/tumugi/tumugi-plugin-bigquery) [![Code Climate](https://codeclimate.com/github/tumugi/tumugi-plugin-bigquery/badges/gpa.svg)](https://codeclimate.com/github/tumugi/tumugi-plugin-bigquery) [![Coverage Status](https://coveralls.io/repos/github/tumugi/tumugi-plugin-bigquery/badge.svg?branch=master)](https://coveralls.io/github/tumugi/tumugi-plugin-bigquery)

# tumugi-plugin-bigquery

tumugi-plugin-bigquery is a plugin for integrate [Google BigQuery](https://cloud.google.com/bigquery/) and [Tumugi](https://github.com/tumugi/tumugi).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'tumugi-plugin-bigquery'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install tumugi-plugin-bigquery


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

Task Plugin ID for DSL is `bigquery_query`.

#### Usage

```rb
task :task1, type: :bigquery_query do
  param_set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  param_set :dataset_id, 'test'
  param_set :table_id, "dest_#{Time.now.to_i}"
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
