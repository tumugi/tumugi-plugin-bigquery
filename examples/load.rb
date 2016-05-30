task :task1, type: :bigquery_load do
  requires :task2
  param_set :bucket, 'tumugi-plugin-bigquery'
  param_set :key, 'test.csv'
  param_set :dataset_id, -> { input.dataset_id }
  param_set :table_id, 'load_test'
  param_set :skip_leading_rows, 1
  param_set :schema, [
    {
      name: 'row_number',
      type: 'INTEGER',
      mode: 'NULLABLE'
    },
    {
      name: 'value',
      type: 'INTEGER',
      mode: 'NULLABLE'
    },
  ]
end

task :task2, type: :bigquery_dataset do
  param_set :dataset_id, 'test'
end
