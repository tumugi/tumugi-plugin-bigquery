task :task1, type: :bigquery_load do
  requires :task2
  bucket 'tumugi-plugin-bigquery'
  key 'test.csv'
  dataset_id { input.dataset_id }
  table_id 'load_test'
  skip_leading_rows 1
  schema [
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
  dataset_id "test"
end
