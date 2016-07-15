task :task1, type: :bigquery_copy do
  src_project_id  { input.project_id }
  src_dataset_id  { input.dataset_id }
  src_table_id    { input.table_id }
  dest_dataset_id "test"
  dest_table_id   "dest_table_1"
  force_copy      true

  requires :task2
end

task :task2, type: :bigquery_query do
  query      "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  dataset_id { input.dataset_id }
  table_id   "dest_#{Time.now.to_i}"

  requires :task3
end

task :task3, type: :bigquery_dataset do
  dataset_id "test"
end
