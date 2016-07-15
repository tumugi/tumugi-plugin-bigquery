task :task1, type: :bigquery_export do
  dataset_id  { input.dataset_id }
  table_id    { input.table_id }

  requires :task2
  output target(:local_file, "tmp/export.csv")
end

task :task2, type: :bigquery_query do
  query      "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  dataset_id "test"
  table_id   "dest_#{Time.now.to_i}"
end
