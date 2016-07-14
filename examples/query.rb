task :task1 do
  requires :task2
  run do
    log input.table_name
  end
end

task :task2, type: :bigquery_query do
  query      "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  dataset_id "test"
  table_id   "dest_#{Time.now.to_i}"
end
