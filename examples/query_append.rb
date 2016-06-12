task :task1 do
  requires :task2
  run do
    log input.table_name
  end
end

task :task2, type: :bigquery_query do
  param_set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  param_set :dataset_id, "test"
  param_set :table_id, "dest_append"
  param_set :mode, "append"
end
