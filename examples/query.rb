task :task1 do
  requires :task2
  run do
    log input.table_name
  end
end

task :task2, type: :bigquery_query do
  param_set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  param_set :dataset, 'test'
  param_set :table, "dest_#{Time.now.to_i}"
end
