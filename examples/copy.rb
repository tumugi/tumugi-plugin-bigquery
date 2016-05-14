task :task1, type: :bigquery_copy do
  param_set :src_project, ->{ input.project_id }
  param_set :src_dataset, ->{ input.dataset_id }
  param_set :src_table,   ->{ input.table_id }
  param_set :dest_dataset, "test"
  param_set :dest_table,  ->{ "dest_table_#{Time.now.to_i}" }

  requires :task2
end

task :task2, type: :bigquery_query do
  param_set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
  param_set :dataset, "test" #->{ input.dataset_id }
  param_set :table, "dest_#{Time.now.to_i}"

  requires :task3
end

task :task3, type: :bigquery_dataset do
  param_set :dataset, "test"
end
