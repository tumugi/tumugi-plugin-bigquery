task :task1 do
  requires :task2
  run do
    log input.dataset_id
  end
end

task :task2, type: :bigquery_dataset do
  param_set :dataset_id, 'test'
end
