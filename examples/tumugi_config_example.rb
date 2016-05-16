Tumugi.config do |c|
  c.section('bigquery') do |s|
    s.project_id = ENV["PROJECT_ID"]
    s.client_email = ENV["CLIENT_EMAIL"]
    s.private_key = ENV["PRIVATE_KEY"].gsub(/\\n/, "\n")
  end
end
