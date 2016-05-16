# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'tumugi/plugin/bigquery/version'

Gem::Specification.new do |spec|
  spec.name          = "tumugi-plugin-bigquery"
  spec.version       = Tumugi::Plugin::Bigquery::VERSION
  spec.authors       = ["Kazuyuki Honda"]
  spec.email         = ["hakobera@gmail.com"]

  spec.summary       = "Tumugi plugin for Google BigQuery"
  spec.homepage      = "https://github.com/tumugi/tumugi-plugin-bigquery"
  spec.license       = "Apache License Version 2.0"

  spec.required_ruby_version = '>= 2.1'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "tumugi", "~> 0.4.5"
  spec.add_runtime_dependency "kura", "0.2.16"

  spec.add_development_dependency 'bundler', '~> 1.11'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'test-unit', '~> 3.1'
  spec.add_development_dependency 'test-unit-rr'
  spec.add_development_dependency 'coveralls'
  spec.add_development_dependency 'github_changelog_generator'
end
