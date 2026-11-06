# What ./setup.sh writes back into the example's .env, for ./pipeline.sh: the
# long-running statements and their views name the functions, and deploy
# requires the artifact ids to be set.
output "env" {
  value = local.produced
}

# Something the rest of an example's configuration can wait for: the topics
# exist once the tables do.
output "tables" {
  value = { for k, s in confluent_flink_statement.table : k => s.statement_name }
}
