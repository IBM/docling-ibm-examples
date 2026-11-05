# This example's setup — see terraform/modules/example for what that is.
# Run through ../setup.sh (plan | apply | destroy), which builds the artifacts,
# hands Terraform the example's configuration and writes the outputs back into
# the example's .env. The state is here, beside this file, and gitignored.

terraform {
  required_version = ">= 1.5"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.93"
    }
  }
}

variable "env" {
  description = "The example's layered configuration. Set by ../setup.sh, never by hand."
  type        = map(string)
}

variable "artifact_files" {
  description = "What each build.sh produced. Set by ../setup.sh."
  type        = map(string)
}

provider "confluent" {
  cloud_api_key    = var.env.CONFLUENT_CLOUD_API_KEY
  cloud_api_secret = var.env.CONFLUENT_CLOUD_API_SECRET

  # Every statement goes to this pool, as this principal, with this key.
  organization_id       = var.env.CC_ORG_ID
  environment_id        = var.env.CC_ENVIRONMENT_ID
  flink_compute_pool_id = var.env.CC_COMPUTE_POOL_ID
  flink_rest_endpoint   = "https://flink.${var.env.CC_REGION}.${var.env.CC_CLOUD}.confluent.cloud"
  flink_api_key         = var.env.FLINK_API_KEY
  flink_api_secret      = var.env.FLINK_API_SECRET
  flink_principal_id    = var.env.CC_PRINCIPAL_ID
}

module "example" {
  source = "../../../terraform/modules/example"

  env            = var.env
  example_dir    = abspath("${path.module}/..")
  artifact_files = var.artifact_files
}

output "env" {
  value = module.example.env
}

# --- the connector ---------------------------------------------------------
#
# Confluent's managed SQS Source: one record per SQS message — the S3 event
# JSON — into the objects topic. Billed by the hour while it runs; destroy
# removes it, first, because it writes into a topic everything else reads.
# After the tables, because the objects topic must exist before it starts.
# The AWS side is scripts/aws.sh's, and it wrote the names below into the
# example's .env.

resource "confluent_connector" "sqs_source" {
  environment {
    id = var.env.CC_ENVIRONMENT_ID
  }
  kafka_cluster {
    id = var.env.CC_CLUSTER_ID
  }

  config_sensitive = {
    "kafka.api.key"     = var.env.KAFKA_API_KEY
    "kafka.api.secret"  = var.env.KAFKA_API_SECRET
    "aws.access.key.id" = var.env.AWS_READER_KEY_ID
    "aws.secret.key.id" = var.env.AWS_READER_SECRET
  }
  config_nonsensitive = {
    "name"               = var.env.SQS_CONNECTOR
    "connector.class"    = "SqsSource"
    "kafka.auth.mode"    = "KAFKA_API_KEY"
    "kafka.topic"        = var.env.OBJECTS_TOPIC
    "sqs.url"            = var.env.SQS_QUEUE_URL
    "output.data.format" = "JSON"
    "tasks.max"          = "1"
  }

  depends_on = [module.example]
}
