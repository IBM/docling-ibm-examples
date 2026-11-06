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

# --- the Real-Time Context Engine ------------------------------------------
#
# Lightning queries over the topics in RTCE_TOPIC_VARS. Opt-in, because it is
# billed while on, by storage and by compute: `../setup.sh context-engine on`
# sets CONTEXT_ENGINE=on in the example's .env and applies; `off` the reverse.
# Best after the first `./pipeline.sh deploy`, so the tables have rows to serve.

locals {
  rtce_topics = lower(lookup(var.env, "CONTEXT_ENGINE", "off")) == "on" ? toset([
    for v in split(" ", var.env.RTCE_TOPIC_VARS) : var.env[v] if v != ""
  ]) : toset([])
}

resource "confluent_rtce_topic" "served" {
  for_each = local.rtce_topics

  cloud       = upper(var.env.CC_CLOUD)
  region      = var.env.CC_REGION
  topic_name  = each.key
  description = var.env.APP_NAME
  environment {
    id = var.env.CC_ENVIRONMENT_ID
  }
  kafka_cluster {
    id = var.env.CC_CLUSTER_ID
  }

  depends_on = [module.example]
}
