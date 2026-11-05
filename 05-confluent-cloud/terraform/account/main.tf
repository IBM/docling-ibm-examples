# Your Confluent Cloud account, once — before any example can run.
#
# Run through ../../account.sh (plan | apply | destroy), not by hand: that is
# what turns .env into `var.env` and writes the outputs back into .env, where
# every script and every example reads them.
#
# Everything here is either *given* or *made*, one thing at a time:
#
#   environment    always looked up, never made: CC_ENVIRONMENT_ID, or else
#                  the account's own "default" environment.
#   Kafka cluster  CC_CLUSTER_ID set: looked up. Unset: a Basic, single-zone
#                  cluster in CC_CLOUD/CC_REGION.
#   compute pool   CC_COMPUTE_POOL_ID set: looked up. Unset: one in the
#                  cluster's region, CC_POOL_MAX_CFU at most.
#   key owner      CC_PRINCIPAL_ID set: that user or service account. Unset: a
#                  service account, "<NS>-docling", with EnvironmentAdmin and
#                  FlinkAdmin on the environment.
#
# A thing that is looked up is a data source, so no plan can ever propose to
# destroy it. A thing that is made is this root's, and `./account.sh destroy`
# removes it — a cluster with every topic in it.
#
# Always made: the API keys — Flink, Kafka, Schema Registry (when the
# environment has one) and, opt-in, a global key. Each secret is in the state
# and goes into .env; nowhere else.
#
# On a vanilla account all of them are unset, and the one thing needed is a
# Cloud API key of someone who may create them: OrganizationAdmin, which the
# account owner is.
#
# What was made is written back into .env like everything else, and account.sh
# hands Terraform those ids blank again — the state, not .env, is what says a
# thing is ours.

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
  description = "The layered .env, as account.sh's load_env sees it. Set by ./account.sh, never by hand."
  type        = map(string)
}

provider "confluent" {
  cloud_api_key    = var.env.CONFLUENT_CLOUD_API_KEY
  cloud_api_secret = var.env.CONFLUENT_CLOUD_API_SECRET
}

locals {
  ns         = var.env.NS
  name       = "${var.env.NS}-docling"
  global_key = lower(lookup(var.env, "GLOBAL_KEY", "no")) == "yes"

  make_cluster     = lookup(var.env, "CC_CLUSTER_ID", "") == ""
  make_pool        = lookup(var.env, "CC_COMPUTE_POOL_ID", "") == ""
  make_principal   = lookup(var.env, "CC_PRINCIPAL_ID", "") == ""
}

data "confluent_organization" "this" {}

# --- the environment -----------------------------------------------------------

# Every account has one called "default"; that is the one used unless .env
# names another. An environment is where everyone's work is visible to
# everyone, so this never makes one of its own.
data "confluent_environment" "this" {
  id           = lookup(var.env, "CC_ENVIRONMENT_ID", "") == "" ? null : var.env.CC_ENVIRONMENT_ID
  display_name = lookup(var.env, "CC_ENVIRONMENT_ID", "") == "" ? "default" : null
}

locals {
  environment = data.confluent_environment.this
}

# --- the Kafka cluster -----------------------------------------------------------

data "confluent_kafka_cluster" "given" {
  count = local.make_cluster ? 0 : 1
  id    = var.env.CC_CLUSTER_ID
  environment {
    id = local.environment.id
  }
}

# Basic: billed by use, no minimum, and all these examples need — Flink reads
# and writes it, nothing here wants dedicated capacity.
resource "confluent_kafka_cluster" "this" {
  count        = local.make_cluster ? 1 : 0
  display_name = local.name
  availability = "SINGLE_ZONE"
  cloud        = upper(var.env.CC_CLOUD)
  region       = var.env.CC_REGION
  basic {}
  environment {
    id = local.environment.id
  }
}

locals {
  cluster = one([for c in concat(confluent_kafka_cluster.this, data.confluent_kafka_cluster.given) : {
    id                 = c.id
    display_name       = c.display_name
    cloud              = c.cloud
    region             = c.region
    api_version        = c.api_version
    kind               = c.kind
    bootstrap_endpoint = c.bootstrap_endpoint
  }])
}

data "confluent_flink_region" "this" {
  cloud  = upper(local.cluster.cloud)
  region = local.cluster.region
}

# --- the compute pool ------------------------------------------------------------

data "confluent_flink_compute_pool" "given" {
  count = local.make_pool ? 0 : 1
  id    = var.env.CC_COMPUTE_POOL_ID
  environment {
    id = local.environment.id
  }

  lifecycle {
    # A pool in another region cannot read the cluster's topics. This is the
    # mistake that otherwise surfaces as a statement that cannot resolve a
    # table, a long way from the cause.
    postcondition {
      condition     = lower(self.cloud) == lower(local.cluster.cloud) && self.region == local.cluster.region
      error_message = "The compute pool ${self.id} is in ${self.cloud}/${self.region}, the cluster in ${local.cluster.cloud}/${local.cluster.region}. Pick a pool in the cluster's region."
    }
  }
}

# Scales to zero with no statement running; CFUs are what Flink bills.
resource "confluent_flink_compute_pool" "this" {
  count        = local.make_pool ? 1 : 0
  display_name = local.name
  cloud        = upper(local.cluster.cloud)
  region       = local.cluster.region
  max_cfu      = tonumber(lookup(var.env, "CC_POOL_MAX_CFU", "10"))
  environment {
    id = local.environment.id
  }
}

locals {
  pool_id = one(concat(confluent_flink_compute_pool.this[*].id, data.confluent_flink_compute_pool.given[*].id))
}

# --- Schema Registry ---------------------------------------------------------------
#
# Plural, so that an environment without Stream Governance is "no key" rather
# than an error — only examples 04 and 05 need one.

data "confluent_schema_registry_clusters" "this" {
  environment {
    id = local.environment.id
  }
}

locals {
  schema_registry     = one(data.confluent_schema_registry_clusters.this.clusters)
  has_schema_registry = local.schema_registry != null
}

# --- who owns the keys ---------------------------------------------------------------
#
# The statements run as the owner of the Flink key, and read and write topics
# as it, so it needs the environment — and Flink in it.

resource "confluent_service_account" "this" {
  count        = local.make_principal ? 1 : 0
  display_name = local.name
  description  = "docling-confluent-cloud: owns the API keys, runs the statements"
}

resource "confluent_role_binding" "environment_admin" {
  count       = local.make_principal ? 1 : 0
  principal   = "User:${confluent_service_account.this[0].id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = local.environment.resource_name
}

resource "confluent_role_binding" "flink_admin" {
  count       = local.make_principal ? 1 : 0
  principal   = "User:${confluent_service_account.this[0].id}"
  role_name   = "FlinkAdmin"
  crn_pattern = local.environment.resource_name
}

locals {
  principal = local.make_principal ? confluent_service_account.this[0].id : var.env.CC_PRINCIPAL_ID
  owner = {
    id          = local.principal
    api_version = "iam/v2"
    # An API key's owner is a user or a service account, and the API wants to
    # be told which.
    kind = local.make_principal || startswith(local.principal, "sa-") ? "ServiceAccount" : "User"
  }
}

# --- the keys ------------------------------------------------------------------------
#
# After the role bindings: a key is usable at once, and what it may do is what
# its owner may do.

# A *Flink region* key. Submits statements, and nothing else does.
resource "confluent_api_key" "flink" {
  display_name = "${local.ns}-flink"
  description  = "docling-confluent-cloud"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.this.id
    api_version = data.confluent_flink_region.this.api_version
    kind        = data.confluent_flink_region.this.kind
    environment {
      id = local.environment.id
    }
  }
  depends_on = [confluent_role_binding.environment_admin, confluent_role_binding.flink_admin]
}

# A *cluster* key. The scripts' producers and consumers, and example 03's
# connector; Flink reads and writes the topics as itself.
resource "confluent_api_key" "kafka" {
  display_name = "${local.ns}-kafka"
  description  = "docling-confluent-cloud"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = local.cluster.id
    api_version = local.cluster.api_version
    kind        = local.cluster.kind
    environment {
      id = local.environment.id
    }
  }
  depends_on = [confluent_role_binding.environment_admin]
}

# Examples 04 and 05 write typed records and their scripts produce and consume
# through the registry. Flink talks to Schema Registry as itself; this key is
# for them.
resource "confluent_api_key" "schema_registry" {
  count        = local.has_schema_registry ? 1 : 0
  display_name = "${local.ns}-schema-registry"
  description  = "docling-confluent-cloud"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = local.schema_registry.id
    api_version = local.schema_registry.api_version
    kind        = local.schema_registry.kind
    environment {
      id = local.environment.id
    }
  }
  depends_on = [confluent_role_binding.environment_admin]
}

# The Real-Time Context Engine's MCP endpoint takes a *global* key and rejects
# every other kind. Opt-in (GLOBAL_KEY="yes" in .env): it authenticates to
# everything the principal may use, and a principal may hold at most two.
# Minting one takes more than EnvironmentAdmin — OrganizationAdmin's Cloud key
# can; a service account's environment-scoped one gets 403.
resource "confluent_api_key" "global" {
  count        = local.global_key ? 1 : 0
  display_name = "${local.ns}-global-rtce"
  description  = "docling-confluent-cloud: Real-Time Context Engine"
  owner {
    id          = local.owner.id
    api_version = local.owner.api_version
    kind        = local.owner.kind
  }
  managed_resource {
    id          = "global"
    api_version = "global/v1"
    kind        = "Global"
  }
  depends_on = [confluent_role_binding.environment_admin]
}

# --- what goes back into .env ---------------------------------------------------

output "env" {
  description = "Written into .env by ./account.sh apply."
  sensitive   = true
  value = merge(
    {
      CC_ORG_ID          = data.confluent_organization.this.id
      CC_ENVIRONMENT_ID  = local.environment.id
      CC_CLUSTER_ID      = local.cluster.id
      CC_COMPUTE_POOL_ID = local.pool_id
      CC_PRINCIPAL_ID    = local.principal
      CC_CLOUD           = lower(local.cluster.cloud)
      CC_REGION          = local.cluster.region
      # Display names, not ids: a bare table name resolves against these, and
      # ids are accepted syntactically and then nothing resolves.
      CC_CATALOG  = local.environment.display_name
      CC_DATABASE = local.cluster.display_name
      # The API spells it SASL_SSL://pkc-....:9092; every client wants it bare.
      KAFKA_BOOTSTRAP_SERVERS = replace(local.cluster.bootstrap_endpoint, "/^[A-Za-z_]+:///", "")
      FLINK_API_KEY           = confluent_api_key.flink.id
      FLINK_API_SECRET        = confluent_api_key.flink.secret
      KAFKA_API_KEY           = confluent_api_key.kafka.id
      KAFKA_API_SECRET        = confluent_api_key.kafka.secret
    },
    local.has_schema_registry ? {
      SCHEMA_REGISTRY_ID         = local.schema_registry.id
      SCHEMA_REGISTRY_URL        = local.schema_registry.rest_endpoint
      SCHEMA_REGISTRY_API_KEY    = confluent_api_key.schema_registry[0].id
      SCHEMA_REGISTRY_API_SECRET = confluent_api_key.schema_registry[0].secret
    } : {},
    local.global_key ? {
      RTCE_API_KEY    = confluent_api_key.global[0].id
      RTCE_API_SECRET = confluent_api_key.global[0].secret
    } : {},
  )
}

# Which of the three this root made, for account.sh: those ids go back to
# Terraform blank, so a made thing stays made.
output "made" {
  value = compact([
    local.make_cluster ? "CC_CLUSTER_ID" : "",
    local.make_pool ? "CC_COMPUTE_POOL_ID" : "",
    local.make_principal ? "CC_PRINCIPAL_ID" : "",
  ])
}
