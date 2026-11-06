# One example's setup: the things that happen once, before its pipeline can be
# deployed. Every example's terraform/main.tf calls this, and the example
# contributes names and statements, not code — exactly as with the scripts this
# replaced:
#
#   env.sh      the names, all from NS          -> var.env
#   flink/      the statements that are its own; the rest come from common/flink
#
# What it creates, in dependency order:
#
#   artifacts     the Python UDF, and the Java one when JAVA_UDF_DIR is set
#   connections   05_connection_*.sql
#   tables        00_table_*.sql — and with them the topics
#   functions     10_function_*.sql, bound to the artifacts above
#
# The long-running statements (3*.sql) and their views are NOT here: they are
# `./pipeline.sh deploy`, which leaves an unchanged statement running and sets
# its start offsets at the moment it replaces one — a lifecycle Terraform has no
# word for.
#
# Two things Terraform cannot do on its own, and how this does them:
#
# * A table is never dropped. Deleting the statement that created it deletes a
#   record of a finished DDL statement, not the table — so `destroy` leaves the
#   topics and their data alone, by construction. `./setup.sh destroy --topics`
#   is the one way to drop them.
# * A function or a connection IS dropped, and rebound when what it points at
#   changes. CREATE FUNCTION and CREATE CONNECTION both fail on an existing
#   name, so each has a terraform_data beside it whose provisioners run the
#   example's own drop — before the create, and on destroy — through
#   common/tf-hook.sh.

terraform {
  required_providers {
    confluent = {
      source = "confluentinc/confluent"
    }
  }
}

locals {
  env  = var.env
  repo = var.env.REPO_ROOT
  hook = "${local.repo}/common/tf-hook.sh"
  app  = lower(var.env.APP_NAME)

  # --- the statement files ------------------------------------------------
  #
  # Merged by file name, the example's first, so the numeric prefixes order the
  # whole set and an example may replace a shared statement by giving a file
  # the same name — lib.sh's sql_stage, in HCL. An example with no chunks in it
  # sets USE_COMMON_FLINK=no and then only its own directory counts.
  dirs = concat(
    ["${var.example_dir}/flink"],
    lookup(var.env, "USE_COMMON_FLINK", "yes") == "no" ? [] : ["${local.repo}/common/flink"],
  )
  by_name = {
    for pair in flatten([for d in local.dirs : [for f in fileset(d, "*.sql") : { name = trimsuffix(f, ".sql"), path = "${d}/${f}" }]]) :
    pair.name => pair.path...
  }
  files = { for name, paths in local.by_name : name => paths[0] }

  connection_files = { for k, p in local.files : k => p if startswith(k, "05_connection_") }
  table_files      = { for k, p in local.files : k => p if startswith(k, "00_table_") }
  function_files   = { for k, p in local.files : k => p if startswith(k, "10_function_") }

  # --- the artifacts --------------------------------------------------------

  artifacts = merge(
    {
      python = {
        dir      = var.env.PY_UDF_DIR
        name     = var.env.ARTIFACT_NAME
        language = "PYTHON"
        format   = "ZIP"
      }
    },
    lookup(var.env, "JAVA_UDF_DIR", "") == "" ? {} : {
      java = {
        dir      = var.env.JAVA_UDF_DIR
        name     = var.env.JAVA_ARTIFACT_NAME
        language = "JAVA"
        format   = "JAR"
      }
    },
  )

  # What the artifact is built FROM, not the built file: a zip holding an
  # sdist, or a shaded JAR, carries timestamps, so its hash changes on every
  # build of the same code. The sources only change when the code does.
  artifact_sources = {
    for k, a in local.artifacts : k => sha256(join("\n", [
      for f in sort(setunion([for g in ["src/**", "*.toml", "pom.xml", "*.py", "build.sh"] : fileset("${local.repo}/${a.dir}", g)]...)) :
      "${f} ${filesha256("${local.repo}/${a.dir}/${f}")}"
      if !strcontains(f, "__pycache__") && !strcontains(f, "/test/")
    ]))
  }

  # --- rendering ------------------------------------------------------------
  #
  # templatefile() is render.py: `${NAME}` only, and an unset name is an error,
  # not an empty string. The artifact ids are this module's own, so they win
  # over a stale copy in the example's .env.
  produced = merge(
    {
      ARTIFACT_ID      = confluent_flink_artifact.udf["python"].id
      ARTIFACT_VERSION = local.versions["python"]
    },
    contains(keys(local.artifacts), "java") ? {
      JAVA_ARTIFACT_ID      = confluent_flink_artifact.udf["java"].id
      JAVA_ARTIFACT_VERSION = local.versions["java"]
    } : {},
  )
  versions = { for k, a in confluent_flink_artifact.udf : k => a.versions[length(a.versions) - 1].version }

  # A statement that holds a secret is rendered with the secrets marked
  # sensitive, so its text is hidden in a plan; every other statement is
  # rendered without them, so its diff stays readable.
  secret_names = [for k in keys(var.env) : k if can(regex("(SECRET|API_KEY|KEY_ID|TOKEN|PASSWORD)$", k))]
  plain_vars   = merge({ for k, v in var.env : k => v if !contains(local.secret_names, k) }, local.produced)
  all_vars     = merge(local.plain_vars, { for k in local.secret_names : k => sensitive(var.env[k]) })

  connection_sql = {
    for k, p in local.connection_files : k => can(templatefile(p, local.plain_vars)) ? templatefile(p, local.plain_vars) : templatefile(p, local.all_vars)
  }
  table_sql = {
    for k, p in local.table_files : k => templatefile(p, local.plain_vars)
  }
  function_sql = {
    for k, p in local.function_files : k => templatefile(p, local.plain_vars)
  }

  # The connection a 05_connection_*.sql creates, read off the unrendered
  # file — so the name is known at plan time and never sensitive.
  connection_names = {
    for k, p in local.connection_files :
    k => var.env[regex("CREATE CONNECTION `\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}`", file(p))[0]]
  }

  statement_properties = {
    # Display names, not ids — the detail that costs an afternoon.
    "sql.current-catalog"  = var.env.CC_CATALOG
    "sql.current-database" = var.env.CC_DATABASE
    "sql.state-ttl"        = var.env.DEDUP_TTL
  }
}

# `<app>-setup-<kind>-<name>-<hash>`: a statement name cannot be reused, so it
# carries a hash of its SQL — a changed statement gets a new name, and so a new
# statement. `-setup-` keeps these out of the scripts' `$APP_NAME-*` sweeps:
# they are Terraform's to delete.
locals {
  statement_name = {
    for k in keys(local.files) :
    k => "${local.app}-setup-${replace(replace(k, "/^[0-9]+_/", ""), "_", "-")}"
  }
}

# --- artifacts ----------------------------------------------------------------

resource "terraform_data" "artifact_source" {
  for_each = local.artifacts
  input    = local.artifact_sources[each.key]
}

# Built by ./setup.sh before Terraform runs — var.artifact_files is the path
# each build.sh printed. A new upload only when the sources changed; the name
# carries their hash because a display name is unique per cloud, region and
# environment, and the replacement is created before the old one is deleted,
# so the functions are rebound to the new one without a gap.
resource "confluent_flink_artifact" "udf" {
  for_each = local.artifacts

  display_name     = "${each.value.name}-${substr(local.artifact_sources[each.key], 0, 8)}"
  description      = "docling-ibm-examples ${local.app}"
  cloud            = upper(var.env.CC_CLOUD)
  region           = var.env.CC_REGION
  runtime_language = each.value.language
  content_format   = each.value.format
  # Absent only when nothing is built — a destroy, or `./setup.sh tf ...`.
  artifact_file = lookup(var.artifact_files, each.key, null)
  environment {
    id = var.env.CC_ENVIRONMENT_ID
  }

  lifecycle {
    create_before_destroy = true
    replace_triggered_by  = [terraform_data.artifact_source[each.key]]
  }
}

# --- connections --------------------------------------------------------------
#
# A CONNECTION holds an endpoint and a secret, and is the only route out of
# Confluent Cloud a UDF has. Its type here is REST, which the provider's own
# confluent_flink_connection does not offer — so it is a statement, with the
# drop beside it.

resource "terraform_data" "connection" {
  for_each = local.connection_files

  triggers_replace = sha256(local.connection_sql[each.key])
  input = {
    hook        = local.hook
    example_dir = var.example_dir
    name        = local.connection_names[each.key]
  }

  # Before the CREATE, because CREATE CONNECTION fails on an existing name —
  # one left by an earlier run, or by the scripts this replaced.
  provisioner "local-exec" {
    command = "\"$HOOK\" \"$EXAMPLE_DIR\" drop-connection \"$NAME\""
    environment = {
      HOOK        = self.input.hook
      EXAMPLE_DIR = self.input.example_dir
      NAME        = self.input.name
    }
  }

  provisioner "local-exec" {
    when       = destroy
    on_failure = continue
    command    = "\"$HOOK\" \"$EXAMPLE_DIR\" drop-connection \"$NAME\""
    environment = {
      HOOK        = self.input.hook
      EXAMPLE_DIR = self.input.example_dir
      NAME        = self.input.name
    }
  }
}

resource "confluent_flink_statement" "connection" {
  for_each = local.connection_files

  statement_name = "${local.statement_name[each.key]}-${nonsensitive(substr(sha256(local.connection_sql[each.key]), 0, 8))}"
  statement      = local.connection_sql[each.key]
  properties     = local.statement_properties

  depends_on = [terraform_data.connection]
  lifecycle {
    replace_triggered_by = [terraform_data.connection[each.key]]
    precondition {
      condition = (
        !can(regex("connection_docling", each.key)) ||
        lookup(var.env, "DOCLING_SERVICE_URL", "") != ""
      )
      error_message = "DOCLING_SERVICE_URL must be set in .env"
    }
  }
}

# --- tables -------------------------------------------------------------------
#
# CREATE TABLE IF NOT EXISTS, which in Confluent Cloud creates the topic — with
# the partitions and the retention the statement says. A changed file is a new
# statement that finds the table already there and does nothing: a table's
# schema is changed by hand, or by dropping it, never by a plan.

resource "confluent_flink_statement" "table" {
  for_each = local.table_files

  statement_name = "${local.statement_name[each.key]}-${substr(sha256(local.table_sql[each.key]), 0, 8)}"
  statement      = local.table_sql[each.key]
  properties     = local.statement_properties
}

# --- functions ----------------------------------------------------------------
#
# Bound to an artifact by id and version, so a new artifact means DROP and
# CREATE again: the terraform_data is replaced when the rendered SQL changes,
# its destroy-time drop runs, and then the statement is created anew. After the
# connections, because a Java function is bound to them by name.

resource "terraform_data" "function" {
  for_each = local.function_files

  triggers_replace = local.function_sql[each.key]
  input = {
    hook        = local.hook
    example_dir = var.example_dir
    drop_file   = lookup(local.files, replace(each.key, "10_function_", "91_drop_function_"), "")
  }

  provisioner "local-exec" {
    command = "[ -z \"$DROP_FILE\" ] || \"$HOOK\" \"$EXAMPLE_DIR\" run \"$DROP_FILE\""
    environment = {
      HOOK        = self.input.hook
      EXAMPLE_DIR = self.input.example_dir
      DROP_FILE   = self.input.drop_file
    }
  }

  provisioner "local-exec" {
    when       = destroy
    on_failure = continue
    command    = "[ -z \"$DROP_FILE\" ] || \"$HOOK\" \"$EXAMPLE_DIR\" run \"$DROP_FILE\""
    environment = {
      HOOK        = self.input.hook
      EXAMPLE_DIR = self.input.example_dir
      DROP_FILE   = self.input.drop_file
    }
  }

  # Dropped before the connections on destroy, created after them.
  depends_on = [confluent_flink_statement.connection]
}

resource "confluent_flink_statement" "function" {
  for_each = local.function_files

  statement_name = "${local.statement_name[each.key]}-${substr(sha256(local.function_sql[each.key]), 0, 8)}"
  statement      = local.function_sql[each.key]
  properties     = local.statement_properties

  depends_on = [terraform_data.function]
  lifecycle {
    replace_triggered_by = [terraform_data.function[each.key]]
  }
}
