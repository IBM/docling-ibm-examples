variable "env" {
  description = "The example's layered configuration — .env, env.sh, the example's .env, .env.local — as lib.sh's load_env sees it."
  type        = map(string)
}

variable "example_dir" {
  description = "Absolute path of the example: its flink/ is read from here, and the provisioners load its env.sh."
  type        = string
}

variable "artifact_files" {
  description = "python (and java, when the example has one) -> the file its build.sh produced."
  type        = map(string)
}
