# Set up your Confluent Cloud account

`account.sh` prepares your Confluent Cloud account for the examples in
[`05-confluent-cloud`](../../05-confluent-cloud/). You run it once, before any
example, and every example then reads what it wrote to `.env`.

Run every command on this page from the `05-confluent-cloud/` folder.

## Get started

**1. Install the tools.**

| Tool | Needed for | Install |
|---|---|---|
| Python 3 | `init` | [python.org](https://www.python.org/downloads/) |
| [Terraform](https://developer.hashicorp.com/terraform/install) 1.5 or later | `plan`, `apply`, `destroy`, `tf` | On macOS: `brew tap hashicorp/tap && brew install hashicorp/tap/terraform` |
| [jq](https://jqlang.org/download/) and curl | `check`, `plan`, `apply`, `destroy`, `tf` | Often already installed |
| [uv](https://docs.astral.sh/uv/) | `check`, `topics` | See its install page |

**2. Create a Confluent Cloud API key.** In <https://confluent.cloud>, open the
top-right menu, then **User settings** > **API keys** > **Add API key** >
**My account** > **Cloud resource management**. Copy the secret, because it is
shown only once.

To create everything from scratch, the key's owner needs **OrganizationAdmin**,
which whoever created the account has. See
[Permissions](#permissions) if you don't have that role.

**3. Fill in `.env`.** `init` asks only for what your chosen examples need, and
says where to get each value:

```bash
cd 05-confluent-cloud
./account.sh init
```

**4. See what would be created.** This creates nothing:

```bash
./account.sh plan
```

**5. Create it.** `apply` shows the plan and asks before it changes anything.
The resources it creates are billed, as described under [Cost](#cost):

```bash
./account.sh apply
```

**6. Confirm that it works:**

```bash
./account.sh check
```

You can now [run an example](../../05-confluent-cloud/README.md#quick-start).
When you finish, [clean up](../../05-confluent-cloud/README.md#cleaning-up) so
that you stop paying for it.

## Commands

| Command | What it does | Changes your Confluent account? | Writes `.env`? |
|---|---|---|---|
| [`init`](#init) | Asks for each value | No | Yes, after you confirm |
| [`check`](#check) | Reports what your keys can see | No | No |
| [`plan`](#plan) | Shows what `apply` would do | No | No |
| [`apply`](#apply) | Creates what is missing, and the API keys | **Yes, billed** | Yes |
| [`destroy`](#destroy) | Deletes the keys and whatever `apply` created | **Yes, deletes data** | Yes, clears values |
| [`info`](#info) | Prints the ids an example will use | No | No |
| [`topics`](#topics) | Lists the topics on the cluster | No | No |
| [`tf`](#tf) | Runs any other Terraform command | Depends on the command | No |

### `init`

Fills in `.env` by asking questions. For each value, it says what the value is,
where to get it, and whether it is required.

| You choose | It asks for |
|---|---|
| Any example | `CONFLUENT_CLOUD_API_KEY`, `CONFLUENT_CLOUD_API_SECRET`, `NS` |
| Example 02, 03, 04 or 05 | `DOCLING_SERVICE_URL`, `DOCLING_API_KEY` |
| Example 04 | `GLOBAL_KEY` |
| Example 05 | `SEC_USER_AGENT` |
| New resources, when `.env` names no cluster | `CC_CLOUD`, `CC_REGION` |
| Existing resources | `CC_ENVIRONMENT_ID`, `CC_CLUSTER_ID`, `CC_COMPUTE_POOL_ID`, `CC_PRINCIPAL_ID` |

- You can run it again. Press Enter to keep a value, or type `-` to clear an
  optional one. Comments and other values in `.env` stay as they are.
- Nothing is written until you confirm, so Ctrl-C leaves `.env` unchanged.
- What you type for a secret stays hidden, and `.env` ends up readable only by
  you.
- It refuses values containing `"`, `$`, a backtick or a backslash, because the
  shell reads `.env`. If a value needs one of these characters, edit `.env`
  yourself and escape it.
- At the end, it offers to run `check`.

It needs a terminal. In a script, copy `.env.example` to `.env` and edit that
instead.

### `check`

Reads your account and creates nothing. It reports, in this order:

1. Whether the Cloud API key works, and which organizations it can see
2. Environments
3. Kafka clusters
4. Flink compute pools
5. Flink artifacts in your region
6. Whether the Flink API key works
7. The topics on the cluster, using the Kafka API key
8. Whether Docling answers on `DOCLING_SERVICE_URL`
9. Your namespace, and a reminder about compute pool billing

Steps 5 to 7 report little until `apply` has run. Use `check` on a shared account
to find the ids to put in `.env`.

### `plan`

Shows what `apply` would create, change or delete, and creates nothing. Read the
output before you run `apply`. Any arguments go to `terraform plan`.

### `apply`

Runs `terraform apply`, which shows the plan and asks you to confirm. It then
writes every id and key into `.env`. Any arguments go to `terraform apply`.

For each resource, it uses what `.env` names, and otherwise creates it:

| Resource | When `.env` names it | When `.env` doesn't name it |
|---|---|---|
| Environment (`CC_ENVIRONMENT_ID`) | Looks it up | Looks up the one called `default`. It never creates an environment |
| Kafka cluster (`CC_CLUSTER_ID`) | Looks it up | Creates a Basic, single-zone cluster called `<NS>-docling` in `CC_CLOUD`/`CC_REGION` |
| Flink compute pool (`CC_COMPUTE_POOL_ID`) | Looks it up. It must be in the cluster's region | Creates `<NS>-docling` in the cluster's region, up to `CC_POOL_MAX_CFU` (default 10) |
| Key owner (`CC_PRINCIPAL_ID`) | Uses that user or service account | Creates the service account `<NS>-docling`, with EnvironmentAdmin and FlinkAdmin on the environment |

It always creates these API keys, owned by the key owner above:

| Key | Name | Created when | Used for |
|---|---|---|---|
| Flink | `<NS>-flink` | Always | Submitting Flink statements |
| Kafka | `<NS>-kafka` | Always | The scripts' producers and consumers, and example 03's connector |
| Schema Registry | `<NS>-schema-registry` | The environment has Stream Governance | Examples 04 and 05 |
| Global | `<NS>-global-rtce` | `GLOBAL_KEY="yes"` | Example 04's lightning queries |

A new key can take a few seconds to become usable.

Once `apply` has created a cluster, a pool or a service account, it keeps
managing that one. Editing its id in `.env` afterwards has no effect, so to
switch to a different one, run `destroy` first.

### `destroy`

Runs `terraform destroy`, which shows the plan and asks you to confirm. It
deletes the API keys and whatever `apply` created, **including a cluster with
every topic in it**. It never deletes anything that `.env` named when you ran
`apply`, because those are only looked up. It then clears the deleted keys and
ids from `.env`.

Run it last, after `./setup.sh destroy` in every example you ran, because those
scripts use these keys.

### `info`

Prints the organization, environment, cluster, compute pool, region, bootstrap
server, namespace and Docling URL. It prints no secrets.

### `topics`

Lists the topics on the cluster, using the Kafka API key, so it works after
`apply`.

### `tf`

Runs any other Terraform command with your `.env` settings, for example
`./account.sh tf state list`. Use this rather than running `terraform` directly,
which wouldn't have your settings.

## Permissions

A Cloud API key can do what its owner can do, and nothing more.

| Goal | The key's owner needs |
|---|---|
| Create everything from scratch | **OrganizationAdmin** |
| Create the cluster and pool, with `CC_PRINCIPAL_ID` set to the key's owner | **EnvironmentAdmin** and **FlinkAdmin** on the environment |
| Use an existing environment, cluster, pool and owner | **EnvironmentAdmin** and **FlinkAdmin** on the environment |
| Create the global key (`GLOBAL_KEY="yes"`) | **OrganizationAdmin** |

Examples 04 and 05 also need Stream Governance enabled in the environment, for
their Schema Registry.

## Files

| File | What it holds | In git? |
|---|---|---|
| `.env` | Your credentials, ids and settings. [`.env.example`](../../05-confluent-cloud/.env.example) explains each value | No |
| `.env.local` | Optional private overrides, which win over `.env` and the example's own settings | No |
| `terraform/account/terraform.tfstate` | Terraform's record of what `apply` created, **including every secret** | No |
| `terraform/account/.terraform.lock.hcl` | The exact Terraform provider version | Yes |

Keep the state file until you have run `destroy`. Without it, Terraform no longer
knows what it created, and `destroy` can't remove those resources, so you would
have to delete them in the Confluent Cloud console.

## Cost

`init`, `check`, `plan`, `info` and `topics` cost nothing. After `apply`:

- **Kafka cluster:** a Basic cluster is billed by use, with no minimum.
- **Flink compute pool:** billed by CFU while statements run, and it scales to
  zero when none are running. A pipeline left deployed keeps costing money until
  you run `./pipeline.sh delete` in that example.

See [Confluent's pricing](https://www.confluent.io/confluent-cloud/pricing/) for
current rates.

## Troubleshooting

| What you see | What it means | What to do |
|---|---|---|
| `no .env` | You haven't created the file yet | Run `./account.sh init` |
| `NS is not set in .env` | Every name is built from `NS`, so it is required | Run `./account.sh init` |
| `unset in .env / env.sh: ...` | A value that command needs is empty | Set the named values. Those marked `[auto]` in `.env.example` come from `./account.sh apply` |
| `terraform is not installed` | Terraform isn't on your `PATH` | Install it, as in [step 1](#get-started) |
| Homebrew: `Your Command Line Tools are too outdated` | Homebrew won't install Terraform until they are updated. This can change the compiler your other projects use | Update them from Software Update in System Settings |
| Homebrew: `A newer Command Line Tools release is available` | Only a warning, and the install still worked | Ignore it. **Don't run the `sudo rm -rf` it suggests**, which deletes your compiler |
| `terraform init failed`, with `bad response code: 504` | GitHub failed to serve the provider download. This is usually temporary | Run the command again in a few minutes |
| `the Cloud API key was rejected` | The key or secret is wrong | Check both values in `.env`, or create a new key |
| `authenticated, but this key can see no organizations` | The key works, but its owner has no roles | Create the key under your own user, or grant its owner a role. See [Permissions](#permissions) |
| `the Flink API key was rejected` | The key may be too new, or for another region | Wait a few seconds and run `check` again. It must be a Flink key for `CC_CLOUD`/`CC_REGION` |
| `the Docling API key was rejected` | `DOCLING_API_KEY` is wrong | Copy it again from the `API keys` tab in the Docling for IBM watsonx Workbench |
| `could not reach ...` for Docling | `DOCLING_SERVICE_URL` is wrong or unreachable | Copy it again from the `API examples` tab in the Workbench |
| `The compute pool ... is in ..., the cluster in ...` | A pool can't read topics in another region | Name a pool in the cluster's region, or clear `CC_COMPUTE_POOL_ID` |
| HTTP 403 when `apply` creates the global key | Creating it needs OrganizationAdmin | Set `GLOBAL_KEY="no"`, or use a key from an OrganizationAdmin |
| `./account.sh init asks questions, so it needs a terminal` | It ran in a script or pipeline | Copy `.env.example` to `.env` and edit it |
