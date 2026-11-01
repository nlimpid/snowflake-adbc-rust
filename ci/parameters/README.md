# Test account parameters

Real-account tests read `SNOWFLAKE_TEST_*` from the environment. Nothing in this directory is encrypted, and GitHub Actions does not decrypt credentials. `cargo test` skips `crates/snowflake-testing/src/live.rs` when the variables are unset and still exits 0.

To run those four tests locally, export the variables yourself. The layout below is only for someone who wants to keep a passphrase-encrypted copy next to the repo. `ci/scripts/decrypt_parameters.sh` reads that copy; CI never calls it.

- `parameters_aws.json.gpg`
- `parameters_azure.json.gpg`
- `parameters_gcp.json.gpg`

Plaintext shape (never commit the `.json`):

```json
{
  "testconnection": {
    "SNOWFLAKE_TEST_ACCOUNT": "myorg-myacct",
    "SNOWFLAKE_TEST_USER": "SVC_USER",
    "SNOWFLAKE_TEST_PASSWORD": "...",
    "SNOWFLAKE_TEST_WAREHOUSE": "TEST_WH",
    "SNOWFLAKE_TEST_DATABASE": "TEST_DB",
    "SNOWFLAKE_TEST_SCHEMA": "PUBLIC",
    "SNOWFLAKE_TEST_ROLE": "TEST_ROLE"
  }
}
```

Create a file:

```bash
gpg --symmetric --cipher-algo AES256 -o ci/parameters/parameters_aws.json.gpg ci/parameters/parameters_aws.json
```

Store the passphrase as the `PARAMETERS_SECRET` CI secret. Each account needs a `LEGACY_SERVICE` user (password) or a `SERVICE` user with a key pair, a dedicated database, and a small warehouse.

Locally, either export the variables yourself or:

```bash
export PARAMETERS_SECRET='...'
source ci/scripts/decrypt_parameters.sh aws
```
