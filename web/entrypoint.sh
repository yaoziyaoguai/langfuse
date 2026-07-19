#!/bin/sh

# Check whether a database URL's credentials contain characters that typically
# need percent-encoding for Prisma (@ : / % # ?).  Best-effort heuristic —
# strips the scheme, extracts the authority (user:pass@host) before the first
# slash, and checks for common offenders.  Strips %XX sequences first so
# partially-encoded values are caught.
check_unencoded_credentials() {
    _url="$1"
    _no_scheme="${_url#*://}"
    # Extract authority (before first /) so @/# in path or query params
    # don't confuse credential parsing.
    _authority="${_no_scheme%%/*}"
    case "$_authority" in
        *@*)
            _host_part="${_authority##*@}"
            _creds="${_authority%@"$_host_part"}"
            _user="${_creds%%:*}"
            _pass="${_creds#*:}"
            _found=""
            for _val in "$_user" "$_pass"; do
                # Strip valid percent-encoded sequences before checking so
                # partially-encoded values like p%40ss@word are still caught.
                _stripped=$(printf '%s' "$_val" | sed 's/%[0-9A-Fa-f][0-9A-Fa-f]//g')
                case "$_stripped" in
                    *@*|*:*|*/*|*%*|*'#'*|*'?'*) _found="true" ;;
                esac
            done
            if [ "$_found" = "true" ]; then
                echo "HINT: Your DATABASE_URL / DIRECT_URL credentials appear to contain special characters (@, :, /, %, #, ?) that are not URL-encoded."
                echo "  Prisma requires these to be percent-encoded, otherwise you will see P1013 errors."
                echo "  Example: p@ssword → p%40ssword"
                echo "  Reference: https://www.prisma.io/docs/orm/reference/connection-urls#special-characters"
            fi
            ;;
    esac
}

# Check whether CLICKHOUSE_PASSWORD contains characters that would break the
# query-string interpolation used in the migration script (& = # ? % + @ space).
# Strips %XX sequences first so partially-encoded values are caught.
check_clickhouse_password() {
    _pass="$1"
    if [ -z "$_pass" ]; then
        return
    fi
    # Strip valid percent-encoded sequences before checking so
    # partially-encoded values are still caught.
    _stripped=$(printf '%s' "$_pass" | sed 's/%[0-9A-Fa-f][0-9A-Fa-f]//g')
    case "$_stripped" in
        *'&'*|*'='*|*'#'*|*'?'*|*'%'*|*'+'*|*'@'*|*' '*)
            echo "HINT: Your CLICKHOUSE_PASSWORD contains special characters (&, =, #, ?, %, +, @, space) that may break the migration URL."
            echo "  These characters need to be percent-encoded when passed as query parameters."
            echo "  Example: p@ss&word → p%40ss%26word"
            ;;
    esac
}

# Run cleanup script before running migrations
# Check if DATABASE_URL is not set
if [ -z "$DATABASE_URL" ]; then
    # Check if all required variables are provided
    if [ -n "$DATABASE_HOST" ] && [ -n "$DATABASE_USERNAME" ] && [ -n "$DATABASE_PASSWORD" ]  && [ -n "$DATABASE_NAME" ]; then
        # Construct DATABASE_URL from the provided variables
        DATABASE_URL="postgresql://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}/${DATABASE_NAME}"
        export DATABASE_URL
    else
        echo "Error: Required database environment variables are not set. Provide a postgres url for DATABASE_URL."
        exit 1
    fi
    if [ -n "$DATABASE_ARGS" ]; then
        # Append ARGS to DATABASE_URL
        DATABASE_URL="${DATABASE_URL}?$DATABASE_ARGS"
        export DATABASE_URL
    fi
fi

ANALYTICS_BACKEND="${LANGFUSE_ANALYTICS_BACKEND:-clickhouse}"
case "$ANALYTICS_BACKEND" in
    clickhouse|doris) ;;
    *)
        echo "Error: LANGFUSE_ANALYTICS_BACKEND must be one of: clickhouse, doris"
        exit 1
        ;;
esac

# Set DIRECT_URL to the value of DATABASE_URL if it is not set, required for migrations
if [ -z "$DIRECT_URL" ]; then
    export DIRECT_URL="${DATABASE_URL}"
fi

# Always execute the postgres migration, except when disabled.
if [ "$LANGFUSE_AUTO_POSTGRES_MIGRATION_DISABLED" != "true" ]; then
    prisma db execute --url "$DIRECT_URL" --file "./packages/shared/scripts/cleanup.sql"

    # Apply migrations
    prisma migrate deploy --schema=./packages/shared/prisma/schema.prisma
fi
status=$?

# If migration fails (returns non-zero exit status), exit script with that status
if [ $status -ne 0 ]; then
    echo "Applying database migrations failed. Common causes:"
    echo "  1. The database is unavailable or unreachable."
    echo "  2. DATABASE_URL / DIRECT_URL credentials contain special characters that are not URL-encoded."
    check_unencoded_credentials "$DIRECT_URL"
    echo "Exiting..."
    exit $status
fi

if [ "$ANALYTICS_BACKEND" = "clickhouse" ]; then
    if [ -z "$CLICKHOUSE_URL" ]; then
        echo "Error: CLICKHOUSE_URL is required when LANGFUSE_ANALYTICS_BACKEND=clickhouse."
        exit 1
    fi
    if [ "$LANGFUSE_AUTO_CLICKHOUSE_MIGRATION_DISABLED" != "true" ]; then
        migration_attempt=1
        migration_max_attempts=30
        while :; do
            cd ./packages/shared
            sh ./clickhouse/scripts/up.sh
            status=$?
            cd ../../
            if [ $status -eq 0 ] || [ $migration_attempt -ge $migration_max_attempts ]; then
                break
            fi
            echo "ClickHouse migration attempt ${migration_attempt}/${migration_max_attempts} failed; retrying in 2 seconds."
            migration_attempt=$((migration_attempt + 1))
            sleep 2
        done
    fi
    if [ $status -ne 0 ]; then
        echo "Applying ClickHouse migrations failed. Common causes:"
        echo "  1. The database is unavailable or unreachable."
        echo "  2. CLICKHOUSE_PASSWORD contains special characters that are not URL-encoded."
        check_clickhouse_password "$CLICKHOUSE_PASSWORD"
        echo "Exiting..."
        exit $status
    fi
else
    # Doris migrations require a dedicated migrator identity and therefore run
    # as a separate one-shot workload. Web/worker readiness fails closed until
    # that workload has applied the expected checksummed schema.
    echo "Doris analytics selected; expecting the one-shot Doris migrator to have completed."
fi

# Run the command passed to the docker image on start
exec "$@"
