set -euo pipefail

# TODO: replace me with a node script, probably
push_schema () {
    cd $GRAPHQL_TOOLING_DIR

    mkdir -p ./out
    COMPILED_PATH=./out/schema.graphql

    touch $COMPILED_PATH
    > $COMPILED_PATH

    for file in $(find $GRAPHQL_SCHEMA_DIR -name *\.graphql); do
      cat $file >> $COMPILED_PATH
    done

    curl -X POST localhost:8080/admin/schema --data-binary '@out/schema.graphql'
}

if command -v jq &> /dev/null
then
    push_schema | jq
else
    echo "printing raw json. Install jq for nicer output"
    push_schema
fi
