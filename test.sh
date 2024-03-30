json1='["a", "b", "c"]'
json2='["d", "e", "f"]'

merged_json=$(jq -n --argjson a "$json1" --argjson b "$json2" '$a + $b')

echo "$merged_json"