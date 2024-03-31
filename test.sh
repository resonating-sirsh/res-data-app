list='[a,b,c]'

items=$(echo "$list" | tr -d '[]' | tr ',' '\n')

# Read the items into an array
readarray -t items_array <<< "$items"

# Loop over the items
for item in "${items_array[@]}"; do
    echo "Processing item: $item"
    # Add your processing logic here
done