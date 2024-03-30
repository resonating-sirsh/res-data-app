my_list=["item1", "item2", "item3"]

for item in "${my_list[@]}"; do
    echo "Processing item: $item"
    # Add your processing logic here
done