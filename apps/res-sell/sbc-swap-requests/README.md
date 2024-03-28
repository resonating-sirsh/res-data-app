Listen for topic: `res_sell.swap_material_sell.swap_material_sell` in the kafka stream.

This app creates an entry in `redis` with all the style that are being swap to materials.

This entry is call sbc-swap-requests and this is the data shape:
```javascript
{
    // redis key
    ["swap_material_id"]: {
        ["sbc_id": {
            "sbc_id": <sbc_id>,
            "swap_id": <swap_material_id>,
            "style_ids": [<style_id>, ...],
            "from_sbcs": [<sbc_id>...],
            "to_sbcs": [<sbc_id>...],
            "from": "<from_material_code>",
            "to": "<to_material_code>",
        }
    }
}
```

