# Optimus Roll-Printer Assignment

Service that optimizes the queues for selected printers:
* Assigns new rolls to printers
* Orders the queues according to priority of rolls as well as smooth operations

## Local setup
After creating a local environment with the necessary requirements installed (requirements from `res-data-lite` as well as extra requirements listed in the local `requirements.txt`)

## Running locally

### Get roll and printer data from database

Run the script from the root directory (`optimus_roll_printer_assignment`).
For example, to run locally in dry run mode:
```python -m src.main '{"printers":["all"],"materials":["all"],"solver_settings":{"constraints":{"max_print_queue_yards":200,"keep_current_queue":true},"costs":{"material_switch_cost":0.1,"roll_expiration_cost":0.1,"daily_order_delay_cost":0.1,"prioritized_roll_value":10}},"verbose_solver_logging":false,"dry_run":true}'```

Here, the script, which requires an `event` and `context` is provided with the `event` only, which contains the solver settings. The context will then be obtained from the database.


### Provide roll and printer data

The script can also be run completely disconnected from any external database or Kafka. In this case, both the `event` and `context` need to be provided, where the `context` comprises of input data, in this case the rolls and printers for which the queue assignments are to be made, as records.

```python -m src.main '{"printers":["all"],"materials":["all"],"solver_settings":{"constraints":{"max_print_queue_yards":200,"keep_current_queue":true},"costs":{"material_switch_cost":0.1,"roll_expiration_cost":0.1,"daily_order_delay_cost":0.1,"prioritized_roll_value":10}},"verbose_solver_logging":false,"dry_run":true}' '{"printers":[{"printer_name":"JP7 LUNA","printer_status":"IDLE","machine_type":"Printer","printer_model":"MS JP7","active_roll_queue":["R31015_1:CFT97"],"material_capabilities":["CTSPS","CTNSP","RYJ01","FBTRY","PIQUE","CTNPT","OCTHR","TNCDO","BCICT","LY100","LN135","COMCT","OCFLN","HC293","CTW70","SECTN","LY115","CTNBA","CTJ95","PIMA7","LTCSL","CUTWL","OCT70"],"printer_record_id":"recBgpnWC0orjDiQe","current_print_queue_yards":12.76,"current_print_queue_materials":["CFT97"],"current_print_queue_roll_ids":["recPNW2vYqfzsBHaS"],"print_queue_remaining_yards":187.24}, {"printer_name":"JP7 NOVA","printer_status":"IDLE","machine_type":"Printer","printer_model":"MS JP7","active_roll_queue":["R32420:CT406"],"material_capabilities":["CC254","CTSPR","CT406","CFTGR","STCAN","CTPKT","OCTCJ","CTNBA","CFT97","CTFLN","CTF19","CLTWL","CTNL1"],"printer_record_id":"recTGqBANeDSkNrXV","current_print_queue_yards":0.0,"current_print_queue_materials":[],"current_print_queue_roll_ids":[],"print_queue_remaining_yards":200.0}, {"printer_name":"JP5 YODA","printer_status":"OFF","machine_type":"Printer","printer_model":"MS JP5","active_roll_queue":[],"material_capabilities":[],"printer_record_id":"recyJh5pWZMCcnxxu","current_print_queue_yards":0.0,"current_print_queue_materials":[],"current_print_queue_roll_ids":[],"print_queue_remaining_yards":0.0}, {"printer_name":"JP7 FLASH","printer_status":"FAILURE","machine_type":"Printer","printer_model":"MS JP7","active_roll_queue":["R33844: ECVIC"],"material_capabilities":["CHRST","GGT16","OCT70","CTNOX","OC135","CT170","TNSPJ","CDCBM","CUTWL","SLCTN","ECVIC"],"printer_record_id":"reczznci0L4UF9qAU","current_print_queue_yards":0.0,"current_print_queue_materials":[],"current_print_queue_roll_ids":[],"print_queue_remaining_yards":200.0}],"rolls":[{"material_code":"CFT97","roll_name":"R31015_1: CFT97","roll_status":"3. Unassigned","total_nested_length_yards":12.759074083081334,"days_since_pretreatment":34,"roll_utilization_nested_assets":0.7332801197173181,"roll_length_yards":17.4,"assigned_nests":["recu1Qgtnen86TxNE","receiMovavyfwKG1C","rec49HRMFLcGmdWqM","recfYFJdDZQYSAkLH","rec0vRP76hWodHSm2","rec5LRDmjJe4CvPCC"],"roll_print_priority":980,"assigned_print_assets":["reckXUVslAlfQlvrG","recVZ9wyKfapGz29g","recwhnkNVqPv8aVCg","recmMBG2uOKn1njll","rec7zZL50lZMG9gW4","recxM2VlKJk5UqOQ4"],"roll_record_id":"recPNW2vYqfzsBHaS","assigned_printer_name":"JP7 LUNA","is_prioritized":false,"currently_printing_on_printer":null,"roll_print_priority_unit_score":1.0,"is_currently_printing":false}, {"material_code":"SLCTN","roll_name":"R19128_1_E1: SLCTN","roll_status":"3. Unassigned","total_nested_length_yards":21.686944461194003,"days_since_pretreatment":8,"roll_utilization_nested_assets":0.9307701485490988,"roll_length_yards":23.3,"assigned_nests":["recZCirqSpKhb4sCA","recQSa1dKIbuXkmDw","recwpEQRrkFnfL8iW"],"roll_print_priority":1090,"assigned_print_assets":["rec9nym3mWDrboAu6","rectc4SAgEA6dGLRN","rec1Hixlr1M6BcKtG","rec5GKrVLbsxd4OmF","recgc7OYqWvQqgBsI","reckbr78XcczuToLI","recRrJrJ6Fzd0tRZw","recjbGs4TFgFI0XOW"],"roll_record_id":"recVk8hygYB8Nvn59","assigned_printer_name":null,"is_prioritized":false,"currently_printing_on_printer":null,"roll_print_priority_unit_score":1.0,"is_currently_printing":false}],"assets":[{"material_code":"SLCTN","rank":"Primary","asset_key":"10264184_SLCTN_1","days_since_ordered":31,"order_key":["TK-49164"],"print_asset_id":"rec1Hixlr1M6BcKtG","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10260510_SLCTN_1","days_since_ordered":22,"order_key":["TK-49406"],"print_asset_id":"rec5GKrVLbsxd4OmF","emphasis":1.0,"piece_type":null}, {"material_code":"CFT97","rank":"Healing","asset_key":"10255762_CFT97_3_PIECE_4","days_since_ordered":44,"order_key":["AW-2107786"],"print_asset_id":"rec7zZL50lZMG9gW4","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10261372_SLCTN_1","days_since_ordered":19,"order_key":["TK-49492"],"print_asset_id":"rec9nym3mWDrboAu6","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10261657_SLCTN_1","days_since_ordered":17,"order_key":["TK-49543"],"print_asset_id":"recRrJrJ6Fzd0tRZw","emphasis":1.0,"piece_type":null}, {"material_code":"CFT97","rank":"Primary","asset_key":"10259235_CFT97_1","days_since_ordered":28,"order_key":["KT-54480"],"print_asset_id":"recVZ9wyKfapGz29g","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10263261_SLCTN_1","days_since_ordered":12,"order_key":["TK-2111224"],"print_asset_id":"recgc7OYqWvQqgBsI","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10264183_SLCTN_1","days_since_ordered":33,"order_key":["TK-49121"],"print_asset_id":"recjbGs4TFgFI0XOW","emphasis":1.0,"piece_type":null}, {"material_code":"CFT97","rank":"Primary","asset_key":"10258533_CFT97_1","days_since_ordered":31,"order_key":["IZ-2108955"],"print_asset_id":"reckXUVslAlfQlvrG","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10262114_SLCTN_1","days_since_ordered":52,"order_key":["TK-48626"],"print_asset_id":"reckbr78XcczuToLI","emphasis":1.0,"piece_type":null}, {"material_code":"CFT97","rank":"Primary","asset_key":"10259560_CFT97_1","days_since_ordered":26,"order_key":["KT-54568"],"print_asset_id":"recmMBG2uOKn1njll","emphasis":1.0,"piece_type":null}, {"material_code":"SLCTN","rank":"Primary","asset_key":"10261406_SLCTN_1","days_since_ordered":24,"order_key":["TK-49322"],"print_asset_id":"rectc4SAgEA6dGLRN","emphasis":1.0,"piece_type":null}, {"material_code":"CFT97","rank":"Primary","asset_key":"10258731_CFT97_1","days_since_ordered":30,"order_key":["KT-54319"],"print_asset_id":"recwhnkNVqPv8aVCg","emphasis":1.0,"piece_type":null}, {"material_code":"CFT97","rank":"Healing","asset_key":"10255762_CFT97_3_PIECE_6","days_since_ordered":44,"order_key":["AW-2107786"],"print_asset_id":"recxM2VlKJk5UqOQ4","emphasis":1.0,"piece_type":null}]}'```

The following fields are needed for rolls:
```
    "roll_record_id": "recAxByC",
    "roll_name": "R12345: XXXX",
    "material_code": "XXXX",
    "roll_length_yards": 100,
    "total_nested_length_yards": 99,
    "assigned_nests": ["recScDvF","recWcWvEv"],
    "assigned_print_assets": ["recTdYfUf","recYsIfOg","recPrOtIt"],
    "roll_print_priority": 1000,
    "roll_utilization_nested_assets": 99,
    "assigned_printer_name": "JP7 LUNA",
    "roll_status": "3. Unassigned",
    "currently_printing_on_printer": null,
    "roll_print_priority_unit_score": 0.78,
    "is_currently_printing": false,
    "days_since_pretreatment": 5,
    "is_prioritized": false
```
For the print assets on these rolls, we need:
```
    "print_asset_id": "recASDF",
    "asset_key": "12345_XXXXX",
    "material_code": "XXXXX",
    "days_since_ordered": 3,
    "emphasis": 1.0,
    "order_key": ["AA-12344"],
    "rank": "Primary",
    "piece_type": null
```
And for printers, we need the following information:
```
    "active_roll_queue": ["R65432_1: YYYYY", "R44444: ZZZZZ"],
    "current_print_queue_materials": [],
    "current_print_queue_roll_ids": [],
    "current_print_queue_yards": 0.0,
    "machine_type": "Printer",
    "material_capabilities": ["XXXXX", "YYYYY", ...],
    "print_queue_remaining_yards": 200.0,
    "printer_model": "MS JP7",
    "printer_name": "JP7 FLASH",
    "printer_record_id": "reczzQWERTY",
    "printer_status": "IDLE"
```