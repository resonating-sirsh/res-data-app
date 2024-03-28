import json
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any
from warnings import filterwarnings

# import openai_image_to_json
import pandas as pd

import res
from . import openai_image_to_json
from res.connectors.airtable.AirtableUpdateQueue import AirtableQueue
from res.utils import ping_slack

filterwarnings("ignore")

disable_output_for_testing = False  # if you are testing something locally and you dont want ot update airtable and the slack channel

slack_id_nofify = " <@U04HYBREM28> "

# svot = airtable["apprcULXTWu33KFsh"]["tbllUsoGfM117fcvI"].to_dataframe()
# this is just needed for finding missing cols
svot_cols_in_airtable = {
    "Wire Belt 2 and 3 Relaxed Dryer",
    "Production Wash Locked Settings Gate",
    'Fold Long Sensor ("FL") - MS Steamer',
    "Temp. Tetto Warmup - MS Steamer",
    "Batcher Speed - Found Stenter - Production Dry",
    "Print Locked Settings Gate",
    "Steam Roof Temperature",
    "Boiler Temperature Set - MS Steamer",
    "Scour Wash Temperature Chamber 4 (⁰C)",
    "Steam B-Exhaust Speed (HZ)  - Found Steamer",
    "ULTRATEX",
    "Approved Material",
    "Production Wash - Eriopon Dosing for Machine Start up Chamber 2",
    "Steam Ring  - Found Steamer",
    "Pilot Speed (m/min) Relaxed Dryer",
    "Manifold Pressure (MPa) - Found Steamer - Tolerance",
    "Scour Plaiter Relaxed Dryer",
    "Steam Temperature (C)  - Found Steamer - Tolerance",
    "Head Height",
    "Material Type",
    "Scour Dry Overfeed Ratio",
    "Production Wash - Eriopon Dosing for Machine Start up - Chamber 3",
    "Locked Substation Settings Gate",
    "Steam Fan  - Found Steamer",
    "Scour Bend Bars",
    "Softening Channel",
    "Waveform",
    "Pretreatment Width 2 (mm)",
    "PLANT Switch - MS Steamer",
    "Scour Exhaust Blower Relaxed Dryer",
    "Power D Heater",
    "Pretreat Settings Locked",
    "Steam B-Exhaust  - Found Steamer",
    "Plaiter Switch - MS Steamer",
    "Tension Group [Washer]",
    "Heat Blowers 3 and 4 Relaxed Dryer",
    "Pressure Roller Temperature - Tolerance",
    "Scour Pilot Speed (m/min) Relaxed Dryer",
    "Softening Type",
    "Production Wash Temperature Chamber 1 (C)",
    "Comment",
    "Production Wash - Eriopon Dosing - Chamber 2",
    "Pretreatment Width 3 (mm)",
    "Print Belt Temperature",
    "Production Wash Bend Bars",
    "Pretreatment Pressure",
    "Stenter Dry Squeezer Pressure",
    "Scour Dry Width 1 (mm)",
    "Scour Ultravon Initial Qty",
    'Pneumatic Press ("Folder") Switch - MS Steamer',
    "Power D Temperature - Tolerance",
    "Unwinder Pressure",
    "Recirculation Fans - MS Steamer",
    "Scour Invatex Initial Qty",
    "Wire Belt 4 and 5 Relaxed Dryer",
    "Tension Settings Washer",
    "Printer Extraction Modality",
    "Lights Switch - MS Steamer",
    "Heat Blowers 5 and 6 Relaxed Dryer",
    'Chamber Temperature ("Temp [C]") - MS Steamer',
    "Production wash Speed (m/min) Number",
    "Temp. Cappette Set - MS Steamer",
    "Print Mode",
    "Scour Open Chambers",
    "Steam Lamp  - Found Steamer",
    "Temp. Tetto Set - MS Steamer",
    "Scour Wash Locked Settings Gate",
    "Locked Substation Settings Submitted",
    'Steam Capacity ("Vap [%]") - MS Steamer',
    "Steam Input Valve Value - Found Steamer",
    "Scour Wire Belt 4 and 5 Relaxed Dryer",
    "1. Scour Ultravon Redosing Qty",
    "Steam Speed (m/min) - Found Steamer",
    "Production Dry Locked Settings Gate",
    "Steam Out-Let  - Found Steamer",
    "Power D Temperature",
    "Production Wash Invatex AC Dosing - Chamber 5",
    "Scour Dry Width 3 (mm)",
    "Scour Washer Chamber Settings",
    "Steam Blow Speed (HZ)  - Found Steamer",
    "Stenter Dry Speed",
    "Rolling Mill Relaxed Dryer",
    "Steam Infeed-Auto  - Found Steamer",
    "Ultratex Application",
    "Scour Wire Belt 2 and 3 Relaxed Dryer",
    "Pretreatment Fans",
    "Pretreatment Cut Edges",
    "Stenter Dry Width 1 (mm)",
    "Production Wash Temperature Chamber 5 (C)",
    "Material Code",
    "Steam Loop Length (mm) - Found Steamer",
    "Exhaust Blower Relaxed Dryer",
    "Batcher Speed Reference Image - Found Stenter",
    "Scour Overfeed Relaxed Dryer",
    "Pretreatment Width 1 (mm)",
    "Pre-treatment Type",
    "Production Wash Overfeed",
    "Scour Dry Settings Locked",
    'Hoods Temperature ("TC") - MS Steamer',
    "Production Wash Temperature Chamber 4 (C)",
    "Home Washing Time",
    "record_id",
    "Weight (GSM)",
    "pH After Scour Wash",
    "Steam Fan Speed (HZ) - Found Steamer",
    "Boiler Temperature - MS Steamer",
    "Running Time",
    "Pretreatment Roll Final Width",
    "Scour Heat Blowers 5 and 6 Relaxed Dryer",
    "Production Wash - Eriopon Dosing - Chamber 3",
    "Scour Dry Speed (m/min)",
    "Temperature (C) Relaxed Dryer",
    "Steam Dwell Time (min) - Found Steamer",
    "Loop Length - MS Steamer",
    "Steam Input Valve - Found Steamer",
    "pH in Chamber 5 - Production Wash",
    "Material Name",
    "Output Towing Switch - MS Steamer",
    "__timestamp__",
    "Pretreatment Temperature",
    "Washer Expanders",
    "Pretreatment Overfeed Ratio",
    "1. Scour Invatex CS Redosing Qty",
    "Power D Belt",
    "Print Head Height (mm)",
    "Substrate Thickness (mm)",
    "Scour Dry Temperature (⁰C)",
    'Roof Temperature ("TT") - MS Steamer',
    "Steam Blow  - Found Steamer",
    "Steam F-Exhaust Speed (HZ)  - Found Steamer",
    "Pretreatment Lamps",
    "Pressure Roller Temperature",
    "Scour Wash Temperature Chamber 3 (⁰C)",
    "Steam Temp  - Found Steamer",
    "Production Wash Speed (m/min)",
    "Scour Wash Speed (m/min)",
    "Fan Speed - MS Steamer",
    "Slope 1 Speed - MS Steamer",
    "Steam Out-let ACC  - Found Steamer",
    "Scour Wash Temperature Chamber 2 (⁰C)",
    "Belt Temperature - Tolerance",
    "Stenter Dry Overfeed Ratio",
    "Scour Centering Relaxed Dryer",
    'Missing Fabric Sensor ("FP") - MS Steamer',
    "Steamer Group - MS Steamer",
    'Overflow Sensor ("FF") - MS Steamer',
    "Batcher Speed - Found Stenter - Pretreat",
    "Scour Wash Temperature Chamber 1 (⁰C)",
    "Lamps Height (cm) - Pretreat",
    "Scour Dry Locked Settings Gate",
    "Washer Union Sew Method",
    "Plaiter Relaxed Dryer",
    "Production Wash Temperature Chamber 2 (C)",
    "Production Wash Temperature Chamber 3 (C)",
    "Printer Capability",
    "Centering Relaxed Dryer",
    "V2 Change",
    "Pre-Treatment Locked Settings Gate",
    "Softening Suction %",
    "pH in Chamber 5 - Production Wash - Tolerance",
    "Room Temper. Set - MS Steamer",
    "Scour Temperature (C) Relaxed Dryer",
    "Overfeed Relaxed Dryer",
    'Dwell Time ("Dw. Tm. [m]") - MS Steamer',
    "Manifold Pressure (MPa) - Found Steamer",
    "Steam F-Exhaust  - Found Steamer",
    "Scour Dry PSI",
    "Steam Out Ratio %  - Found Steamer",
    "Material Taxonomy",
    "Pretreatment Exhaust",
    "Material Prep",
    "Weigth",
    "Soften Locked Settings Gate",
    "Scour Rolling Mill Relaxed Dryer",
    "Scour Heat Blowers 1 and 2 Relaxed Dryer",
    "Production Wash Chamber Drain",
    "Scour Wash pH Chamber 5",
    "Stenter Dry Width 3 (mm)",
    "Home Drying Time",
    "Heat Blowers 1 and 2 Relaxed Dryer",
    "Material Process Version",
    "Last Synced",
    "Stenter Dry Temperature (C)",
    "Exhauster - MS Steamer",
    "Print Belt Washer",
    "Scour Wash Temperature Chamber 5 (⁰C)",
    "Name",
    "Pretreatment Notes",
    "Wash & Dry Batch Max Weight (lbs)",
    "Control Panel Image - Found Steamer",
    "Drying Machine",
    "Scour Wash Settings Locked",
    "Stenter Dry  Width 2 (mm)",
    "Steam Temperature (C)  - Found Steamer",
    "Extraction 1 Speed - MS Steamer",
    "Print Belt Washer Drain",
    "Negative Pressure Range",
    "Scour Dry Width 2 (mm)",
    "Scour Heat Blowers 3 and 4 Relaxed Dryer",
    "Pretreatment Speed",
    'Water Cooling Switch ("H2O") - MS Steamer',
}


audit_cols_in_airtable = {
    "Control panel check: Relax Dryer",
    "Scour Dry Temperature",
    "Correa Power D",
    "Calentador Power D",
    "Machine Input Picture",
    "Process Audit Last Modified Date",
    "Relax Dry - Temperature Panel Photo",
    "Dry Exit Date",
    "Scour Dry Temperature Photo",
    "Created By",
    "Washer Speed (M/min) - Audit",
    "Foto Temperatura Power D",
    "Washer Tension #3 Set Value",
    "Relaxed Dryer Overfeed - Audit",
    "Stenter Dry Batcher Speed - Audit",
    "Foto Drenaje de Lavador - Audit",
    "__record_id",
    "Production Wash Bend Bars - Audit",
    "Production Wash Overfeed - Audit",
    "Scour Wash Chamber 5 Temperature (°C) (from Rolls)",
    "Pretreatment Exhaust - Audit",
    "Panel Printer JP7",
    "Scour Wash Speed (M/min) (from Rolls)",
    "Scour Wash Chamber 2 Temperature (C) - Audit",
    "Soften Queue (from Rolls)",
    "Calentador Power D - Audit",
    "__timestamp__",
    "Relaxed Dryer Heat Blowers 3 and 4 - Audit",
    "Pretreatment Width 3 (MM) (from Rolls)",
    "Expanders Settings - Correct or not Correct",
    "Pretreatment Queue (from Rolls)",
    "Relaxed Dryer Exhaust Blower - Audit",
    "Supervisor Verification",
    "Print Flow",
    "Power D Belt - Audit",
    "Panel Printer JP7 - Audit",
    "PPU Sew Entry (Entrada Costura)",
    "Washer Chamber 4 Temperature (C) - Audit",
    "Steamer Operator (from Rolls)",
    "Relaxed Dryer Plaiter - Audit",
    "pretreated_at (from Rolls)",
    "Pretreatment Lamps - Audit",
    "Scour Bend Bars - Audit",
    "Right Pressure",
    "Dry Operator (from Rolls) 2",
    "Washer Chamber 5 Temperature (C) - Audit",
    "Scour Dry Velocity (M/min) - Audit",
    "Scour Wash Operator Display",
    "Pretreatment Width 2 (MM) Audit",
    "record_id",
    "Softening Operator (from Rolls)",
    "Steam F-Exhaust Speed - Audit",
    "Batched Dial Picture",
    "Print Queue",
    "Printer Extraction Modality - Audit",
    "Dry Operator Display",
    "Turno",
    "Foto Drenaje de Lavador",
    "Created Date",
    "Defects - Operator",
    "Washer Chamber 3 Temperature (C) - Audit",
    "pH in Chamber 5 Production Wash - Audit",
    "Washer Tension #2 Set Value",
    "Pretreatment Material Width Post-Pad (MM) (from Rolls)",
    "Pretreatment Cut Edges - Audit",
    "Tension Settings - Correct or not Correct",
    "Washer Expanders - Production Wash Audit",
    "Notas - Audit",
    "Foto Correa de Impresión",
    "Presión de Desenrollador",
    "Lavador de Correa",
    "Scour Dry Panel Check",
    "Scour Dry Overfeed Ratio (from Rolls)",
    "Relaxed Dryer Wire Belt 4 and 5 - Audit",
    "Pretreatment Width 2 (MM) (from Rolls)",
    "Printed Jobs on Buffer? - Audit",
    "Washer Tension #6 Set Value",
    "Unwinder Pressure - Audit",
    "Dryer Width 1 (mm) (from Rolls)",
    "Washer Tension #9 Set Value",
    "Steam Out Ratio - Audit",
    "pH After Scour (from Rolls)",
    "Relaxed Dryer Wire Belt 2 and 3 - Audit",
    "Steamer Fan Speed - Audit",
    "Pressure Photo",
    "Scour Wash Chamber 1 Temperature (C) - Audit",
    "Relaxed Dryer Heat Blowers 1 and 2 - Audit",
    "Material",
    "Pretratment Supervisor Validation",
    "Dryer Speed (m/s) (from Rolls)",
    "Washer Chamber 1 Temperature (°C) (from Rolls)",
    "Scour Wash Operator (from Rolls)",
    "Printed Jobs on Buffer Photo - Operator",
    "Drenaje de Lavador",
    "Washer Tension #1 Set Value",
    "Print Belt Washer Drain - Audit",
    "Printed Time",
    "Scour Wash Chamber 4 Temperature (C) - Audit",
    "Material Display",
    "Terminated Job? - Audit",
    "Washer Chamber 2 Temperature (°C) (from Rolls)",
    "Temperature Settings - Correct or not Correct copy",
    "Pretreatment Overfeed Ratio - Audit",
    "Steamer Loop Length - Audit",
    "Dryer Width 3 (mm) (from Rolls)",
    "Wash Operator Display",
    "Washer Expanders - Scour Wash Audit",
    "Pretreatment Width 1 (MM) (from Rolls)",
    "Scour Wash Exit",
    "Washer Chamber 2 Temperature (C) - Audit",
    "Pretreatment Operator (from Rolls)",
    "Foto Temperatura Correa Printer",
    "Dry Operator (from Rolls)",
    "Pretreatment End Roll Width (mm) (from Rolls)",
    "Pretreatment Fans - Audit",
    "Steamer Input Valve - Audit",
    "Relax Dry Temperature - Audit",
    "Created Date_Format",
    "Process Audit  Flow",
    "Notas - Operator",
    "Who are you? - Auditor Printer",
    "Steamer Temperature - Audit",
    "Printed Date",
    "Print Belt Temperature - Audit",
    "Pretreatment Width 1 (MM) - Audit",
    "Washer Chamber 5 Temperature (°C) (from Rolls)",
    "pH After Scour Wash - Audit",
    "Washer Chamber 4 Temperature (°C) (from Rolls)",
    "Temperatura Power D",
    "Steam B-Exhaust Speed - Audit",
    "Rolls Display",
    "Foto Correa de Impresión - Audit",
    "Washer Chamber 1 Temperature (C) - Audit",
    "Printer",
    "Pretreatment Temperature (°C) - Audit",
    "Washer Tension #4 Set Value",
    "Pretreatment Roll Final Width - Audit",
    "Output Front Pretreatment Temperature (°C)",
    "Dry Overfeed Ratio (from Rolls)",
    "Scour Wash Chamber 4 Temperature (°C) (from Rolls)",
    "Foto Temperatura Correa Printer - Audit",
    "Washer Expander 4 Validation",
    "Wash Operator (from Rolls)",
    "Relaxed Dryer Rolling Mill - Audit",
    "Pretreatment Operator Display",
    "Softening Cycle Time (min) (from Rolls)",
    "Dryer Width 2 (mm) (from Rolls)",
    "Steamer Manifold Pressure - Audit",
    "Pretreatment Speed - Audit",
    "Washer Expander 3 Validation",
    "Who are you? - Operator Printer",
    "Steam Queue (from Rolls) 2",
    "Steamer Blow Speed - Audit",
    "Relax Dry Pilot Speed - Audit",
    "Scour Dry Queue (from Rolls)",
    "Scour Wash Chamber 3 Temperature (C) - Audit",
    "Washer Chamber 3 Temperature (°C) (from Rolls)",
    "Terminated Job? - Operator",
    "Relax Dry Centering Speed - Audit",
    "Left Pressure",
    "Print Belt Washer - Audit",
    "Operator Verification",
    "Power D Temperature - Audit",
    "Softening Suction % (from Rolls)",
    "Flag for Review (Rolls)",
    "Pressure Roller Temperature - Audit",
    "Who are you? Relax Dry Audit",
    "Washer Tension #7 Set Value",
    "Output Back Pretreatment Temperature (°C)",
    "Temperature Verification",
    "Temperatura Correa Printer",
    "Machine Output Picture",
    "Washer Expander 1 Validation",
    "Rolls",
    "Scour Wash Chamber 5 Temperature (C) - Audit",
    "Defects - Audit",
    "Scour Dry Overfeed Ratio - Audit",
    "Notes from Supervisor Validation",
    "Production Processing Validation -  Team Lead",
    "Washer Tension #8 Set Value",
    "Pre-Treatment Original Width (mm) (from Rolls)",
    "Washer Expander Entry Validation",
    "Name",
    "Foto Correa Power D - Audit",
    "Steamer Screen 1 Photo",
    "Relaxed Dryer Heat Blowers 5 and 6 - Audit",
    "Softening Channel # (from Rolls)",
    "Washer Expander 2 Validation",
    "Foto Temperatura Power D - Audit",
    "Printer - Foto Presión de Desenrollador",
    "Pretreatment Batcher Speed - Audit",
    "Printed Jobs on Buffer? - Operator",
    "Scour Dry Pressure (PSI) (from Rolls)",
    "Contract Variables",
    "Relax Dry - Machine Entry Photo",
    "Dryer Temperature (°C) (from Rolls)",
    "Who are you? - Auditor",
    "ONE.Number",
    "Foto Correa Power D",
    "PPU Sew at Nails (Costura en Rama)",
    "Pretreatment Pressure - Audit",
    "Scour Wash Speed (M/min) - Audit",
    "Steam Roof Temperature - Audit",
    "Steam Queue (from Rolls)",
    "__should_archive",
    "Steam Control Panel Check",
    "Pretreatment Velocity",
    "Printer (from Rolls)",
    "Temperature Photo",
    "Washer Tension #5 Set Value",
    "Pretreatment Pressure (PSI) (from Rolls)",
    "Washer Speed (M/min) (from Rolls)",
    "Washer Expander 5 Validation",
    "SCOUR WASH - PH PHOTO",
    "Dry Queue (from Rolls)",
    "Current Substation (from Rolls)",
    "Pretreatment Original Width - Audit",
    "Scour Dry Velocity (M/min) (from Rolls)",
    "Pretreatment Width 3 (MM) - Audit",
    "Control Panel Check",
    "Steamer Speed - Audit",
}

scour_wash_dict = {
    "Scour Wash Speed (m/min)": "Scour Wash Speed (M/min) - Audit",
    "Scour Wash Temperature Chamber 1 (⁰C)": "Scour Wash Chamber 1 Temperature (C) - Audit",
    "Scour Wash Temperature Chamber 2 (⁰C)": "Scour Wash Chamber 2 Temperature (C) - Audit",
    "Scour Wash Temperature Chamber 3 (⁰C)": "Scour Wash Chamber 3 Temperature (C) - Audit",
    "Scour Wash Temperature Chamber 4 (⁰C)": "Scour Wash Chamber 4 Temperature (C) - Audit",
    "Scour Wash Temperature Chamber 5 (⁰C)": "Scour Wash Chamber 5 Temperature (C) - Audit",
    "Scour Bend Bars": "Scour Bend Bars - Audit",
    "Washer Expanders": "Washer Expanders - Scour Wash Audit",
    "pH After Scour Wash": "pH After Scour Wash - Audit",
}


list_range_based_keys = ["pH After Scour Wash"]


prod_wash_dict = {
    "Production Wash Speed (m/min)": "Washer Speed (M/min) - Audit",
    "Production Wash Temperature Chamber 1 (C)": "Washer Chamber 1 Temperature (C) - Audit",
    "Production Wash Temperature Chamber 2 (C)": "Washer Chamber 2 Temperature (C) - Audit",
    "Production Wash Temperature Chamber 3 (C)": "Washer Chamber 3 Temperature (C) - Audit",
    "Production Wash Temperature Chamber 5 (C)": "Washer Chamber 5 Temperature (C) - Audit",
    "Production Wash Temperature Chamber 4 (C)": "Washer Chamber 4 Temperature (C) - Audit",
    "Washer Expanders": "Washer Expanders - Production Wash Audit",
    "pH in Chamber 5 - Production Wash": "pH in Chamber 5 Production Wash - Audit",
    "Production Wash Bend Bars": "Production Wash Bend Bars - Audit",
    "Production Wash Overfeed": "Production Wash Overfeed - Audit",
}


supported_process_audit_flows = [
    "Steam",
    "Scour Wash",
    "Production Wash",
    "Pretreatment",
    "Production Dry - Relax Dry",
    "Print",
    "Scour Dry - Relax Dryer",
    "Production Dry - Stenter",
]

pretreat_dict = {
    "Pretreatment Speed": "Pretreatment Speed - Audit",
    "Pretreatment Pressure": "Pretreatment Pressure - Audit",
    "Pretreatment Overfeed Ratio": "Pretreatment Overfeed Ratio - Audit",
    "Pretreatment Width 1 (mm)": "Pretreatment Width 1 (MM) - Audit",
    "Pretreatment Width 3 (mm)": "Pretreatment Width 3 (MM) - Audit",
    "Pretreatment Width 2 (mm)": "Pretreatment Width 2 (MM) Audit",
    "Pretreatment Temperature": "Pretreatment Temperature (°C) - Audit",
    "Batcher Speed - Found Stenter - Pretreat": "Pretreatment Batcher Speed - Audit",
    "Pretreatment Roll Final Width": "Pretreatment Roll Final Width - Audit",
    "Pretreatment Fans": "Pretreatment Fans - Audit",
    "Pretreatment Lamps": "Pretreatment Lamps - Audit",
    "Pretreatment Cut Edges": "Pretreatment Cut Edges - Audit",
    "Pretreatment Exhaust": "Pretreatment Exhaust - Audit",
}


prod_relax_dry_dict = {
    "Temperature (C) Relaxed Dryer": "Relax Dry Temperature - Audit",
    "Pilot Speed (m/min) Relaxed Dryer": "Relax Dry Pilot Speed - Audit",
    "Centering Relaxed Dryer": "Relax Dry Centering Speed - Audit",
    "Rolling Mill Relaxed Dryer": "Relaxed Dryer Rolling Mill - Audit",
    "Overfeed Relaxed Dryer": "Relaxed Dryer Overfeed - Audit",
    "Wire Belt 2 and 3 Relaxed Dryer": "Relaxed Dryer Wire Belt 2 and 3 - Audit",
    "Wire Belt 4 and 5 Relaxed Dryer": "Relaxed Dryer Wire Belt 4 and 5 - Audit",
    "Plaiter Relaxed Dryer": "Relaxed Dryer Plaiter - Audit",
    "Exhaust Blower Relaxed Dryer": "Relaxed Dryer Exhaust Blower - Audit",
    "Heat Blowers 1 and 2 Relaxed Dryer": "Relaxed Dryer Heat Blowers 1 and 2 - Audit",
    "Heat Blowers 3 and 4 Relaxed Dryer": "Relaxed Dryer Heat Blowers 3 and 4 - Audit",
    "Heat Blowers 5 and 6 Relaxed Dryer": "Relaxed Dryer Heat Blowers 5 and 6 - Audit",
}


found_steamer = {
    "Steam Speed (m/min) - Found Steamer": "Steamer Speed - Audit",
    "Steam Loop Length (mm) - Found Steamer": "Steamer Loop Length - Audit",
    "Steam Temperature (C)  - Found Steamer": "Steamer Temperature - Audit",
    "Steam Blow Speed (HZ)  - Found Steamer": "Steamer Blow Speed - Audit",
    "Steam F-Exhaust Speed (HZ)  - Found Steamer": "Steam F-Exhaust Speed - Audit",
    "Steam B-Exhaust Speed (HZ)  - Found Steamer": "Steam B-Exhaust Speed - Audit",
    "Steam Input Valve Value - Found Steamer": "Steamer Input Valve - Audit",
    "Steam Fan Speed (HZ) - Found Steamer": "Steamer Fan Speed - Audit",
    "Manifold Pressure (MPa) - Found Steamer": "Steamer Manifold Pressure - Audit",
    "Steam Out Ratio %  - Found Steamer": "Steam Out Ratio - Audit",
    "Steam Roof Temperature": "Steam Roof Temperature - Audit",
}


print_dict = {
    "Printer Extraction Modality": "Printer Extraction Modality - Audit",
    "Print Belt Washer Drain": "Print Belt Washer Drain - Audit",
    "Print Belt Washer": "Print Belt Washer - Audit",
    "Power D Belt": "Power D Belt - Audit",
    "Unwinder Pressure": "Unwinder Pressure - Audit",
    "Print Belt Temperature": "Print Belt Temperature - Audit",
    "Pressure Roller Temperature": "Pressure Roller Temperature - Audit",
    "Power D Temperature": "Power D Temperature - Audit",
}


prod_dry_in_stenter_dict = {
    "Stenter Dry Speed": "Stenter Dryer Speed (m/m) - Audit",
    "Stenter Dry Temperature (C)": "Stenter Dryer Temperature (C) - Audit",
    "Stenter Dry Overfeed Ratio": "Stenter Dry Overfeed Ratio - Audit",
    "Stenter Dry Width 1 (mm)": "Stenter Dryer Width 1 (mm) - Audit",
    "Stenter Dry  Width 2 (mm)": "Stenter Dryer Width 2 (mm) - Audit",
    "Stenter Dry Width 3 (mm)": "Stenter Dryer Width 3 (mm) - Audit",
    "Stenter Dry Squeezer Pressure": "Stenter Dry Squeezer Pressure - Audit",
    "Batcher Speed - Found Stenter - Production Dry": "Stenter Dry Batcher Speed - Audit",
}


scour_dry_relax_dry_dict = {
    "Scour Temperature (C) Relaxed Dryer": "Scour Temperature (C) Relaxed Dryer - Audit",
    "Scour Centering Relaxed Dryer": "Scour Centering Relaxed Dryer  - Audit",
    "Scour Pilot Speed (m/min) Relaxed Dryer": "Scour Pilot Speed (m/min) Relaxed Dryer - Audit",
    "Scour Rolling Mill Relaxed Dryer": "Scour Rolling Mill Relaxed Dryer - Audit",
    "Scour Overfeed Relaxed Dryer": "Scour Overfeed Relaxed Dryer - Audit",
    "Scour Wire Belt 2 and 3 Relaxed Dryer": "Scour Wire Belt 2 and 3 Relaxed Dryer - Audit",
    "Scour Wire Belt 4 and 5 Relaxed Dryer": "Scour Wire Belt 4 and 5 Relaxed Dryer - Audit",
    "Scour Plaiter Relaxed Dryer": "Scour Plaiter Relaxed Dryer - Audit",
    "Scour Exhaust Blower Relaxed Dryer": "Scour Exhaust Blower Relaxed Dryer - Audit",
    "Scour Heat Blowers 1 and 2 Relaxed Dryer": "Scour Heat Blowers 1 and 2 Relaxed Dryer - Audit",
    "Scour Heat Blowers 3 and 4 Relaxed Dryer": "Scour Heat Blowers 3 and 4 Relaxed Dryer - Audit",
    "Scour Heat Blowers 5 and 6 Relaxed Dryer": "Scour Heat Blowers 5 and 6 Relaxed Dryer - Audit",
}


def try_float(input):
    try:
        if isinstance(input, str) and input.endswith("%"):
            value = float(input.rstrip("%")) / 100
        else:
            value = float(input)
    except (ValueError, TypeError):
        value = None
    return value


def same_within_error_bars(settingval: float, apival: float) -> bool:
    """non temperature related settings same if within 5% of each other"""
    return abs(settingval - apival) <= (settingval * 0.05)


def same_within_temperature_error_bars(settingval: float, apival: float) -> bool:
    """Temperatre is considered same if within 5C absoloute value .."""
    return abs(settingval - apival) <= (5)


def same_within_temperature_error_bars_printer(
    settingval: float, apival: float
) -> bool:
    """Temperatre is considered same if within 10C absoloute value for Printer"""
    return abs(settingval - apival) <= (10)


def same_within_error_bars_tolerance_from_svot(
    settingval: float, apival: float, settingval_tolerance: float
) -> bool:
    return abs(settingval - apival) <= (settingval_tolerance)


def str_compare(str1, str2):
    if str1.strip().lower() == str2.strip().lower():
        return True
    else:
        return False


def extract_floats(input_str):
    numbers = input_str.split("-")
    return float(numbers[0].strip()), float(numbers[1].strip())


def compare_row(
    flow_name, settingsval, settingsvaltolerance, apival, settings_key_name: str
):
    validation_passed = False
    settings_num = try_float(settingsval)
    api_num = try_float(apival)
    settingsvaltolerance_float = try_float(settingsvaltolerance)

    if settings_num and api_num:
        if settingsvaltolerance_float:
            if same_within_error_bars_tolerance_from_svot(
                settings_num, api_num, settingsvaltolerance_float
            ):
                validation_passed = True
        elif "print" in flow_name.lower() and "temperat" in settings_key_name.lower():
            if same_within_temperature_error_bars_printer(settings_num, api_num):
                validation_passed = True
        elif (
            "temperat" in settings_key_name.lower()
        ):  # "temperat" caputure spanish and english temperature
            if same_within_temperature_error_bars(settings_num, api_num):
                validation_passed = True
        elif same_within_error_bars(settings_num, api_num):
            validation_passed = True

    else:
        if str_compare(settingsval, apival):
            validation_passed = True
        elif settingsval.lower() == "off" and (api_num == 0 or apival.strip() == ""):
            validation_passed = True
        elif settings_key_name in list_range_based_keys:
            lower_range, upper_range = extract_floats(settingsval)
            if api_num:
                if api_num <= upper_range and api_num >= lower_range:
                    validation_passed = True
    return validation_passed


process_audit_crosswalk_dict = {
    "Scour Wash": scour_wash_dict,
    "Pretreatment": pretreat_dict,
    "Production Wash": prod_wash_dict,
    "Steam": found_steamer,
    "Production Dry - Relax Dry": prod_relax_dry_dict,
    "Print": print_dict,
    "Scour Dry - Relax Dryer": scour_dry_relax_dry_dict,
    "Production Dry - Stenter": prod_dry_in_stenter_dict,
}


def test_all_airtable_cols_correctly_named():
    """THis is a test function, not called from the prod code. Its role is to determine which of the columns in the various cofig dicts are missing in airtable, so it can be reported to make engineering"""
    all_crosswalk_dicts = [
        scour_wash_dict,
        pretreat_dict,
        prod_wash_dict,
        found_steamer,
        prod_relax_dry_dict,
        print_dict,
        scour_dry_relax_dry_dict,
    ]  # list(process_audit_crosswalk_dict.values())
    audit_cols_in_config = set(
        [auditcols for d in all_crosswalk_dicts for auditcols in d.values()]
    )
    svot_cols_in_config = set(
        [svotcols for d in all_crosswalk_dicts for svotcols in d.keys()]
    )

    # airtable = res.connectors.load("airtable")

    missing_svot_cols = svot_cols_in_config.difference(svot_cols_in_airtable)

    # audit = airtable["apprcULXTWu33KFsh"]["tblvB5B1dckYJ1idI"].to_dataframe()
    # audit_cols_in_airtable = set(audit.columns)

    missing_audit_cols = audit_cols_in_config.difference(audit_cols_in_airtable)

    res.utils.logger.info(
        f"prod_dry_in_stenter_dict. \nmissing cols from svot/settings: {missing_svot_cols}   \nmissing_audit_cols: {missing_audit_cols}"
    )

    res.utils.logger.info(f"\n\nmissing_audit_cols: {missing_audit_cols}")
    res.utils.logger.info(f"\n\nmissing_svot_cols: {missing_svot_cols}")


def s3_log(dict_to_log, now_utc, input_or_output):
    s3 = res.connectors.load("s3")
    process_audit_flow = dict_to_log.get("Process Audit  Flow") or dict_to_log.get(
        "Process Audit Flow"
    )
    roll_and_material = str(dict_to_log.get("Name")).replace(":", "_")

    now_utc_str = now_utc.strftime("%Y%m%d-%H-%M-%S")
    yearAndMonth = now_utc.strftime("%Y-%B")
    s3filename = f"s3://res-data-platform/res-make-api/validate-locked-settings/{yearAndMonth}/{input_or_output}/{now_utc_str}_{roll_and_material}_{process_audit_flow}_{input_or_output}.json"
    res.utils.logger.info(f"writing dict to s3: {s3filename}")
    s3.write(
        s3filename,
        dict_to_log,
        index=False,
    )


def filter_settings_for_material(settings_df, material):
    settings_df = settings_df[settings_df["Name"] == material]
    return settings_df


def validate_dict(json_dict_from_api: dict, main_settings_df: pd.DataFrame):
    """
    this function aims to validate a dict supplied by the API - the ultimate source of which is an airtable row (supplied as json_dict_from_api)
    we want to validate this against another row taken from a different table in airtable (supplied as settings_df)
    """
    try:
        airtable = res.connectors.load("airtable")
        slack_notify_on_valid = True
        slack_channel_notify = "lockedsettings_validation"

        now_utc = datetime.now(timezone.utc)

        now_utc_str_hr_min = now_utc.strftime("%Y-%m-%d %H:%M:%S")
        # 1. Log what we received, so we can replay, review, audit etc
        s3_log(json_dict_from_api, now_utc, "input")

        list_output_dicts = []
        res.utils.logger.info(
            f"Process Audit Flow: {json_dict_from_api.get('Process Audit  Flow')}  Name: {json_dict_from_api.get('Name')}"
        )

        output = dict()
        if (
            json_dict_from_api.get("Process Audit  Flow")
            not in supported_process_audit_flows
        ):
            res.utils.logger.info(
                f"Process Audit Flow: {json_dict_from_api.get('Process Audit  Flow')} is not on type of process audit to process. No further action being taken"
            )
            return
        else:
            apiinput = json_dict_from_api
            # for the flow, get the dict that maps from SVOT to audit settings. this is the attrib cross walk dict
            attrib_crosswalk_dict = process_audit_crosswalk_dict.get(
                apiinput["Process Audit  Flow"]
            )

            settings_df = main_settings_df

            count_failures = 0
            failure_cols = []
            # setup some common-to-all flows defaults, eg Name etc
            setup_output_defaults(now_utc, output, apiinput)
            # determine the SVOT for this particular material

            settings_for_mat = filter_settings_for_material(
                settings_df, apiinput["Material"]
            )

            # for every single key in the SVOT, read the audit settings, any possible tolerances too, and then compare
            for settings_key_name in attrib_crosswalk_dict.keys():
                if not settings_for_mat.empty:
                    settings_val = str(settings_for_mat[settings_key_name].iloc[0])
                    settings_val_tolerance = settings_for_mat.get(
                        settings_key_name + " - Tolerance"
                    )
                    # maps from SVOT directly to audit (i.e apiinput)
                    apiinput_val = str(
                        apiinput[attrib_crosswalk_dict[settings_key_name]]
                    )
                    validation_passed = compare_row(
                        apiinput["Process Audit  Flow"],
                        settings_val,
                        settings_val_tolerance,
                        apiinput_val,
                        settings_key_name,
                    )
                    if not validation_passed:
                        count_failures += 1
                        failure_cols.append(settings_key_name)
                        output["Station Validated"] = False

                    output[settings_key_name + "-Validation"] = validation_passed
                    output[settings_key_name] = settings_val
                    output[attrib_crosswalk_dict[settings_key_name]] = apiinput_val

                else:
                    output["Station Validated"] = False
                    output[
                        "Other"
                    ] = f"Settings not found for material: {apiinput['Material']}"
                    ping_slack(
                        f"Settings not found for material: {apiinput['Material']} {slack_id_nofify} ",
                        slack_channel_notify,
                    )

            res.utils.logger.info(f"validation dict: {json.dumps(output, indent=4)}")
            list_output_dicts.append(output)

            log_only_cols = extract_log_only_columns_from_image(
                apiinput.get("Process Audit  Flow"),
                apiinput.get("record_id"),
                airtable,
                apiinput.get("Name"),
            )
            output.update(log_only_cols)

            failure_cols_string = ""
            if not output["Station Validated"]:
                failure_cols_string = f" -- invalid cols:[{','.join(failure_cols)}]"

            markup_in_bold = "*" if output["Station Validated"] == False else ""
            if slack_notify_on_valid or output["Station Validated"] == False:
                slackMsg = (
                    f"{now_utc_str_hr_min} -- Flow: {json_dict_from_api.get('Process Audit  Flow')} -- Name: {json_dict_from_api.get('Name')} -- Fails: {count_failures} {failure_cols_string} -- Validated: {markup_in_bold}{output['Station Validated']}{markup_in_bold}",
                )
                res.utils.logger.info(slackMsg)
                if not disable_output_for_testing:
                    res.utils.ping_slack(
                        slackMsg,
                        slack_channel_notify,
                    )

        for d in list_output_dicts:
            s3_log(d, now_utc, "output")

            write_output_to_airtable(airtable, d)

        res.utils.logger.info(f"validation complete for: {output.get('Name')} ")

    except Exception as ex:
        import traceback

        res.utils.logger.error(
            f"Error validating locked settings  : {json_dict_from_api.get('Name')} {traceback.format_exc()}"
        )

        ping_slack(
            f"Error validating locked settings --  {json_dict_from_api.get('Name')} {slack_id_nofify} exception : {traceback.format_exc()}",
            "damo-test",
        )

        ping_slack(
            f"Error validating locked settings -- {json_dict_from_api.get('Name')} {slack_id_nofify} exception : {ex}",
            slack_channel_notify,
        )


def write_output_to_airtable(airtable, d):
    if disable_output_for_testing:
        return

    base = "apprcULXTWu33KFsh"  # prod apprcULXTWu33KFsh     dev appUFmlaynyTcQ5wm -- one plat prod apps71RVe4YgfOO8q
    tab = "Locked Settings Validator Output"  #  prod tblrPs9PpFr4L1pWG     dev tblyGoUp9IvXZWLJp
    refreshAirtableSchema = False
    res.utils.logger.info(f"Writing to airtable - {base} / {tab}  - {d.get('Name')}")
    if refreshAirtableSchema:
        res.utils.logger.info(f"Refreshing airtable schema for {tab}")

        AirtableQueue.refresh_schema_from_record(
            d,
            tab,  # "Locked Settings Validator Output",
            key_field="KEY",
            airtable_base=base,
        )

    try:
        response = airtable[base][tab].update_record(d)

    except Exception as ex:
        AirtableQueue.refresh_schema_from_record(
            d,
            tab,  # "Locked Settings Validator Output",
            key_field="KEY",
            airtable_base=base,
        )
        airtable[base][tab].update_record(d)


def setup_output_defaults(now_utc, output, apiinput):
    output["Name"] = f"{apiinput['Name']}_{apiinput['Process Audit  Flow']}"
    output["KEY"] = f"{output['Name']}_{str(uuid.uuid4())[-4:]}"
    output["Validated Datetime"] = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    output["Roll Name and Material"] = f"{apiinput['Name']}"
    output["Process Audit Flow"] = f"{apiinput['Process Audit  Flow']}"
    output["Station Validated"] = True
    output["Other"] = ""


def get_urls_from_dataframe(dt: pd.DataFrame, colName: str):
    strCol = f"{colName} not found in df"
    urls = []
    try:
        strCol = dt.get(colName)

        for i in range(0, len(dt[colName][0])):
            urls.append(dt[colName][0][i].get("url"))
    except:
        res.utils.logger.info(f"could not find url in {colName} : cell: {strCol}")

    return urls


def extract_log_only_columns_from_image(
    flow_name: str, record_id: str, airtable, roll_info
):
    try:
        log_only_cols = dict()
        pretreat_pressure_gauges_dict = dict()
        rollFlowInfo = f"{flow_name}_{roll_info}"
        url = None
        if flow_name:
            if flow_name in ["Steam", "Pretreatment", "Relax Dry"]:
                filters = f"AND({{__record_id}} = '{record_id}' )"
                #
                at = airtable["apprcULXTWu33KFsh"]["tblvB5B1dckYJ1idI"]
                dt = at.to_dataframe(
                    fields=[
                        "Steam Control Panel Check",
                        "Control Panel Check",
                        "Pressure Photo",
                        "Control panel check: Relax Dryer",
                    ],
                    filters=filters,
                )

                if flow_name == "Steam":
                    urlSteamerPhotoUrls = get_urls_from_dataframe(
                        dt, "Steam Control panel Check"
                    )
                    log_only_cols = (
                        openai_image_to_json.get_steamer_multiple_photos_gpt(
                            urlSteamerPhotoUrls, rollFlowInfo
                        )
                    )

                if flow_name == "Pretreatment":
                    urlPretreatControlPanelUrls = get_urls_from_dataframe(
                        dt, "Control Panel Check"
                    )
                    urlPressurePhotoUrls = get_urls_from_dataframe(dt, "Pressure Photo")
                    pretreat_pressure_gauges_dict = (
                        openai_image_to_json.get_pretreat_double_pressure_gauges_gpt(
                            urlPressurePhotoUrls, rollFlowInfo
                        )
                    )
                    log_only_cols.update(pretreat_pressure_gauges_dict)

                if flow_name == "Relax Dry":
                    urlsRelaxDryerControlPanel = get_urls_from_dataframe(
                        dt, "Control panel check: Relax Dryer"
                    )

                    relax_dryer_control_panel_dicts = (
                        openai_image_to_json.get_relax_dry_control_panel_gpt(
                            urlsRelaxDryerControlPanel, rollFlowInfo
                        )
                    )
                    log_only_cols.update(relax_dryer_control_panel_dicts)

        return log_only_cols
    except:
        exstack = traceback.format_exc()

        res.utils.logger.warn(f"extract_log_only_columns_from_image - {exstack} ")
        ping_slack(f"extract_log_only_columns_from_image - {exstack}", "damo-test")
        return log_only_cols


if (__name__) == "__main__":
    test_all_airtable_cols_correctly_named()
