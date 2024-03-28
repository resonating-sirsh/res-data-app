from xml.etree import ElementTree as ET
import boto3
from airtable import Airtable
import os
import time
from xml.dom import minidom

folders_path = "/Users/bsosa/Documents/GitHub/res-data-platform/apps/res-premises/hot_folders/folders"

hotfolders_root = ET.Element("Hotfolders")

# Get Material Table from res.Magic.MaterialDevelopment
materials_table = Airtable(
    "app1FBXxTRoicCW8k", "tblD1kPG5jpf6GCQl", os.environ.get("AIRTABLE_API_KEY")
)
# Filter making sure that Hotfolders are only created for materials Approved for Production
formula = "AND({Development Status}='Approved')"

approved_material_table = materials_table.get_all(
    formula=formula, sort=[("Material Code", "asc")]
)

# Iterate through each material with its diluent curves to create a hotfolder/workflow for it.
# Need to verify if the naming convention was ever changed at some point while configuring in Caldera.
for material in approved_material_table:
    for curve in material["fields"].get("Diluent Curve Name (from Diluent Curves)"):
        hotfolder_name = material["fields"].get("Material Code") + "_" + curve
        hotfolders_child = ET.SubElement(
            hotfolders_root, "Hotfolder", name=hotfolder_name
        )
        hotfolder_state = ET.SubElement(hotfolders_child, "hf_state")
        hotfolder_state.text = "true"  # from airtable
        hotfolder_workflow = ET.SubElement(hotfolders_child, "hf_Workflow")
        hotfolder_workflow.text = "/qp/MS-JP7-NOVA : {} - {}".format(
            material["fields"].get("Material Code"), curve
        )  # will modify for all printers
        hotfolder_cmdLin = ET.SubElement(hotfolders_child, "hf_cmdLin")
        hotfolder_cmdLin.text = "fileman -load -prevallpages "
        hotfolder_filter = ET.SubElement(hotfolders_child, "hf_filter")
        hotfolder_filter.text = (
            "*" + material["fields"].get("Material Code") + "_" + curve + ".png"
        )
        hotfolder_filemanConfig = ET.SubElement(hotfolders_child, "hf_filemanConfig")
        hotfolder_directory = ET.SubElement(hotfolders_child, "hf_directory")
        hotfolder_directory.text = (
            folders_path + "/" + material["fields"].get("Material Code") + "/" + curve
        )
        hotfolder_TimeOut = ET.SubElement(hotfolders_child, "hf_TimeOut")
        hotfolder_TimeOut.text = "5"


# Save xmlfile to be used on the workstation
tree = ET.ElementTree(hotfolders_root)
# print(prettify(tree))
with open(
    "/Users/bsosa/Documents/GitHub/res-data-platform/apps/res-premises/hot_folders/file.xml",
    "wb",
) as f:
    tree.write(f, encoding="utf-8")
    f.close()
