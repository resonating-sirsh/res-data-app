from collections import defaultdict
from res.airtable.misc import MATERIAL_PROP
from functools import lru_cache


@lru_cache(maxsize=1)
def get_material_taxonomy():
    material_records = MATERIAL_PROP.all(
        fields=["Material Taxonomy", "Material Code"],
        formula="{Development Status}='Approved'",
    )
    taxonomy = defaultdict(list)
    for r in material_records:
        taxonomy[r["fields"]["Material Taxonomy"]].append(r["fields"]["Material Code"])
    return taxonomy


def get_neighbor_materials(material_code):
    taxonomy = get_material_taxonomy()
    node = next((v for v in taxonomy.values() if material_code in v), [])
    return [c for c in node if c != material_code]


def suggest_material_swaps(material_fullness, min_length_requirement):
    swaps = {}
    taxonomy = get_material_taxonomy()
    taxonomy_fullness = {
        k: sum(material_fullness[u] for u in v if u in material_fullness)
        for k, v in taxonomy.items()
    }
    for k, f in taxonomy_fullness.items():
        if f >= min_length_requirement:
            fullness = {
                m: material_fullness[m] for m in taxonomy[k] if m in material_fullness
            }
            ordered_materials = sorted(fullness.keys(), key=lambda m: fullness[m])
            total = 0
            swapped = []
            swap_to = None
            for o in ordered_materials:
                total += fullness[o]
                if total >= min_length_requirement:
                    swap_to = o
                    break
                else:
                    swapped.append(o)
            for s in swapped:
                swaps[s] = swap_to
    return swaps
