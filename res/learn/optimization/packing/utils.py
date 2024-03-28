import res
from res.airtable.print import NESTS
from res.learn.optimization.packing.annealed.progressive import SEPERATOR_HEIGHT_PX_UNCOMPENSATED
from shapely.wkt import loads as shapely_loads
from shapely.ops import unary_union
from shapely.affinity import scale, translate


def get_geometry_from_df(df):
    """
    Get the geometry from a dataframe that has a nested_geometry column
    """
    nested_shapes = df.nested_geometry.apply(shapely_loads)
    scale_x = df.stretch_x[0]
    scale_y = df.stretch_y[0]
    return scale(unary_union(nested_shapes), 1.0 / scale_x, 1.0 / scale_y)


def load_nest_geometry(s3_path):
    """
    Load up the geometry of a nest and scale it back down to where
    it should be after washing/drying (aka "returning to home")""
    """
    df = res.connectors.load("s3").read(s3_path)
    return get_geometry_from_df(df)


def load_roll_geometry(roll_name):
    """
    Load all the geometry on the roll.
    Doesnt account for the space between successive printfiles properly but it should be good enough.
    Likewise the difference between headers and dividers between nests.
    """
    nests = NESTS.all(
        formula = f"{{Roll Name}}='{roll_name}'",
        fields = ["Dataframe Path", "Rank"],
        sort = ["-Rank"],
    )
    nested_geom = None
    for nest in nests:
        ng = load_nest_geometry(nest["fields"]["Dataframe Path"])
        if nested_geom is None:
            nested_geom = ng
        else:
            nested_geom = unary_union([
                nested_geom,
                translate(ng, 0, nested_geom.bounds[3] + SEPERATOR_HEIGHT_PX_UNCOMPENSATED)
            ])
    return nested_geom